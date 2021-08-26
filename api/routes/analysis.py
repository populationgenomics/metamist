from typing import List, Optional

import io
import csv
from datetime import date

from fastapi import APIRouter
from pydantic import BaseModel
from starlette.responses import StreamingResponse

from models.enums import AnalysisType, AnalysisStatus
from models.models.sample import sample_id_transform_to_raw, sample_id_format
from db.python.tables.analysis import AnalysisTable

from api.utils.db import (
    get_projectless_db_connection,
    get_project_db_connection,
    Connection,
)

router = APIRouter(prefix='/analysis', tags=['analysis'])


class AnalysisModel(BaseModel):
    """Model for 'createNewAnalysis'"""

    sample_ids: List[str]
    type: AnalysisType
    status: AnalysisStatus
    output: str = None


class AnalysisUpdateModel(BaseModel):
    """Update analysis model"""

    status: AnalysisStatus
    output: Optional[str] = None


@router.put('/{project}/', operation_id='createNewAnalysis', response_model=int)
async def create_new_analysis(
    analysis: AnalysisModel, connection: Connection = get_project_db_connection
):
    """Create a new analysis"""

    atable = AnalysisTable(connection)

    sample_ids = sample_id_transform_to_raw(analysis.sample_ids)

    analysis_id = await atable.insert_analysis(
        analysis_type=analysis.type,
        status=analysis.status,
        sample_ids=sample_ids,
        output=analysis.output,
    )

    return analysis_id


@router.patch('/{analysis_id}/', operation_id='updateAnalysisStatus')
async def update_analysis_status(
    analysis_id: int,
    analysis: AnalysisUpdateModel,
    connection: Connection = get_projectless_db_connection,
):
    """Update status of analysis"""
    atable = AnalysisTable(connection)
    await atable.update_analysis(
        analysis_id, status=analysis.status, output=analysis.output
    )
    return True


@router.get(
    '/{project}/type/{analysis_type}/not-included-sample-ids',
    operation_id='getAllSampleIdsWithoutAnalysisType',
)
async def get_all_sample_ids_without_analysis_type(
    analysis_type: AnalysisType,
    connection: Connection = get_project_db_connection,
):
    """get_all_sample_ids_without_analysis_type"""
    atable = AnalysisTable(connection)
    result = await atable.get_all_sample_ids_without_analysis_type(analysis_type)
    return {'sample_ids': sample_id_format(result)}


@router.get(
    '/{project}/incomplete',
    operation_id='getIncompleteAnalyses',
)
async def get_incomplete_analyses(connection: Connection = get_project_db_connection):
    """Get analyses with status queued or in-progress"""
    atable = AnalysisTable(connection)
    result = await atable.get_incomplete_analyses()
    return result


@router.get(
    '/{project}/latest_complete',
    operation_id='getLatestCompleteAnalyses',
)
async def get_latest_complete_analyses(
    connection: Connection = get_project_db_connection,
):
    """Get "complete" analyses with the latest timestamp_completed"""
    atable = AnalysisTable(connection)
    results = await atable.get_latest_complete_analyses_per_sample()
    for result in results:
        result.sample_ids = sample_id_format(result.sample_ids)

    return results


@router.get(
    '/{project}/latest_complete/{analysis_type}',
    operation_id='getLatestCompleteAnalysesByType',
)
async def get_latest_complete_analyses_by_type(
    analysis_type: str,
    connection: Connection = get_project_db_connection,
):
    """Get "complete" analyses with the latest timestamp_completed"""
    atable = AnalysisTable(connection)
    results = await atable.get_latest_complete_analyses_per_sample(analysis_type)
    for result in results:
        result.sample_ids = sample_id_format(result.sample_ids)
    return results


@router.post(
    '/latest_complete/gvcf/for-samples', operation_id='getLatestGvcfsForSamples'
)
async def get_latest_complete_gvcfs_for_samples(
    sample_ids: List[str],
    allow_missing: bool = True,
    connection: Connection = get_projectless_db_connection,
):
    """Get latest complete gvcfs for sample"""
    atable = AnalysisTable(connection)
    results = await atable.get_latest_complete_gvcfs_for_samples(
        sample_ids=sample_id_transform_to_raw(sample_ids), allow_missing=allow_missing
    )

    for result in results:
        result.sample_ids = sample_id_format(result.sample_ids)

    return results


@router.get('/{analysis_id}/details', operation_id='getAnalysisById')
async def get_analysis_by_id(
    analysis_id: int, connection: Connection = get_projectless_db_connection
):
    """Get analysis by ID"""
    atable = AnalysisTable(connection)
    result = await atable.get_analysis_by_id(analysis_id)
    result.sample_ids = sample_id_format(result.sample_ids)
    return result


@router.get(
    '/{project}/cram-paths/csv',
    operation_id='getCramPathsCsv',
    response_class=StreamingResponse,
)
async def get_cram_path_csv(connection: Connection = get_project_db_connection):
    """
    Get map of ExternalSampleId,pathToCram,InternalSampleID for seqr

    Description:
        Column 1: Individual ID
        Column 2: gs:// Google bucket path or server filesystem path for this Individual
        Column 3: Sample ID for this file, if different from the Individual ID. Used primarily for gCNV files to identify the sample in the batch path
    """

    at = AnalysisTable(connection)
    rows = await at.get_cram_path_csv_for_seqr(project=connection.project)

    # remap sample ids
    for row in rows:
        row[2] = sample_id_format(row[2])

    output = io.StringIO()
    writer = csv.writer(output, delimiter='\t')
    writer.writerows(rows)

    basefn = f'{connection.project}-{date.today().isoformat()}'

    return StreamingResponse(
        iter(output.getvalue()),
        media_type='text/csv',
        headers={'Content-Disposition': f'filename={basefn}.ped'},
    )
