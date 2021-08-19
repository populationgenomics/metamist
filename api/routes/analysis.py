from typing import List, Optional

from fastapi import APIRouter
from pydantic import BaseModel

from models.enums import AnalysisType, AnalysisStatus
from models.models.analysis import Analysis
from models.models.sample import sample_id_transform_to_raw, sample_id_format
from db.python.layers.analysis import AnalysisLayer

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

    atable = AnalysisLayer(connection)

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
    atable = AnalysisLayer(connection)
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
    atable = AnalysisLayer(connection)
    result = await atable.get_all_sample_ids_without_analysis_type(
        connection.project, analysis_type
    )
    return {'sample_ids': sample_id_format(result)}


@router.get(
    '/{project}/incomplete',
    operation_id='getIncompleteAnalyses',
)
async def get_incomplete_analyses(connection: Connection = get_project_db_connection):
    """Get analyses with status queued or in-progress"""
    atable = AnalysisLayer(connection)
    result = await atable.get_incomplete_analyses(project=connection.project)
    return result


@router.post(
    '/latest_complete/{analysis_type}/for-samples',
    operation_id='getLatestGvcfsForSamples',
)
async def get_latest_complete_gvcfs_for_samples(
    sample_ids: List[str],
    analysis_type: AnalysisType,
    allow_missing: bool = True,
    connection: Connection = get_projectless_db_connection,
):
    """Get latest complete gvcfs for sample"""
    atable = AnalysisLayer(connection)
    results = await atable.get_latest_complete_analysis_for_samples_by_type(
        analysis_type=analysis_type,
        sample_ids=sample_id_transform_to_raw(sample_ids),
        allow_missing=allow_missing,
    )

    for result in results:
        result.sample_ids = sample_id_format(result.sample_ids)

    return results


@router.get(
    '/{analysis_id}/details', operation_id='getAnalysisById', response_model=Analysis
)
async def get_analysis_by_id(
    analysis_id: int, connection: Connection = get_projectless_db_connection
):
    """Get analysis by ID"""
    atable = AnalysisLayer(connection)
    result = await atable.get_analysis_by_id(analysis_id)
    result.sample_ids = sample_id_format(result.sample_ids)
    return result
