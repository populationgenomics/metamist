from typing import List, Optional

from fastapi import APIRouter
from pydantic import BaseModel

from models.enums import AnalysisType, AnalysisStatus
from models.models.sample import sample_id_transform_to_raw, sample_id_format
from db.python.tables.analysis import AnalysisTable

from api.utils.db import get_db_connection, Connection

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


@router.put('/', operation_id='createNewAnalysis', response_model=int)
async def create_new_analysis(
    analysis: AnalysisModel, connection: Connection = get_db_connection
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
    connection: Connection = get_db_connection,
):
    """Update status of analysis"""
    atable = AnalysisTable(connection)
    await atable.update_analysis(
        analysis_id, status=analysis.status, output=analysis.output
    )
    return True


@router.get(
    '/type/{analysis_type}/not-included-sample-ids',
    operation_id='getAllSampleIdsWithoutAnalysisType',
)
async def get_all_sample_ids_without_analysis_type(
    analysis_type: AnalysisType,
    connection: Connection = get_db_connection,
):
    """get_all_sample_ids_without_analysis_type"""
    atable = AnalysisTable(connection)
    result = await atable.get_all_sample_ids_without_analysis_type(analysis_type)
    return {'sample_ids': sample_id_format(result)}


@router.get(
    '/new_gvcfs_since_last_successful_joint_call',
    operation_id='getAllNewGvcfsSinceLastSuccessfulJointCall',
)
async def get_all_new_gvcfs_since_last_successful_joint_call(
    connection: Connection = get_db_connection,
):
    """get_all_new_gvcfs_since_last_successful_joint_call"""
    atable = AnalysisTable(connection)
    result = await atable.get_all_new_gvcfs_since_last_successful_joint_call()
    return {'analysis_ids': result}


@router.get(
    '/incomplete',
    operation_id='getIncompleteAnalyses',
)
async def get_incomplete_analyses(connection: Connection = get_db_connection):
    """Get analyses with status queued or in-progress"""
    atable = AnalysisTable(connection)
    result = await atable.get_incomplete_analyses()
    return result


@router.get(
    '/latest_complete',
    operation_id='getLatestCompleteAnalyses',
)
async def get_latest_complete_analyses(connection: Connection = get_db_connection):
    """Get "complete" analyses with the latest timestamp_completed"""
    atable = AnalysisTable(connection)
    result = await atable.get_latest_complete_analyses()
    return result


@router.get(
    '/latest_complete/{analysis_type}',
    operation_id='getLatestCompleteAnalysesByType',
)
async def get_latest_complete_analyses_by_type(
    analysis_type: str,
    connection: Connection = get_db_connection,
):
    """Get "complete" analyses with the latest timestamp_completed"""
    atable = AnalysisTable(connection)
    result = await atable.get_latest_complete_analyses(analysis_type)
    return result
