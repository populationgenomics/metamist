from typing import List

from fastapi import APIRouter
from pydantic import BaseModel

from models.enums import AnalysisType, AnalysisStatus

from api.utils.db import get_db_connection, Connection
from db.python.tables.analysis import AnalysisTable


router = APIRouter(prefix='/analysis', tags=['analysis'])


class NewAnalysisModel(BaseModel):
    """Model for 'createNewAnalysis'"""

    sample_ids: List[int]
    type: AnalysisType
    status: AnalysisStatus = AnalysisStatus.QUEUED
    output: str = None


@router.post('/', operation_id='createNewAnalysis')
async def create_new_analysis(
    analysis: NewAnalysisModel, connection: Connection = get_db_connection
):
    """Create a new analysis"""

    atable = AnalysisTable(connection)

    analysis_id = await atable.insert_analysis(
        analysis_type=analysis.type,
        status=analysis.status,
        sample_ids=analysis.sample_ids,
        output=analysis.output,
    )

    return analysis_id


@router.patch('/{analysis_id}/status/{status}', operation_id='updateAnalysisStatus')
async def update_analysis_status(
    analysis_id: int, status: AnalysisStatus, connection: Connection = get_db_connection
):
    """Update status of analysis"""
    atable = AnalysisTable(connection)
    await atable.update_status_of_analysis(analysis_id, status)
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
    return {'sample_ids': result}


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
