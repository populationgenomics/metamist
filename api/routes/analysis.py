from typing import List

from fastapi import APIRouter
from pydantic import BaseModel

from models.enums import AnalysisType, AnalysisStatus

from api.utils.db import get_db_connection, Connection


router = APIRouter(prefix='/analysis', tags=['analysis'])


class NewAnalysisModel(BaseModel):
    """Model for 'createNewAnalysis'"""

    sample_ids: List[int]
    type: AnalysisType
    status: AnalysisStatus = AnalysisStatus.QUEUED


@router.post('/', operation_id='createNewAnalysis')
async def create_new_analysis(
    analysis: NewAnalysisModel, connection: Connection = get_db_connection
):
    """Create a new analysis"""

    # success: bool
    # analysisId: int

    raise NotImplementedError(f'Not implemented for sampleIds={analysis.sample_ids}')
