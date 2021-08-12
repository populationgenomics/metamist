from typing import Optional, List, Union

from pydantic.main import BaseModel

from models.enums import AnalysisType, AnalysisStatus


class Analysis(BaseModel):
    """Model for Analysis"""

    id: str = None
    type: AnalysisType = None
    status: AnalysisStatus = None
    output: Optional[str] = None
    sample_ids: List[Union[str, int]] = None
    timestamp_completed: Optional[str] = None
    project: int = None

    @staticmethod
    def from_db(**kwargs):
        """
        Convert from db keys, mainly converting id to id_
        """
        analysis_type = kwargs.pop('type', None)
        status = kwargs.pop('status', None)
        sample_ids = [kwargs.pop('sample_id')]
        output = kwargs.pop('output', [])
        timestamp_completed = kwargs.pop('timestamp_completed', None)
        if timestamp_completed:
            if not isinstance(timestamp_completed, str):
                timestamp_completed = timestamp_completed.isoformat()

        return Analysis(
            id=kwargs.pop('id'),
            type=AnalysisType(analysis_type),
            status=AnalysisStatus(status),
            sample_ids=sample_ids,
            output=output,
            timestamp_completed=timestamp_completed,
            project=kwargs.pop('project'),
        )
