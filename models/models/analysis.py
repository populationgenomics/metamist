import json
from datetime import date
from typing import Optional, List, Union, Dict, Any

from pydantic import BaseModel

from models.base import SMBase
from models.enums import AnalysisType, AnalysisStatus, SequenceType


class Analysis(SMBase):
    """Model for Analysis"""

    id: Optional[int]
    type: AnalysisType
    status: AnalysisStatus
    output: Optional[str] = None
    sample_ids: List[Union[int, str]]
    timestamp_completed: Optional[str] = None
    project: Optional[int] = None
    active: Optional[bool] = None
    meta: Dict[str, Any] = {}
    author: Optional[str] = None

    @staticmethod
    def from_db(**kwargs):
        """
        Convert from db keys, mainly converting id to id_
        """
        analysis_type = kwargs.pop('type', None)
        status = kwargs.pop('status', None)
        timestamp_completed = kwargs.pop('timestamp_completed', None)
        meta = kwargs.get('meta')

        if meta and isinstance(meta, str):
            meta = json.loads(meta)

        if timestamp_completed is not None and not isinstance(timestamp_completed, str):
            timestamp_completed = timestamp_completed.isoformat()

        sample_ids = (
            [kwargs.pop('sample_id')]
            if ('sample_id' in kwargs and kwargs.get('sample_id') is not None)
            else []
        )
        return Analysis(
            id=kwargs.pop('id'),
            type=AnalysisType(analysis_type),
            status=AnalysisStatus(status),
            sample_ids=sample_ids,
            output=kwargs.pop('output', []),
            timestamp_completed=timestamp_completed,
            project=kwargs.get('project'),
            meta=meta,
            active=bool(kwargs.get('active')),
            author=kwargs.get('author'),
        )


class DateSizeModel(BaseModel):
    """Date Size model"""

    start: date
    end: date | None
    size: dict[SequenceType, int]


class SampleSizeModel(BaseModel):
    """Project Size model"""

    sample: str
    dates: list[DateSizeModel]


class ProjectSizeModel(BaseModel):
    """Project Size model"""

    project: str
    samples: list[SampleSizeModel]
