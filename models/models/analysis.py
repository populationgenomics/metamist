from typing import Optional, List, Union, Dict, Any
import json

from models.base import SMBase
from models.enums import AnalysisType, AnalysisStatus


class Analysis(SMBase):
    """Model for Analysis"""

    id: Optional[int]
    type: AnalysisType
    status: AnalysisStatus
    output: Optional[str] = None
    sample_ids: List[Union[str, int]]
    timestamp_completed: Optional[str] = None
    project: Optional[int] = None
    active: Optional[bool] = None
    meta: Dict[str, Any] = {}

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

        return Analysis(
            id=kwargs.pop('id'),
            type=AnalysisType(analysis_type),
            status=AnalysisStatus(status),
            sample_ids=[kwargs.pop('sample_id')] if 'sample_id' in kwargs else [],
            output=kwargs.pop('output', []),
            timestamp_completed=timestamp_completed,
            project=kwargs.get('project'),
            meta=meta,
            active=bool(kwargs.get('active')),
        )
