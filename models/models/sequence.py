from typing import Optional, Dict, Any

from pydantic import BaseModel

from models.enums import SequenceType, SequenceStatus


class SampleSequencing(BaseModel):
    """Model to hold SampleSequence data"""

    id_: Optional[int] = None
    sample_id: Optional[int] = None
    type: Optional[SequenceType] = None
    meta: Optional[Dict[str, Any]] = None
    status: Optional[SequenceStatus] = None

    def __repr__(self):
        return ', '.join(f'{k}={v}' for k, v in vars(self).items())
