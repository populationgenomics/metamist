from typing import Optional, Dict, Any

from pydantic import BaseModel

from models.enums import SequencingType, SequencingStatus


class SampleSequencing(BaseModel):
    """Model to hold SampleSequence data"""

    id_: Optional[int] = None
    sample_id: Optional[int] = None
    type: Optional[SequencingType] = None
    meta: Optional[Dict[str, Any]] = None
    status: Optional[SequencingStatus] = None

    def __repr__(self):
        return ', '.join(f'{k}={v}' for k, v in vars(self).items())
