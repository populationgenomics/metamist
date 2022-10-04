from typing import Optional, Dict, Any, Union

import json

from pydantic import BaseModel

from models.enums import SequenceType, SequenceStatus


class SampleSequencing(BaseModel):
    """Model to hold SampleSequence data"""

    id: Optional[int] = None
    external_ids: Optional[dict[str, str]] = None
    sample_id: Union[str, int]
    type: SequenceType
    meta: Optional[Dict[str, Any]] = None
    status: SequenceStatus

    def __repr__(self):
        return ', '.join(f'{k}={v}' for k, v in vars(self).items())

    def __eq__(self, other):
        return (
            self.id == other.id
            and int(self.sample_id) == int(other.sample_id)
            and self.status == other.status
            and self.meta == other.meta
            and self.type == other.type
        )

    @staticmethod
    def from_db(d: Dict):
        """Take DB mapping object, and return SampleSequencing"""
        type_ = d.pop('type')
        status = d.pop('status')
        meta = d.pop('meta', None)

        if type_:
            type_ = SequenceType(type_)
        if status:
            status = SequenceStatus(status)

        if meta:
            if isinstance(meta, bytes):
                meta = meta.decode()
            if isinstance(meta, str):
                meta = json.loads(meta)
        return SampleSequencing(type=type_, status=status, meta=meta, **d)
