import json
from typing import Optional, Dict, Union

from pydantic import BaseModel

from models.enums.sample import SampleType, SampleUpdateType


class Sample(BaseModel):
    """Model for a Sample"""

    id_: Union[int, str] = None
    external_id: str = None
    participant_id: Optional[str] = None
    active: Optional[bool] = None
    meta: Optional[Dict] = None
    type: Optional[SampleType] = None

    @staticmethod
    def from_db(**kwargs):
        """
        Convert from db keys, mainly converting id to id_
        """
        _id = kwargs.pop('id', None)
        type_ = kwargs.pop('type', None)
        meta = kwargs.pop('meta', None)
        active = kwargs.pop('active', None)
        if active is not None:
            active = bool(active)
        if meta:
            if isinstance(meta, bytes):
                meta = meta.decode()
            if isinstance(meta, str):
                meta = json.loads(meta)

        return Sample(
            id_=_id, type=SampleType(type_), meta=meta, active=active, **kwargs
        )


class SampleUpdate:
    """Model to represent an update to a sample"""

    def __init__(
        self,
        sample_id,
        type_: SampleUpdateType,
        update: Dict,
        author=None,
        timestamp=None,
    ):
        super().__init__()

        self.sample_id = sample_id
        self.type = type_
        self.update = update
        self.timestamp = timestamp
        self.author = author

    @staticmethod
    def from_db(obj: Dict[str, any]):
        """Parse from db keys"""
        type_ = obj.pop('type')
        return SampleUpdate(type_=type_, **obj)
