from typing import Optional, Dict

from models.enums.sample import SampleType, SampleUpdateType


class Sample:
    """Model for a Sample"""

    def __init__(
        self,
        id_: Optional[int],
        *,
        external_id: str,
        participant_id: str = None,
        type_: SampleType = None,
        active: bool = None,
        meta: Dict = None,
    ):
        self.id = id_
        self.external_id = external_id
        self.participant_id = participant_id
        self.active = active
        self.meta = meta
        self.type = SampleType(type_)

    @staticmethod
    def from_db(**kwargs):
        """
        Convert from db keys, mainly converting id to id_
        """
        _id = kwargs.pop('id', None)
        type_ = kwargs.pop('type', None)

        return Sample(id_=_id, type_=type_, **kwargs)


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
