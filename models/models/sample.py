from typing import Optional, Dict

from models.enums.sample import SampleType


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
