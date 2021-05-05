from typing import Dict

from models.enums.sample import SampleType


class Sample:
    """Model for a Sample"""

    def __init__(
        self,
        id_: int,
        external_id: str,
        participant_id: str = None,
        active: bool = None,
        sample_meta: Dict = None,
        sample_type: SampleType = None,
    ):
        self.id = id_
        self.external_id = external_id
        self.participant_id = participant_id
        self.active = active
        self.sample_meta = sample_meta
        self.sample_type = SampleType(sample_type)

    @staticmethod
    def from_db(**kwargs):
        """
        Convert from db keys, mainly converting id to id_
        """
        _id = kwargs.pop('id', None)

        return Sample(id_=_id, **kwargs)
