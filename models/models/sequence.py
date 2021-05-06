from typing import Dict

from models.enums import SequencingType, SequencingStatus


class SampleSequencing:
    """Model to hold SampleSequence data"""

    def __init__(
        self,
        id_: int,
        *,
        sample_id: int,
        type_: SequencingType,
        meta: Dict[str, any],
    ):
        self.id = id_
        self.sample_id = sample_id
        self.type = type_
        self.meta = meta

    def __repr__(self):
        return ', '.join(f'{k}={v}' for k, v in vars(self).items())


class SampleSequencingStatus:
    """Model to hold SampleSequenceStatus data"""

    def __init__(self, sample_sequencing_id: int, status: SequencingStatus, timestamp):
        super().__init__()

        self.sample_sequencing_id = sample_sequencing_id
        self.status = SequencingStatus(status)
        self.timestamp = timestamp
