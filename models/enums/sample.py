from enum import Enum


class SampleType(Enum):
    """Enum describing types of physical samples"""

    blood = 'blood'
    saliva = 'saliva'


class SampleUpdateType(Enum):
    """Allowed types of updates to a sample"""

    created = 'created'
    external_id = 'external_id'
    metadata = 'metadata'
    status = 'status'
    participant_id = 'participant_id'
