from enum import Enum


class SampleType(Enum):
    """Enum describing types of physical samples"""

    BLOOD = 'blood'
    SALIVA = 'saliva'


class SampleUpdateType(Enum):
    """Allowed types of updates to a sample"""

    CREATED = 'created'
    EXTERNAL_ID = 'external_id'
    METADATA = 'metadata'
    STATUS = 'status'
    PARTICIPANT_ID = 'participant_id'
