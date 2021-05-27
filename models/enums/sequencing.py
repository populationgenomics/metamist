from enum import Enum


class SequencingType(Enum):
    """Type of sequencing"""

    WGS = 'wgs'
    SINGLE_CELL = 'single-cell'


class SequencingStatus(Enum):
    """Status of sequencing"""

    UNKNOWN = 'unknown'
    RECEIVED = 'received'
    SENT_TO_SEQUENCING = 'sent-to-sequencing'
    COMPLETED_SEQUENCING = 'completed-sequencing'
    COMPLETED_QC = 'completed-qc'
    FAILED_QC = 'failed-qc'
    UPLOADED = 'uploaded'
