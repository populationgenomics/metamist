from enum import Enum


class SequencingType(Enum):
    """Type of sequencing"""

    wgs = 'wgs'
    single_cell = 'single-cell'


class SequencingStatus(Enum):
    """Status of sequencing"""

    unknown = 'unknown'
    received = 'received'
    sent_to_sequencing = 'sent-to-sequencing'
    completed_sequencing = 'completed-sequencing'
    completed_qc = 'completed-qc'
    failed_qc = 'failed-qc'
    uploaded = 'uploaded'
