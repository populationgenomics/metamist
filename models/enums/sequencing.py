from enum import Enum


class SequenceType(Enum):
    """Type of sequencing"""

    GENOME = 'genome'
    EXOME = 'exome'
    SINGLE_CELL = 'single-cell'
    MT = 'mtseq'


class SequenceStatus(Enum):
    """Status of sequencing"""

    RECEIVED = 'received'
    SENT_TO_SEQUENCING = 'sent-to-sequencing'
    COMPLETED_SEQUENCING = 'completed-sequencing'
    COMPLETED_QC = 'completed-qc'
    FAILED_QC = 'failed-qc'
    UPLOADED = 'uploaded'
    UNKNOWN = 'unknown'
