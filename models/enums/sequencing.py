from enum import Enum


class SequenceType(Enum):
    """Type of sequencing"""

    GENOME = 'genome'
    EXOME = 'exome'
    TRANSCRIPTOME = 'transcriptome'
    CHIP = 'chip'
    MT = 'mtseq'


class SequenceTechnology(Enum):
    """Sequencing technologies"""
    SHORT_READ = 'short-read'
    LONG_READ = 'long-read'
    SINGLE_CELL_RNA_SEQ = 'single-cell-rna-seq'
    BULK_RNA_SEQ = 'bulk-rna-seq'


class SequenceStatus(Enum):
    """Status of sequencing"""

    RECEIVED = 'received'
    SENT_TO_SEQUENCING = 'sent-to-sequencing'
    COMPLETED_SEQUENCING = 'completed-sequencing'
    COMPLETED_QC = 'completed-qc'
    FAILED_QC = 'failed-qc'
    UPLOADED = 'uploaded'
    UNKNOWN = 'unknown'
