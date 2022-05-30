from enum import Enum


class AnalysisType(Enum):
    """Types of analysis"""

    QC = 'qc'
    JOINT_CALLING = 'joint-calling'
    GVCF = 'gvcf'
    CRAM = 'cram'
    CUSTOM = 'custom'
    ES_INDEX = 'es-index'


class AnalysisStatus(Enum):
    """Status that an analysis can be run"""

    QUEUED = 'queued'
    IN_PROGRESS = 'in-progress'
    FAILED = 'failed'
    COMPLETED = 'completed'
    UNKNOWN = 'unknown'
