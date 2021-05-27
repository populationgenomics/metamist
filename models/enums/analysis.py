from enum import Enum


class AnalysisType(Enum):
    """Types of analysis"""

    QC = 'qc'
    JOINT_CALLING = 'joint-calling'
    CUSTOM = 'custom'


class AnalysisStatus(Enum):
    """Status that an analysis can be run"""

    QUEUED = 'queued'
    IN_PROGRESS = 'in-progress'
    FAILED = 'failed'
    COMPLETED = 'completed'
