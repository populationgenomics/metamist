from enum import Enum


class AnalysisType(Enum):
    """Types of analysis"""

    qc = 'qc'
    joint_calling = 'joint-calling'
    custom = 'custom'


class AnalysisStatus(Enum):
    """Status that an analysis can be run"""

    queued = 'queued'
    in_progress = 'in-progress'
    failed = 'failed'
    completed = 'completed'
