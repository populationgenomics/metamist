from enum import Enum


class AnalysisStatus(Enum):
    """Status that an analysis can be run"""

    QUEUED = 'queued'
    IN_PROGRESS = 'in-progress'
    FAILED = 'failed'
    COMPLETED = 'completed'
    UNKNOWN = 'unknown'
