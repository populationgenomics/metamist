from enum import Enum


class EtlStatus(Enum):
    """Etl Status Enum"""

    FAILED = 'FAILED'
    SUCCESS = 'SUCCESS'