from enum import Enum

class CohortStatus(str, Enum):
    """
    Enum for cohort status.
    Based on the Active/Archived/DELETED status of Sample, SG, CSG tables,
    this list can be extended.
    """

    ACTIVE = 'active'
    INACTIVE = 'inactive'