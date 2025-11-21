from enum import Enum


class CohortStatus(str, Enum):
    """
    Enum for cohort status.
    Based on the Active/Archived status of Sample, SG, CSG tables,
    this list can be extended.
    """

    ACTIVE = 'active'
    ARCHIVED = 'archived'  # when cohort marked inactive
    INVALID = 'invalid'  # when sgs or samples marked inactive/archived


class CohortUpdateStatus(str, Enum):
    """
    Enum for update cohort status.
    """

    ACTIVE = 'active'
    INACTIVE = 'inactive'
