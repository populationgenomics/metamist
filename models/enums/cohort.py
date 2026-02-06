from enum import StrEnum


class CohortStatus(StrEnum):
    """
    Enum for cohort status.
    Based on the Active/Archived status of Sample, SG, CSG tables,
    this list can be extended.
    """

    active = 'active'
    archived = 'archived'  # when cohort marked archived
    invalid = 'invalid'  # when sgs or samples marked archived


class CohortUpdateStatus(StrEnum):
    """
    Enum for update cohort status.
    """

    active = 'active'
    archived = 'archived'
