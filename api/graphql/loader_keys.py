import enum
from typing import Any, TypedDict

from db.python.connect import Connection


class LoaderKeys(enum.Enum):
    """
    Keys for the data loaders, define them to it's clearer when we add / remove
    them, and reduces the chance of typos
    """

    PROJECTS_FOR_IDS = 'projects_for_id'

    AUDIT_LOGS_BY_IDS = 'audit_logs_by_ids'
    AUDIT_LOGS_BY_ANALYSIS_IDS = 'audit_logs_by_analysis_ids'

    ANALYSES_FOR_SEQUENCING_GROUPS = 'analyses_for_sequencing_groups'

    ASSAYS_FOR_IDS = 'assays_for_ids'
    ASSAYS_FOR_SAMPLES = 'sequences_for_samples'
    ASSAYS_FOR_SEQUENCING_GROUPS = 'assays_for_sequencing_groups'

    SAMPLES_FOR_IDS = 'samples_for_ids'
    SAMPLES_FOR_PARTICIPANTS = 'samples_for_participants'
    SAMPLES_FOR_PROJECTS = 'samples_for_projects'
    SAMPLES_FOR_PARENTS = 'samples_for_parents'

    PHENOTYPES_FOR_PARTICIPANTS = 'phenotypes_for_participants'

    PARTICIPANTS_FOR_IDS = 'participants_for_ids'
    PARTICIPANTS_FOR_FAMILIES = 'participants_for_families'
    PARTICIPANTS_FOR_PROJECTS = 'participants_for_projects'

    FAMILIES_FOR_PARTICIPANTS = 'families_for_participants'
    FAMILY_PARTICIPANTS_FOR_FAMILIES = 'family_participants_for_families'
    FAMILY_PARTICIPANTS_FOR_PARTICIPANTS = 'family_participants_for_participants'
    FAMILIES_FOR_IDS = 'families_for_ids'

    SEQUENCING_GROUPS_FOR_IDS = 'sequencing_groups_for_ids'
    SEQUENCING_GROUPS_FOR_SAMPLES = 'sequencing_groups_for_samples'
    SEQUENCING_GROUPS_FOR_PROJECTS = 'sequencing_groups_for_projects'
    SEQUENCING_GROUPS_FOR_ANALYSIS = 'sequencing_groups_for_analysis'

    COMMENTS_FOR_SAMPLE_IDS = 'comments_for_sample_ids'
    COMMENTS_FOR_PARTICIPANT_IDS = 'comments_for_participant_ids'
    COMMENTS_FOR_ASSAY_IDS = 'comments_for_assay_ids'
    COMMENTS_FOR_PROJECT_IDS = 'comments_for_project_ids'
    COMMENTS_FOR_SEQUENCING_GROUP_IDS = 'comments_for_sequencing_group_ids'
    COMMENTS_FOR_FAMILY_IDS = 'comments_for_family_ids'

    USERS = 'users'


class GraphQLContext(TypedDict):
    """Basic dict type for GraphQL context to be passed to resolvers"""

    loaders: dict[LoaderKeys, Any]
    connection: Connection
