import json

from models.base import SMBase
from models.models.project import ProjectId


class CohortInternal(SMBase):
    """Model for Cohort"""

    id: int
    name: str
    author: str
    project: ProjectId
    description: str
    template_id: int

    @staticmethod
    def from_db(d: dict):
        """
        Convert from db keys, mainly converting id to id_
        """
        _id = d.pop('id', None)
        project = d.pop('project', None)
        description = d.pop('description', None)
        name = d.pop('name', None)
        author = d.pop('author', None)
        template_id = d.pop('template_id', None)

        return CohortInternal(
            id=_id,
            name=name,
            author=author,
            project=project,
            description=description,
            template_id=template_id,
        )


class CohortTemplateInternal(SMBase):
    """Model for CohortTemplate"""

    id: int
    name: str
    description: str
    criteria: dict
    project: ProjectId

    @staticmethod
    def from_db(d: dict):
        """
        Convert from db keys, mainly converting id to id_
        """
        _id = d.pop('id', None)
        name = d.pop('name', None)
        description = d.pop('description', None)
        criteria = d.pop('criteria', None)
        if criteria and isinstance(criteria, str):
            criteria = json.loads(criteria)

        project = d.pop('project', None)

        return CohortTemplateInternal(
            id=_id,
            name=name,
            description=description,
            criteria=criteria,
            project=project,
        )


class CohortBody(SMBase):
    """Represents the expected JSON body of the create cohort request"""

    name: str
    description: str
    template_id: str | None = None


class CohortCriteria(SMBase):
    """Represents the expected JSON body of the create cohort request"""

    projects: list[str] | None = []
    sg_ids_internal: list[str] | None = None
    excluded_sgs_internal: list[str] | None = None
    sg_technology: list[str] | None = None
    sg_platform: list[str] | None = None
    sg_type: list[str] | None = None
    sample_type: list[str] | None = None


class CohortTemplate(SMBase):
    """Represents a cohort template, to be used to build cohorts."""

    id: int | None
    name: str
    description: str
    criteria: CohortCriteria


class NewCohort(SMBase):
    """Represents a cohort, which is a collection of sequencing groups."""

    dry_run: bool = False
    cohort_id: str
    sequencing_group_ids: list[str]
