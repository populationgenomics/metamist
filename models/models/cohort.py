import json

from pydantic import BaseModel

from models.base import SMBase
from models.models.project import ProjectId
from models.models.sequencing_group import SequencingGroup, SequencingGroupExternalId


class Cohort(SMBase):
    """Model for Cohort"""

    id: int
    name: str
    author: str
    project: ProjectId
    description: str
    template_id: int
    sequencing_groups: list[SequencingGroup | SequencingGroupExternalId]

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
        sequencing_groups = d.pop('sequencing_groups', [])

        return Cohort(
            id=_id,
            name=name,
            author=author,
            project=project,
            description=description,
            template_id=template_id,
            sequencing_groups=sequencing_groups,
        )


class CohortTemplateInternal(SMBase):
    """Model for CohortTemplate"""

    id: int
    name: str
    description: str
    criteria: dict

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

        return CohortTemplateInternal(
            id=_id,
            name=name,
            description=description,
            criteria=criteria,
        )


class CohortBody(BaseModel):
    """Represents the expected JSON body of the create cohort request"""

    name: str
    description: str
    template_id: str | None = None


class CohortCriteria(BaseModel):
    """Represents the expected JSON body of the create cohort request"""

    projects: list[str] | None = []
    sg_ids_internal: list[str] | None = None
    excluded_sgs_internal: list[str] | None = None
    sg_technology: list[str] | None = None
    sg_platform: list[str] | None = None
    sg_type: list[str] | None = None
    sample_type: list[str] | None = None


class CohortTemplate(BaseModel):
    """Represents a cohort template, to be used to build cohorts."""

    id: int | None
    name: str
    description: str
    criteria: CohortCriteria
