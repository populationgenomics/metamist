import json

from models.base import SMBase
from models.models.project import ProjectId
from models.utils.cohort_id_format import cohort_id_format
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_format_list,
    sequencing_group_id_transform_to_raw_list,
)


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


class CohortCriteriaInternal(SMBase):
    """Internal Model for CohortCriteria"""

    projects: list[ProjectId] | None = []
    sg_ids_internal_raw: list[int] | None = None
    excluded_sgs_internal_raw: list[int] | None = None
    sg_technology: list[str] | None = None
    sg_platform: list[str] | None = None
    sg_type: list[str] | None = None
    sample_type: list[str] | None = None


class CohortTemplateInternal(SMBase):
    """Model for CohortTemplate"""

    id: int | None
    name: str
    description: str
    criteria: CohortCriteriaInternal

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

    def to_internal(
        self, projects_internal: list[ProjectId] | None
    ) -> CohortCriteriaInternal:
        """
        Convert to internal model
        """

        sg_ids_raw = None
        if self.sg_ids_internal:
            sg_ids_raw = sequencing_group_id_transform_to_raw_list(self.sg_ids_internal)

        excluded_sgs_raw = None
        if self.excluded_sgs_internal:
            excluded_sgs_raw = sequencing_group_id_transform_to_raw_list(
                self.excluded_sgs_internal
            )

        return CohortCriteriaInternal(
            projects=projects_internal,
            sg_ids_internal_raw=sg_ids_raw,
            excluded_sgs_internal_raw=excluded_sgs_raw,
            sg_technology=self.sg_technology,
            sg_platform=self.sg_platform,
            sg_type=self.sg_type,
            sample_type=self.sample_type,
        )


class CohortTemplate(SMBase):
    """Represents a cohort template, to be used to build cohorts."""

    id: int | None
    name: str
    description: str
    criteria: CohortCriteria

    def to_internal(self, criteria_projects: list[ProjectId]) -> CohortTemplateInternal:
        """
        Convert to internal model
        """
        return CohortTemplateInternal(
            id=self.id,
            name=self.name,
            description=self.description,
            criteria=self.criteria.to_internal(criteria_projects),
        )


class NewCohort(SMBase):
    """Represents a cohort, which is a collection of sequencing groups."""

    dry_run: bool = False
    cohort_id: str
    sequencing_group_ids: list[str]


class NewCohortInternal(SMBase):
    """Represents a cohort, which is a collection of sequencing groups."""

    dry_run: bool = False
    cohort_id: int | None
    sequencing_group_ids: list[int] | None = None

    def to_external(self) -> NewCohort:
        """
        Convert to external model
        """
        return NewCohort(
            dry_run=self.dry_run,
            cohort_id=(
                cohort_id_format(self.cohort_id) if self.cohort_id else 'CREATE NEW'
            ),
            sequencing_group_ids=(
                sequencing_group_id_format_list(self.sequencing_group_ids)
                if self.sequencing_group_ids
                else []
            ),
        )
