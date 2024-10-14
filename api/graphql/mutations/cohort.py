import strawberry
from strawberry.types import Info

from api.graphql.types import NewCohortType
from db.python.connect import Connection
from db.python.layers.cohort import CohortLayer
from models.models.cohort import CohortCriteria, CohortTemplate
from models.models.project import ProjectId, ProjectMemberRole, ReadAccessRoles
from models.utils.cohort_template_id_format import (
    cohort_template_id_format,
    cohort_template_id_transform_to_raw,
)


@strawberry.input
class CohortCriteriaInput:
    """Cohort criteria"""

    projects: list[str] | None
    sg_ids_internal: list[str] | None
    excluded_sgs_internal: list[str] | None
    sg_technology: list[str] | None
    sg_platform: list[str] | None
    sg_type: list[str] | None
    sample_type: list[str] | None


@strawberry.input
class CohortBodyInput:
    """Cohort body"""

    name: str
    description: str
    template_id: str | None


@strawberry.input
class CohortTemplateInput:
    """Cohort template input"""

    id: int | None
    name: str
    description: str
    criteria: CohortCriteriaInput


@strawberry.type
class CohortMutations:
    """Cohort mutations"""

    @strawberry.mutation
    async def create_cohort_from_criteria(
        self,
        info: Info,
        cohort_spec: CohortBodyInput,
        cohort_criteria: CohortCriteriaInput | None = None,
        dry_run: bool = False,
    ) -> NewCohortType:
        """
        Create a cohort with the given name and sample/sequencing group IDs.
        """
        connection: Connection = info.context['connection']
        connection.check_access(
            {ProjectMemberRole.writer, ProjectMemberRole.contributor}
        )
        cohort_layer = CohortLayer(connection)

        if not connection.project:
            raise ValueError('A cohort must belong to a project')

        if not cohort_criteria and not cohort_spec.template_id:
            raise ValueError(
                'A cohort must have either criteria or be derived from a template'
            )

        internal_project_ids: list[ProjectId] = []

        if cohort_criteria and cohort_criteria.projects:
            projects = connection.get_and_check_access_to_projects_for_names(
                project_names=cohort_criteria.projects, allowed_roles=ReadAccessRoles
            )

            internal_project_ids = [p.id for p in projects if p.id]

        template_id_raw = (
            cohort_template_id_transform_to_raw(cohort_spec.template_id)
            if cohort_spec.template_id
            else None
        )
        assert connection.project_id
        cohort_output = await cohort_layer.create_cohort_from_criteria(
            project_to_write=connection.project_id,
            description=cohort_spec.description,
            cohort_name=cohort_spec.name,
            dry_run=dry_run,
            cohort_criteria=(
                CohortCriteria.from_dict(
                    strawberry.asdict(cohort_criteria)
                ).to_internal(internal_project_ids)
                if cohort_criteria
                else None
            ),
            template_id=template_id_raw,
        )

        return NewCohortType.from_internal(cohort_output)

    @strawberry.mutation
    async def create_cohort_template(
        self,
        template: CohortTemplateInput,
        info: Info,
    ) -> str:
        """
        Create a cohort template with the given name and sample/sequencing group IDs.
        """
        connection: Connection = info.context['connection']
        connection.check_access(
            {ProjectMemberRole.writer, ProjectMemberRole.contributor}
        )
        cohort_layer = CohortLayer(connection)

        if not connection.project:
            raise ValueError('A cohort template must belong to a project')

        criteria_project_ids: list[ProjectId] = []

        if template.criteria.projects:
            projects_for_criteria = (
                connection.get_and_check_access_to_projects_for_names(
                    project_names=template.criteria.projects,
                    allowed_roles=ReadAccessRoles,
                )
            )
            criteria_project_ids = [p.id for p in projects_for_criteria if p.id]

        assert connection.project_id
        cohort_raw_id = await cohort_layer.create_cohort_template(
            cohort_template=CohortTemplate.from_dict(
                strawberry.asdict(template)
            ).to_internal(
                criteria_projects=criteria_project_ids,
                template_project=connection.project_id,
            ),
            project=connection.project_id,
        )

        return cohort_template_id_format(cohort_raw_id)
