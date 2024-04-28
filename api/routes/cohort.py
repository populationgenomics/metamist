from fastapi import APIRouter

from api.utils.db import Connection, get_project_readonly_connection
from db.python.layers.cohort import CohortLayer
from db.python.tables.project import ProjectPermissionsTable
from models.models.cohort import CohortBody, CohortCriteria, CohortTemplate, NewCohort
from models.models.project import ProjectId
from models.utils.cohort_template_id_format import (
    cohort_template_id_format,
    cohort_template_id_transform_to_raw,
)

router = APIRouter(prefix='/cohort', tags=['cohort'])


@router.post('/{project}/cohort', operation_id='createCohortFromCriteria')
async def create_cohort_from_criteria(
    cohort_spec: CohortBody,
    cohort_criteria: CohortCriteria | None = None,
    connection: Connection = get_project_readonly_connection,
    dry_run: bool = False,
) -> NewCohort:
    """
    Create a cohort with the given name and sample/sequencing group IDs.
    """
    cohort_layer = CohortLayer(connection)

    if not connection.project:
        raise ValueError('A cohort must belong to a project')

    if not cohort_criteria and not cohort_spec.template_id:
        raise ValueError(
            'A cohort must have either criteria or be derived from a template'
        )

    internal_project_ids: list[ProjectId] = []

    if cohort_criteria:
        if cohort_criteria.projects:
            pt = ProjectPermissionsTable(connection)
            projects = await pt.get_and_check_access_to_projects_for_names(
                user=connection.author,
                project_names=cohort_criteria.projects,
                readonly=True,
            )
            if projects:
                internal_project_ids = [p.id for p in projects if p.id]

    template_id_raw = (
        cohort_template_id_transform_to_raw(cohort_spec.template_id)
        if cohort_spec.template_id
        else None
    )
    cohort_output = await cohort_layer.create_cohort_from_criteria(
        project_to_write=connection.project,
        description=cohort_spec.description,
        cohort_name=cohort_spec.name,
        dry_run=dry_run,
        cohort_criteria=(
            cohort_criteria.to_internal(internal_project_ids)
            if cohort_criteria
            else None
        ),
        template_id=template_id_raw,
    )

    return cohort_output.to_external()


@router.post('/{project}/cohort_template', operation_id='createCohortTemplate')
async def create_cohort_template(
    template: CohortTemplate,
    connection: Connection = get_project_readonly_connection,
) -> str:
    """
    Create a cohort template with the given name and sample/sequencing group IDs.
    """
    cohort_layer = CohortLayer(connection)

    if not connection.project:
        raise ValueError('A cohort template must belong to a project')

    criteria_project_ids: list[ProjectId] = []

    if template.criteria.projects:
        pt = ProjectPermissionsTable(connection)
        projects_for_criteria = await pt.get_and_check_access_to_projects_for_names(
            user=connection.author,
            project_names=template.criteria.projects,
            readonly=False,
        )
        if projects_for_criteria:
            criteria_project_ids = [p.id for p in projects_for_criteria if p.id]

    cohort_raw_id = await cohort_layer.create_cohort_template(
        cohort_template=template.to_internal(criteria_projects=criteria_project_ids),
        project=connection.project,
    )

    return cohort_template_id_format(cohort_raw_id)
