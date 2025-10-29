from fastapi import APIRouter

from api.utils.db import (
    Connection,
    get_project_db_connection,
    get_projectless_db_connection,
)
from db.python.filters import GenericFilter
from db.python.layers.cohort import CohortLayer
from db.python.tables.cohort import CohortFilter
from models.models.cohort import (
    CohortBody,
    CohortCriteria,
    CohortTemplate,
    NewCohort,
    CohortUpdateBody,
    CohortExternal,
)
from models.models.project import (
    FullWriteAccessRoles,
    ProjectId,
    ProjectMemberRole,
    ReadAccessRoles,
)
from models.utils.cohort_template_id_format import (
    cohort_template_id_format,
    cohort_template_id_transform_to_raw,
)

router = APIRouter(prefix='/cohort', tags=['cohort'])


@router.post('/{project}/cohort', operation_id='createCohortFromCriteria')
async def create_cohort_from_criteria(
    cohort_spec: CohortBody,
    cohort_criteria: CohortCriteria | None = None,
    connection: Connection = get_project_db_connection(
        {ProjectMemberRole.writer, ProjectMemberRole.contributor}
    ),
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
    connection: Connection = get_project_db_connection(
        {ProjectMemberRole.writer, ProjectMemberRole.contributor}
    ),
) -> str:
    """
    Create a cohort template with the given name and sample/sequencing group IDs.
    """
    cohort_layer = CohortLayer(connection)

    if not connection.project:
        raise ValueError('A cohort template must belong to a project')

    criteria_project_ids: list[ProjectId] = []

    if template.criteria.projects:
        projects_for_criteria = connection.get_and_check_access_to_projects_for_names(
            project_names=template.criteria.projects,
            allowed_roles=FullWriteAccessRoles,
        )
        criteria_project_ids = [p.id for p in projects_for_criteria if p.id]

    assert connection.project_id
    cohort_raw_id = await cohort_layer.create_cohort_template(
        cohort_template=template.to_internal(
            criteria_projects=criteria_project_ids,
            template_project=connection.project_id,
        ),
        project=connection.project_id,
    )

    return cohort_template_id_format(cohort_raw_id)

@router.get('/{cohort_id}', operation_id='getCohortById')
async def get_cohort_by_id(
    cohort_id: int, connection: Connection = get_projectless_db_connection
) -> CohortExternal:

    """Get cohort by ID"""

    cohort_layer = CohortLayer(connection)
    resp = await cohort_layer.query(CohortFilter(id=GenericFilter(eq=cohort_id)))

    if not resp:
        raise ValueError(f'Cohort with ID {cohort_id} not found')

    return resp[0].to_external()


@router.patch('/{cohort_id}', operation_id='updateCohortById')
async def update_cohort_by_id(
    cohort_id: int,
    cohort: CohortUpdateBody,
    connection: Connection = get_projectless_db_connection
) -> CohortExternal:

    """update cohort by ID"""

    cohort_layer = CohortLayer(connection)
    await cohort_layer.update_cohort(cohort, cohort_id)
    updated_cohort = (await cohort_layer.query(CohortFilter(id=GenericFilter(eq=cohort_id))))[0]
    return updated_cohort.to_external()