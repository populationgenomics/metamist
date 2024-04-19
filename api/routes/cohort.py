from typing import Any

from fastapi import APIRouter

from api.utils.db import Connection, get_project_write_connection
from db.python.layers.cohort import CohortLayer
from models.models.cohort import CohortBody, CohortCriteria, CohortTemplate, NewCohort

router = APIRouter(prefix='/cohort', tags=['cohort'])


@router.post('/{project}/cohort', operation_id='createCohortFromCriteria')
async def create_cohort_from_criteria(
    cohort_spec: CohortBody,
    connection: Connection = get_project_write_connection,
    cohort_criteria: CohortCriteria = None,
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

    cohort_output = await cohort_layer.create_cohort_from_criteria(
        project_to_write=connection.project,
        description=cohort_spec.description,
        cohort_name=cohort_spec.name,
        dry_run=dry_run,
        cohort_criteria=cohort_criteria,
        template_id=cohort_spec.template_id,
    )

    return cohort_output


@router.post('/{project}/cohort_template', operation_id='createCohortTemplate')
async def create_cohort_template(
    template: CohortTemplate,
    connection: Connection = get_project_write_connection,
) -> dict[str, Any]:
    """
    Create a cohort template with the given name and sample/sequencing group IDs.
    """
    cohort_layer = CohortLayer(connection)

    if not connection.project:
        raise ValueError('A cohort template must belong to a project')

    return await cohort_layer.create_cohort_template(
        cohort_template=template, project=connection.project
    )
