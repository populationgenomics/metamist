from typing import Any

from fastapi import APIRouter

from api.utils.db import Connection, get_project_write_connection
from db.python.layers.cohort import CohortLayer
from models.models.cohort import CohortBody, CohortCriteria, CohortTemplate

router = APIRouter(prefix='/cohort', tags=['cohort'])


@router.post('/{project}/cohort', operation_id='createCohortFromCriteria')
async def create_cohort_from_criteria(
    cohort_spec: CohortBody,
    connection: Connection = get_project_write_connection,
    cohort_criteria: CohortCriteria = None,
) -> dict[str, Any]:
    """
    Create a cohort with the given name and sample/sequencing group IDs.
    """
    cohortlayer = CohortLayer(connection)

    if not connection.project:
        raise ValueError('A cohort must belong to a project')

    cohort_id = await cohortlayer.create_cohort_from_criteria(
        project_to_write=connection.project,
        description=cohort_spec.description,
        author=connection.author,
        cohort_name=cohort_spec.name,
        cohort_criteria=cohort_criteria,
        template_id=cohort_spec.derived_from,
    )

    return {'cohort_id': cohort_id}


@router.post('/{project}/cohort_template', operation_id='createCohortTemplate')
async def create_cohort_template(
    template: CohortTemplate,
    connection: Connection = get_project_write_connection,
) -> dict[str, Any]:
    """
    Create a cohort template with the given name and sample/sequencing group IDs.
    """
    cohortlayer = CohortLayer(connection)

    if not connection.project:
        raise ValueError('A cohort template must belong to a project')

    return await cohortlayer.create_cohort_template(
        cohort_template=template,
        project=connection.project
    )
