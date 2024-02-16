from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from api.utils.db import Connection, get_project_write_connection
from db.python.layers.cohort import CohortLayer

from db.python.tables.project import ProjectPermissionsTable

router = APIRouter(prefix='/cohort', tags=['cohort'])


class CohortBody(BaseModel):
    """Represents the expected JSON body of the create cohort request"""

    name: str
    description: str
    derived_from: int | None = None


class CohortCriteria(BaseModel):
    """Represents the expected JSON body of the create cohort request"""

    projects: list[str]


@router.post('/{project}/', operation_id='createCohort')
async def create_cohort(
    cohort: CohortBody,
    connection: Connection = get_project_write_connection,
) -> dict[str, Any]:
    """
    Create a cohort with the given name and sample/sequencing group IDs.
    """
    cohortlayer = CohortLayer(connection)

    if not connection.project:
        raise ValueError('A cohort must belong to a project')

    cohort_id = await cohortlayer.create_cohort(
        project=connection.project,
        cohort_name=cohort.name,
        derived_from=cohort.derived_from,
        description=cohort.description,
        author=connection.author,
        sequencing_group_ids=cohort.sequencing_group_ids,
    )

    return {'cohort_id': cohort_id}


@router.post('/{project}/cohort', operation_id='createCohortFromCriteria')
async def create_cohort_from_criteria(
    cohort_spec: CohortBody,
    cohort_criteria: CohortCriteria,
    connection: Connection = get_project_write_connection,
) -> dict[str, Any]:
    """
    Create a cohort with the given name and sample/sequencing group IDs.
    """
    cohortlayer = CohortLayer(connection)

    if not connection.project:
        raise ValueError('A cohort must belong to a project')

    pt = ProjectPermissionsTable(connection)
    projects_to_pull = await pt.get_and_check_access_to_projects_for_names(
        user=connection.author, project_names=cohort_criteria.projects, readonly=True
    )
    projects_to_pull = [p.id for p in projects_to_pull]

    cohort_id = await cohortlayer.create_cohort_from_criteria(
        project_to_write=connection.project,
        projects_to_pull=projects_to_pull,
        description=cohort_spec.description,
        author=connection.author,
        cohort_name=cohort_spec.name,
    )

    return {'cohort_id': cohort_id}
