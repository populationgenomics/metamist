from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from api.utils.db import Connection, get_project_write_connection
from db.python.layers.cohort import CohortLayer

router = APIRouter(prefix='/cohort', tags=['cohort'])


class CohortBody(BaseModel):
    name: str
    description: str
    sequencing_group_ids: list[str]
    derived_from: int | None = None


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
