from typing import Any

from fastapi import APIRouter

from api.utils.db import Connection, get_project_write_connection
from db.python.layers.cohort import CohortLayer

router = APIRouter(prefix='/cohort', tags=['cohort'])


@router.post('/{project}/', operation_id='createCohort')
async def create_cohort(
    cohort_name: str,
    description: str,
    sequencing_group_ids: list[str],
    derived_from: int | None = None,
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
        cohort_name=cohort_name,
        derived_from=derived_from,
        description=description,
        author=connection.author,
        sequencing_group_ids=sequencing_group_ids,
    )

    return {'cohort_id': cohort_id}
