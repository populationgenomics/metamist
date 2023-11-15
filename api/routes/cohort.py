from typing import Any

from fastapi import APIRouter

from api.utils.db import (Connection, get_project_readonly_connection,
                          get_project_write_connection,
                          get_projectless_db_connection)
from db.python.layers.cohort import CohortLayer

router = APIRouter(prefix='/cohort', tags=['cohort'])


@router.put('/', operation_id='getSGsForCohort')
async def get_sgs_for_cohort(
    projects: list[str],
    connection: Connection = get_projectless_db_connection,
):
    """
    Get all sequencing groups for a cohort
    """
    # We should probably do this project per project?
    cohortlayer = CohortLayer(connection)
    return await cohortlayer.get_sgs_for_cohort(projects)

@router.post('/{project}/', operation_id='createCohort')
async def create_cohort(
    cohort_name: str,
    sequencing_group_ids: list[str],
    description: str,
    connection: Connection = get_project_write_connection,
) -> dict[str, Any]:
    """
    Create a cohort with the given name and sample/sequencing group IDs.
    """
    cohortlayer = CohortLayer(connection)
    cohort_id = await cohortlayer.create_cohort(project=connection.project, cohort_name=cohort_name, sequencing_group_ids=sequencing_group_ids, description=description,author=connection.author)
    #sequencing_group_ids = sequencing_group_id_transform_to_raw_list(sequencing_group_ids)
    return {'cohort_id': cohort_id}
