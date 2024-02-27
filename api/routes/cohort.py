from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from api.utils.db import Connection, get_project_write_connection

from db.python.layers.cohort import CohortLayer
from db.python.tables.project import ProjectPermissionsTable

from models.models.cohort import CohortBody, CohortCriteria, CohortTemplate
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_transform_to_raw_list,
)

router = APIRouter(prefix='/cohort', tags=['cohort'])


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
        sg_ids_internal=sequencing_group_id_transform_to_raw_list(cohort_criteria.sg_ids_internal),
        exlude_sg_ids_internal=sequencing_group_id_transform_to_raw_list(cohort_criteria.excluded_sgs_internal),
        sg_technology=cohort_criteria.sg_technology,
        sg_platform=cohort_criteria.sg_platform,
        sg_type=cohort_criteria.sg_type,
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
    )

