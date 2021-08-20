from typing import List

from fastapi import APIRouter

from api.utils.db import Connection, get_projectless_db_connection
from db.python.tables.project import ProjectPermissionsTable
from models.models.project import ProjectRow

router = APIRouter(prefix='/project', tags=['project'])


@router.get('/', operation_id='getProjects', response_model=List[ProjectRow])
async def get_projects(connection=get_projectless_db_connection):
    """Get list of projects"""
    ptable = ProjectPermissionsTable(connection.connection)
    return await ptable.get_project_rows(author=connection.author)


@router.put('/', operation_id='createProject')
async def create_project(
    name: str,
    dataset: str,
    gcp_id: str,
    connection: Connection = get_projectless_db_connection,
) -> int:
    """Get sample by external ID"""
    ptable = ProjectPermissionsTable(connection.connection)
    pid = await ptable.create_project(
        project_name=name,
        dataset_name=dataset,
        gcp_project_id=gcp_id,
        author=connection.author,
    )

    return pid
