from typing import List, Optional

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
    read_secret_name: Optional[str],
    write_secret_name: Optional[str],
    connection: Connection = get_projectless_db_connection,
) -> int:
    """Get sample by external ID"""
    ptable = ProjectPermissionsTable(connection.connection)
    pid = await ptable.create_project(
        project_name=name,
        dataset_name=dataset,
        gcp_project_id=gcp_id,
        read_secret_name=read_secret_name,
        write_secret_name=write_secret_name,
        author=connection.author,
    )

    return pid
