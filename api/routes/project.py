from typing import List, Optional

from fastapi import APIRouter

from api.utils.db import Connection, get_projectless_db_connection
from db.python.tables.project import ProjectPermissionsTable
from models.models.project import ProjectRow

router = APIRouter(prefix='/project', tags=['project'])


@router.get('/all', operation_id='getAllProjects', response_model=List[ProjectRow])
async def get_all_projects(connection=get_projectless_db_connection):
    """Get list of projects"""
    ptable = ProjectPermissionsTable(connection.connection)
    return await ptable.get_project_rows(
        author=connection.author, check_permissions=False
    )


@router.get('/', operation_id='getMyProjects', response_model=List[str])
async def get_my_projects(connection=get_projectless_db_connection):
    """Get projects I have access to"""
    ptable = ProjectPermissionsTable(connection.connection)
    pmap = await ptable.get_projects_accessible_by_user(
        author=connection.author, readonly=True
    )
    return list(pmap.values())


@router.put('/', operation_id='createProject')
async def create_project(
    name: str,
    dataset: str,
    gcp_id: str,
    read_secret_name: Optional[str] = None,
    write_secret_name: Optional[str] = None,
    create_test_project: bool = True,
    connection: Connection = get_projectless_db_connection,
) -> int:
    """
    Create a new project
    """
    if not read_secret_name:
        read_secret_name = f'{dataset}-sample-metadata-main-read-members-cache'
    if not write_secret_name:
        write_secret_name = f'{dataset}-sample-metadata-main-write-members-cache'

    ptable = ProjectPermissionsTable(connection.connection)
    pid = await ptable.create_project(
        project_name=name,
        dataset_name=dataset,
        gcp_project_id=gcp_id,
        read_secret_name=read_secret_name,
        write_secret_name=write_secret_name,
        create_test_project=create_test_project,
        author=connection.author,
    )

    return pid


@router.get('/seqr/all', operation_id='getSeqrProjects')
async def get_seqr_projects(connection: Connection = get_projectless_db_connection):
    """Get SM projects that should sync to seqr"""
    ptable = ProjectPermissionsTable(connection.connection)
    return await ptable.get_seqr_projects()


@router.post('/{project}/update', operation_id='updateProject')
async def update_project(
    project: str,
    project_update_model: dict,
    connection: Connection = get_projectless_db_connection,
):
    """Update a project by project name"""
    ptable = ProjectPermissionsTable(connection.connection)
    return await ptable.update_project(
        project_name=project, update=project_update_model, author=connection.author
    )


@router.delete('/{project}', operation_id='deleteProject')
async def delete_project(
    project: str,
    connection: Connection = get_projectless_db_connection,
):
    """
    Delete a project by project name.
    Requires READ access + project-creator permissions
    """
    ptable = ProjectPermissionsTable(connection.connection)
    pid = await ptable.get_project_id_from_name_and_user(
        connection.author, project, readonly=False
    )
    success = await ptable.delete_project(project_id=pid, author=connection.author)

    return {'success': success}
