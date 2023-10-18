from typing import List, Optional

from fastapi import APIRouter

from api.utils.db import Connection, get_projectless_db_connection
from db.python.tables.project import ProjectPermissionsTable
from models.models.project import Project

router = APIRouter(prefix='/project', tags=['project'])


@router.get('/all', operation_id='getAllProjects', response_model=List[Project])
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
    read_group_name: Optional[str] = None,
    write_group_name: Optional[str] = None,
    create_test_project: bool = True,
    connection: Connection = get_projectless_db_connection,
) -> int:
    """
    Create a new project
    """
    if not read_group_name:
        read_group_name = f'{dataset}-sample-metadata-main-read'
    if not write_group_name:
        write_group_name = f'{dataset}-sample-metadata-main-write'

    ptable = ProjectPermissionsTable(connection.connection)
    pid = await ptable.create_project(
        project_name=name,
        dataset_name=dataset,
        read_group_name=read_group_name,
        write_group_name=write_group_name,
        create_test_project=create_test_project,
        author=connection.author,
    )

    return pid


@router.get('/seqr/all', operation_id='getSeqrProjects')
async def get_seqr_projects(connection: Connection = get_projectless_db_connection):
    """Get SM projects that should sync to seqr"""
    # ptable = ProjectPermissionsTable(connection.connection)
    # return await ptable.get_seqr_projects()
    return [
        {'id': 1, 'name': 'greek-myth'},
        {'id': 2, 'name': 'test-dataset-2'},
        {'id': 3, 'name': 'test-dataset-3'},
    ]


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


@router.delete('/{project}', operation_id='deleteProjectData')
async def delete_project_data(
    project: str,
    delete_project: bool = False,
    connection: Connection = get_projectless_db_connection,
):
    """
    Delete all data in a project by project name.
    Can optionally delete the project itself.
    Requires READ access + project-creator permissions
    """
    ptable = ProjectPermissionsTable(connection.connection)
    pid = await ptable.get_project_id_from_name_and_user(
        connection.author, project, readonly=False
    )
    success = await ptable.delete_project_data(
        project_id=pid, delete_project=delete_project, author=connection.author
    )

    return {'success': success}
