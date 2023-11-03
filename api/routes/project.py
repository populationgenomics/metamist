from typing import List

from fastapi import APIRouter

from api.utils.db import Connection, get_projectless_db_connection
from db.python.tables.project import ProjectPermissionsTable
from models.models.project import Project

router = APIRouter(prefix='/project', tags=['project'])


@router.get('/all', operation_id='getAllProjects', response_model=List[Project])
async def get_all_projects(connection=get_projectless_db_connection):
    """Get list of projects"""
    ptable = ProjectPermissionsTable(connection)
    return await ptable.get_all_projects(author=connection.author)


@router.get('/', operation_id='getMyProjects', response_model=List[str])
async def get_my_projects(connection=get_projectless_db_connection):
    """Get projects I have access to"""
    ptable = ProjectPermissionsTable(connection)
    projects = await ptable.get_projects_accessible_by_user(
        author=connection.author, readonly=True
    )
    return [p.name for p in projects]


@router.put('/', operation_id='createProject')
async def create_project(
    name: str,
    dataset: str,
    create_test_project: bool = False,
    connection: Connection = get_projectless_db_connection,
) -> int:
    """
    Create a new project
    """
    ptable = ProjectPermissionsTable(connection)
    pid = await ptable.create_project(
        project_name=name,
        dataset_name=dataset,
        author=connection.author,
    )

    if create_test_project:
        await ptable.create_project(
            project_name=name + '-test',
            dataset_name=dataset,
            author=connection.author,
        )

    return pid


@router.get('/seqr/all', operation_id='getSeqrProjects')
async def get_seqr_projects(connection: Connection = get_projectless_db_connection):
    """Get SM projects that should sync to seqr"""
    ptable = ProjectPermissionsTable(connection)
    return await ptable.get_seqr_projects()


@router.post('/{project}/update', operation_id='updateProject')
async def update_project(
    project: str,
    project_update_model: dict,
    connection: Connection = get_projectless_db_connection,
):
    """Update a project by project name"""
    ptable = ProjectPermissionsTable(connection)
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
    ptable = ProjectPermissionsTable(connection)
    p_obj = await ptable.get_and_check_access_to_project_for_name(
        user=connection.author, project_name=project, readonly=False
    )
    success = await ptable.delete_project_data(
        project_id=p_obj.id, delete_project=delete_project, author=connection.author
    )

    return {'success': success}


@router.patch('/{project}/members', operation_id='updateProjectMembers')
async def update_project_members(
    project: str,
    members: list[str],
    readonly: bool,
    connection: Connection = get_projectless_db_connection,
):
    """
    Update project members for specific read / write group.
    Not that this is protected by access to a specific access group
    """
    ptable = ProjectPermissionsTable(connection)
    await ptable.set_group_members(
        group_name=ptable.get_project_group_name(project, readonly=readonly),
        members=members,
        author=connection.author,
    )

    return {'success': True}
