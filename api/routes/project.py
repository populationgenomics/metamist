from fastapi import APIRouter, HTTPException

from api.utils.db import (
    Connection,
    get_project_db_connection,
    get_projectless_db_connection,
)
from db.python.tables.project import ProjectPermissionsTable
from db.python.utils import Forbidden
from models.models.project import (
    FullWriteAccessRoles,
    Project,
    ProjectMemberRole,
    ProjectMemberUpdate,
    ReadAccessRoles,
    project_member_role_names,
)

router = APIRouter(prefix='/project', tags=['project'])


@router.get('/all', operation_id='getAllProjects', response_model=list[Project])
async def get_all_projects(connection: Connection = get_projectless_db_connection):
    """Get list of projects"""
    return connection.all_projects()


@router.get('/', operation_id='getMyProjects', response_model=list[str])
async def get_my_projects(connection: Connection = get_projectless_db_connection):
    """Get projects I have access to"""
    return [
        p.name
        for p in connection.projects_with_role(
            ReadAccessRoles.union(FullWriteAccessRoles)
        )
    ]


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

    # Creating a project requires the project creator permission
    await ptable.check_project_creator_permissions(author=connection.author)

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
    seqr_project_ids = await ptable.get_seqr_project_ids()
    my_seqr_projects = [
        p for p in connection.all_projects() if p.id in seqr_project_ids
    ]

    # Fail if user doesn't have access to all seqr projects. This endpoint is used
    # for joint-calling where we would want to include all seqr projects and know if
    # any are missing, so it is important to raise an error here rather than just
    # excluding projects due to permission issues
    if len(my_seqr_projects) != len(seqr_project_ids):
        raise Forbidden('The current user does not have access to all seqr projects')

    return my_seqr_projects


@router.post('/{project}/update', operation_id='updateProject')
async def update_project(
    project_update_model: dict,
    connection: Connection = get_project_db_connection(
        {ProjectMemberRole.project_admin}
    ),
):
    """Update a project by project name"""
    ptable = ProjectPermissionsTable(connection)

    project = connection.project
    assert project
    return await ptable.update_project(
        project_name=project.name, update=project_update_model, author=connection.author
    )


@router.delete('/{project}', operation_id='deleteProjectData')
async def delete_project_data(
    delete_project: bool = False,
    connection: Connection = get_project_db_connection(
        {ProjectMemberRole.project_admin}
    ),
):
    """
    Delete all data in a project by project name.
    Can optionally delete the project itself.
    Requires READ access + project-creator permissions
    """
    if delete_project:
        # stop allowing delete project with analysis-runner entries
        raise ValueError('2024-03-08: delete_project is no longer allowed')

    ptable = ProjectPermissionsTable(connection)

    assert connection.project
    success = await ptable.delete_project_data(project=connection.project)
    return {'success': success}


@router.patch('/{project}/members', operation_id='updateProjectMembers')
async def update_project_members(
    members: list[ProjectMemberUpdate],
    connection: Connection = get_project_db_connection(
        {ProjectMemberRole.project_member_admin}
    ),
):
    """
    Update project members for specific read / write group.
    Not that this is protected by access to a specific access group
    """
    ptable = ProjectPermissionsTable(connection)

    for member in members:
        for role in member.roles:
            if role not in project_member_role_names:
                raise HTTPException(
                    400, f'Role {role} is not valid for member {member.member}'
                )
    assert connection.project
    await ptable.set_project_members(project=connection.project, members=members)

    return {'success': True}
