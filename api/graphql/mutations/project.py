import strawberry
from graphql import GraphQLError
from strawberry.types import Info

from api.graphql.types import CustomJSON, ProjectMemberUpdateInput
from db.python.connect import Connection
from db.python.tables.project import ProjectPermissionsTable
from models.models.project import (
    ProjectMemberRole,
    ProjectMemberUpdate,
    updatable_project_member_role_names,
)


@strawberry.type
class ProjectMutations:
    """Project mutations"""

    @strawberry.mutation
    async def create_project(
        self,
        name: str,
        dataset: str,
        create_test_project: bool,
        info: Info,
    ) -> int:
        """
        Create a new project
        """
        # TODO: Reconfigure connection permissions as per `routes``
        connection: Connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection)

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

    @strawberry.mutation
    async def update_project(
        self,
        project: str,
        project_update_model: strawberry.scalars.JSON,
        info: Info,
    ) -> CustomJSON:
        """Update a project by project name"""
        # TODO: Reconfigure connection permissions as per `routes``
        connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection)
        await ptable.update_project(
            project_name=project, update=project_update_model, author=connection.author
        )

        return CustomJSON({'success': True})

    @strawberry.mutation
    async def delete_project_data(
        self,
        delete_project: bool,
        info: Info,
    ) -> CustomJSON:
        """
        Delete all data in a project by project name.
        """
        # TODO: Reconfigure connection permissions as per `routes``
        connection: Connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection)
        assert connection.project

        # Allow data manager role to delete test projects
        data_manager_deleting_test = (
            connection.project.is_test
            and connection.project.roles & {ProjectMemberRole.data_manager}
        )
        if not data_manager_deleting_test:
            # Otherwise, deleting a project additionally requires the project creator permission
            await ptable.check_project_creator_permissions(author=connection.author)

        success = await ptable.delete_project_data(
            project_id=connection.project.id,
            delete_project=delete_project,
        )

        return CustomJSON({'success': success})

    @strawberry.mutation
    async def update_project_members(
        self,
        members: list[ProjectMemberUpdateInput],
        info: Info,
    ) -> CustomJSON:
        """
        Update project members for specific read / write group.
        Not that this is protected by access to a specific access group
        """
        # TODO: Reconfigure connection permissions as per `routes``
        connection: Connection = info.context['connection']
        connection.check_access({ProjectMemberRole.project_member_admin})
        ptable = ProjectPermissionsTable(connection)

        await ptable.check_member_admin_permissions(author=connection.author)

        for member in members:
            for role in member.roles:
                if role not in updatable_project_member_role_names:
                    raise GraphQLError(
                        f'Role {role} is not valid for member {member.member}'
                    )
        member_objs = [
            ProjectMemberUpdate.from_dict(strawberry.asdict(member))
            for member in members
        ]
        assert connection.project
        await ptable.set_project_members(
            project=connection.project, members=member_objs
        )

        return CustomJSON({'success': True})
