import strawberry
from strawberry.types import Info

from api.graphql.types import CustomJSON
from db.python.tables.project import ProjectPermissionsTable


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
        connection = info.context['connection']
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
        project: str,
        delete_project: bool,
        info: Info,
    ) -> CustomJSON:
        """
        Delete all data in a project by project name.
        """
        # TODO: Reconfigure connection permissions as per `routes``
        connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection)
        p_obj = await ptable.get_and_check_access_to_project_for_name(
            user=connection.author, project_name=project, readonly=False
        )
        success = await ptable.delete_project_data(
            project_id=p_obj.id, delete_project=delete_project, author=connection.author
        )

        return CustomJSON({'success': success})

    @strawberry.mutation
    async def update_project_members(
        self,
        project: str,
        members: list[str],
        readonly: bool,
        info: Info,
    ) -> CustomJSON:
        """
        Update project members for specific read / write group.
        Not that this is protected by access to a specific access group
        """
        # TODO: Reconfigure connection permissions as per `routes``
        connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection)
        await ptable.set_group_members(
            group_name=ptable.get_project_group_name(project, readonly=readonly),
            members=members,
            author=connection.author,
        )

        return CustomJSON({'success': True})
