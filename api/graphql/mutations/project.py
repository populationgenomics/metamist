from typing import TYPE_CHECKING

import strawberry
from strawberry.types import Info
from typing_extensions import Annotated

from api.graphql.types import CustomJSON
from db.python.tables.project import ProjectPermissionsTable

if TYPE_CHECKING:
    from ..schema import GraphQLProject


@strawberry.type
class ProjectMutations:
    """Project mutations"""

    @strawberry.mutation
    async def get_all_projects(
        self,
        info: Info,
    ) -> Annotated['GraphQLProject', strawberry.lazy('..schema')]:
        """Get list of projects"""
        # TODO: Reconfigure connection permissions as per `routes``
        connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection)

        #  pylint: disable=no-member
        return Annotated[
            'GraphQLProject', strawberry.lazy('..schema')
        ].from_internal(  # type: ignore [attr-defined]
            await ptable.get_all_projects(author=connection.author)
        )

    @strawberry.mutation
    async def get_my_projects(
        self,
        info: Info,
    ) -> list[str | None]:
        """Get projects I have access to"""
        # TODO: Reconfigure connection permissions as per `routes``
        connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection)
        projects = await ptable.get_projects_accessible_by_user(
            author=connection.author, readonly=True
        )
        return [p.name for p in projects]

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
    async def get_seqr_projects(
        self,
        info: Info,
    ) -> list[CustomJSON]:
        """Get SM projects that should sync to seqr"""
        # TODO: Reconfigure connection permissions as per `routes``
        connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection)
        return [CustomJSON(project) for project in await ptable.get_seqr_projects()]

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
