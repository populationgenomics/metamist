# pylint: disable=redefined-builtin, import-outside-toplevel, ungrouped-imports

from typing import TYPE_CHECKING, Annotated

from graphql import GraphQLError
import strawberry
from strawberry.types import Info
from strawberry.scalars import JSON

from api.graphql.loaders import GraphQLContext
from db.python.connect import Connection
from db.python.layers.comment import CommentLayer
from db.python.tables.project import ProjectPermissionsTable
from models.models.project import (
    ProjectMemberRole,
    ProjectMemberUpdate,
    project_member_role_names,
)
from models.models.comment import CommentEntityType

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment, GraphQLProject


@strawberry.input
class ProjectMemberUpdateInput:
    """Project member update input"""

    member: str
    roles: list[str]


@strawberry.type
class ProjectMutations:
    """Project mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: int,
        info: Info[GraphQLContext, 'ProjectMutations'],
    ) -> Annotated['GraphQLComment', strawberry.lazy('api.graphql.schema')]:
        """Add a comment to a project"""
        # Import needed here to avoid circular import
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
        cl = CommentLayer(connection)
        result = await cl.add_comment_to_entity(
            entity=CommentEntityType.project, entity_id=id, content=content
        )
        return GraphQLComment.from_internal(result)

    @strawberry.mutation
    async def create_project(
        self,
        name: str,
        dataset: str,
        create_test_project: bool,
        info: Info,
    ) -> Annotated['GraphQLProject', strawberry.lazy('api.graphql.schema')]:
        """
        Create a new project
        """
        from api.graphql.schema import GraphQLProject

        connection: Connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection)

        await ptable.check_project_creator_permissions(author=connection.author)

        pid = await ptable.create_project(
            project_name=name,
            dataset_name=dataset,
            author=connection.author,
        )

        (project,) = connection.get_and_check_access_to_projects_for_ids(
            [pid], {ProjectMemberRole.project_admin}
        )

        if create_test_project:
            await ptable.create_project(
                project_name=name + '-test',
                dataset_name=dataset,
                author=connection.author,
            )

        return GraphQLProject.from_internal(project)

    @strawberry.mutation
    async def update_project(
        self,
        project: str,
        project_update_model: JSON,
        info: Info,
    ) -> JSON:
        """Update a project by project name"""
        connection: Connection = info.context['connection']
        connection.check_access_to_projects_for_names(
            [project], {ProjectMemberRole.project_admin}
        )

        ptable = ProjectPermissionsTable(connection)
        await ptable.update_project(
            project_name=project,
            update=project_update_model,  # type: ignore [arg-type]
            author=connection.author,
        )

        return JSON({'success': True})

    @strawberry.mutation
    async def delete_project_data(
        self,
        project: str,
        delete_project: bool,
        info: Info,
    ) -> JSON:
        """
        Delete all data in a project by project name.
        """
        connection: Connection = info.context['connection']
        (target_project,) = connection.get_and_check_access_to_projects_for_names(
            [project], {ProjectMemberRole.project_admin}
        )

        ptable = ProjectPermissionsTable(connection)

        success = await ptable.delete_project_data(
            project_id=target_project.id,
            delete_project=delete_project,
        )

        return JSON({'success': success})

    @strawberry.mutation
    async def update_project_members(
        self,
        project: str,
        members: list[ProjectMemberUpdateInput],
        info: Info,
    ) -> JSON:
        """
        Update project members for specific read / write group.
        Not that this is protected by access to a specific access group
        """
        connection: Connection = info.context['connection']
        (target_project,) = connection.get_and_check_access_to_projects_for_names(
            [project], {ProjectMemberRole.project_member_admin}
        )

        ptable = ProjectPermissionsTable(connection)

        for member in members:
            for role in member.roles:
                if role not in project_member_role_names:
                    raise GraphQLError(
                        f'Role {role} is not valid for member {member.member}'
                    )
        member_objs = [
            ProjectMemberUpdate.from_dict(strawberry.asdict(member))
            for member in members
        ]
        await ptable.set_project_members(project=target_project, members=member_objs)

        return JSON({'success': True})
