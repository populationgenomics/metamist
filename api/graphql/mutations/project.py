# pylint: disable=redefined-builtin
from typing import Annotated

import strawberry
from graphql import GraphQLError
from strawberry.scalars import JSON
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from api.graphql.types.comments import GraphQLComment
from api.graphql.types.project import GraphQLProject
from db.python.connect import Connection
from db.python.layers.comment import CommentLayer
from db.python.tables.project import ProjectPermissionsTable
from models.models.comment import CommentEntityType
from models.models.project import (
    ProjectMemberRole,
    ProjectMemberUpdate,
    project_member_role_names,
)


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
    ) -> Annotated[GraphQLComment, strawberry.lazy('api.graphql.schema')]:
        """Add a comment to a project"""
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
    ) -> Annotated[GraphQLProject, strawberry.lazy('api.graphql.schema')]:
        """
        Create a new project
        """
        connection: Connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection)

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
    ) -> Annotated[GraphQLProject, strawberry.lazy('api.graphql.schema')]:
        """Update a project by project name"""
        connection: Connection = info.context['connection']
        (p,) = connection.get_and_check_access_to_projects_for_names(
            [project], {ProjectMemberRole.project_admin}
        )

        ptable = ProjectPermissionsTable(connection)
        await ptable.update_project(
            project_name=project,
            update=project_update_model,  # type: ignore [arg-type]
            author=connection.author,
        )

        await connection.refresh_projects()
        (p,) = connection.get_and_check_access_to_projects_for_names(
            [project], {ProjectMemberRole.project_admin}
        )

        return GraphQLProject.from_internal(p)

    @strawberry.mutation
    async def update_project_members(
        self,
        project: str,
        members: list[ProjectMemberUpdateInput],
        info: Info,
    ) -> Annotated[GraphQLProject, strawberry.lazy('api.graphql.schema')]:
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
        await connection.refresh_projects()

        (target_project,) = connection.get_and_check_access_to_projects_for_names(
            [project], {ProjectMemberRole.project_member_admin}
        )
        return GraphQLProject.from_internal(target_project)
