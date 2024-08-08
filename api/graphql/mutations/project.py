import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from db.python.tables.comment import CommentTable
from models.models.comment import CommentEntityType
from models.models.project import ProjectMemberRole


@strawberry.type
class ProjectMutations:
    """Project mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: int,
        info: Info[GraphQLContext, 'ProjectMutations'],
    ) -> int:
        """Add a comment to a project"""
        connection = info.context['connection']
        connection.check_access_to_projects_for_ids(
            [id],
            allowed_roles={ProjectMemberRole.writer, ProjectMemberRole.contributor},
        )
        ct = CommentTable(connection)
        return await ct.add_comment(CommentEntityType.project, id, content)
