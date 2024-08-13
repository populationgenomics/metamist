from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from db.python.layers.comment import CommentLayer
from models.models.comment import CommentEntityType

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment


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
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
        cl = CommentLayer(connection)
        result = await cl.add_comment_to_entity(
            entity=CommentEntityType.project, entity_id=id, content=content
        )
        return GraphQLComment.from_internal(result)
