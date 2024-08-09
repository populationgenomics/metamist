from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from db.python.layers.comment import CommentLayer

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment


@strawberry.type
class CommentMutations:
    """Comment mutations"""

    @strawberry.mutation
    async def add_comment_to_thread(
        self,
        parent_id: int,
        content: str,
        info: Info[GraphQLContext, 'CommentMutations'],
    ) -> Annotated['GraphQLComment', strawberry.lazy('api.graphql.schema')]:
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
        comment_layer = CommentLayer(connection)
        comment = await comment_layer.add_comment_to_thread(parent_id, content)
        return GraphQLComment.from_internal(comment)

    @strawberry.mutation
    async def update_comment(
        self,
        id: int,
        content: str,
        info: Info[GraphQLContext, 'CommentMutations'],
    ) -> Annotated['GraphQLComment', strawberry.lazy('api.graphql.schema')]:
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
        comment_layer = CommentLayer(connection)
        comment = await comment_layer.update_comment(id, content)

        return GraphQLComment.from_internal(comment)

    @strawberry.mutation
    async def delete_comment(
        self, id: int, info: Info[GraphQLContext, 'CommentMutations']
    ) -> Annotated['GraphQLComment', strawberry.lazy('api.graphql.schema')]:
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
        comment_layer = CommentLayer(connection)
        comment = await comment_layer.delete_comment(id)
        return GraphQLComment.from_internal(comment)

    @strawberry.mutation
    async def restore_comment(
        self, id: int, info: Info[GraphQLContext, 'CommentMutations']
    ) -> Annotated['GraphQLComment', strawberry.lazy('api.graphql.schema')]:
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
        comment_layer = CommentLayer(connection)
        comment = await comment_layer.restore_comment(id)
        return GraphQLComment.from_internal(comment)
