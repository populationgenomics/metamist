import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext


@strawberry.type
class CommentMutations:
    """Comment mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        message: str,
        thread_id: int | None,
        info: Info[GraphQLContext, 'CommentMutations'],
    ) -> int:
        """Create a new comment"""
        # connection: Connection = info.context['connection']

        print(message)
        return 1

    # @strawberry.mutation
    # async def remove_comment(
    #     self,

    # @strawberry.mutation
    # async def edit_comment(
    #     self,
    #     comment_id: int,
    #     message: str,
    #     info: Info,
    # ) -> int:
    #     """Create a new comment"""
    #     # connection: Connection = info.context['connection']

    #     print(message)
    #     return 1
