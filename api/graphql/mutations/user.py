import strawberry
from strawberry.types import Info
from typing import Optional, Any

from db.python.layers.user import UserLayer
from api.graphql.schema import GraphQLUser
from api.graphql.loaders import GraphQLContext


@strawberry.type
class UserMutations:
    """GraphQL mutations for creating and updating users."""

    @strawberry.mutation
    async def create_user(
        self,
        info: Info[GraphQLContext, None],
        email: str,
        full_name: Optional[str] = None,
        settings: Optional[Any] = None,
    ) -> GraphQLUser:
        """Create a new user with the given email, full name, and settings."""
        connection = info.context['connection']
        user_layer = UserLayer(connection)
        user_id = await user_layer.create_user(email, full_name, settings)
        user = await user_layer.get_user_by_id(user_id)

        if user is None:
            raise ValueError('Failed to create user')

        return GraphQLUser.from_internal(user)

    @strawberry.mutation
    async def update_user(
        self,
        info: Info[GraphQLContext, None],
        user_id: int,
        full_name: Optional[str] = None,
        settings: Optional[Any] = None,
    ) -> Optional[GraphQLUser]:
        """Update the current user's full name and/or settings."""
        connection = info.context['connection']
        user_layer = UserLayer(connection)
        await user_layer.update_user(user_id, full_name, settings)
        user = await user_layer.get_user_by_id(user_id)
        return GraphQLUser.from_internal(user) if user else None
