import strawberry
from strawberry.types import Info
from typing import Annotated, Optional

from db.python.layers.user import UserLayer
from api.graphql.loaders import GraphQLContext
from api.graphql.types.user import GraphQLUser, UserSettings

JSON = strawberry.scalars.JSON


@strawberry.type
class UserMutations:
    """GraphQL mutations for creating and updating users."""

    @strawberry.mutation
    async def create_user(
        self,
        info: Info[GraphQLContext, None],
        email: str,
        full_name: Optional[str] = None,
        settings: Optional[UserSettings] = None,
    ) -> Annotated['GraphQLUser', strawberry.lazy('api.graphql.schema')]:
        """Create a new user with the given email, full name, and settings."""
        connection = info.context['connection']
        user_layer = UserLayer(connection)
        user_id = await user_layer.create_user(email, full_name, settings)
        user = await user_layer.get_user_by_id(user_id)

        if user is None:
            raise ValueError('Failed to create user')

        return GraphQLUser.from_internal(dict(user))

    @strawberry.mutation
    async def update_user(
        self,
        info: Info[GraphQLContext, None],
        user_id: int,
        email: Optional[str] = None,
        full_name: Optional[str] = None,
        settings: Optional[UserSettings] = None,
    ) -> Optional[Annotated['GraphQLUser', strawberry.lazy('api.graphql.schema')]]:
        """Update the current user's full name and/or settings."""
        connection = info.context['connection']
        user_layer = UserLayer(connection)
        await user_layer.update_user(user_id, email, full_name, settings)
        user = await user_layer.get_user_by_id(user_id)
        return GraphQLUser.from_internal(dict(user)) if user else None

    @strawberry.mutation
    async def delete_user(
        self,
        info: Info[GraphQLContext, None],
        user_email: str,
    ) -> Optional[Annotated['GraphQLUser', strawberry.lazy('api.graphql.schema')]]:
        """Delete the user with the given ID."""
        connection = info.context['connection']
        user_layer = UserLayer(connection)
        user = await user_layer.get_user_by_email(user_email)

        if not user:
            raise ValueError(f'Cannot delete: User with email {user_email} not found')

        await user_layer.delete_user(user.id)
        return GraphQLUser.from_internal(dict(user)) if user else None
