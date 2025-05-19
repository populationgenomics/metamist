from db.python.layers.base import BaseLayer
from db.python.tables.user import UsersTable
from typing import Optional, Any
from api.utils.db import get_projectless_db_connection


class UserLayer(BaseLayer):
    """Layer for interfacing with the users table via UsersTable.

    Enforces that users can only create, update, or delete their own user record,
    based on the email from the JWT token (available as self.connection.author).
    """

    def __init__(self, connection=get_projectless_db_connection):
        """Initialize the UserLayer with a database connection."""
        super().__init__(connection)
        self.users_table = UsersTable(connection)

    def whoami(self) -> Optional[str]:
        """Get the current user email from the connection."""
        if self.connection.author is None:
            raise PermissionError('No user is authenticated.')

        return self.connection.author or None

    def is_this_me(self, user: dict[str, Any] | None) -> bool:
        """Check if the given user dictionary belongs to the current user."""
        me = self.whoami()

        if user is None or me is None:
            return False

        if user['email'].lower() == me.lower():
            return True

        return False

    def check_authentication(self, user: dict[str, Any] | None):
        """Check if the current user is authenticated."""
        if not self.is_this_me(user):
            raise PermissionError('You can only access your own user record.')

    async def get_all_users(self) -> list[dict]:
        """Retrieve all users from the users table."""
        return await self.users_table.get_all_users()

    async def get_user_by_id(self, user_id: int) -> Optional[dict]:
        """Retrieve a user by their unique user ID."""
        user = await self.users_table.get_by_id(user_id)
        self.check_authentication(user)
        return user

    async def get_user_by_email(self, email: str) -> Optional[dict]:
        """Retrieve a user by their email address."""
        user = await self.users_table.get_by_email(email)
        self.check_authentication(user)
        return user

    async def create_user(
        self,
        email: str,
        full_name: Optional[str] = None,
        settings: Optional[Any] = None,
    ) -> int:
        """Create a new user if the email matches the authenticated user's email."""
        self.check_authentication({'email': email})
        return await self.users_table.create(email, full_name, settings)

    async def update_user(
        self,
        user_id: int,
        full_name: Optional[str] = None,
        settings: Optional[Any] = None,
    ):
        """Update the current user's record if the user ID matches the authenticated user."""
        user = await self.users_table.get_by_id(user_id)
        self.check_authentication(user)
        await self.users_table.update(user_id, full_name, settings)

    async def delete_user(self, user_id: int):
        """Delete the current user's record if the user ID matches the authenticated user."""
        user = await self.users_table.get_by_id(user_id)
        self.check_authentication(user)
        await self.users_table.delete(user_id)
