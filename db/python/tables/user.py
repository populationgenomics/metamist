from typing import Any, Optional
from api.graphql.types.user import UserSettingsInput
from db.python.tables.base import DbBase

from models.models.user import UserInternal


class UsersTable(DbBase):
    """
    Table operations and queries for the users table
    """

    table_name = 'users'

    async def get_all_users(self) -> list[UserInternal]:
        """Retrieve all users from the users table."""
        query = f'SELECT * FROM {self.table_name} ORDER BY full_name'
        rows = await self.connection.fetch_all(query)
        return [UserInternal.from_db(dict(row)) for row in rows] if rows else []

    async def get_by_id(self, user_id: int) -> Optional[UserInternal]:
        """Retrieve a user by their unique user ID."""
        query = f'SELECT * FROM {self.table_name} WHERE id = :user_id'
        row = await self.connection.fetch_one(query, {'user_id': user_id})
        return UserInternal.from_db(dict(row)) if row else None

    async def get_by_email(self, email: str) -> Optional[UserInternal]:
        """Retrieve a user by their email address."""
        query = f'SELECT * FROM {self.table_name} WHERE email = :email'
        row = await self.connection.fetch_one(query, {'email': email})
        return UserInternal.from_db(dict(row)) if row else None

    async def create(
        self,
        email: str,
        full_name: Optional[str] = None,
        settings: Optional[UserSettingsInput] = None,
    ) -> int:
        """Create a new user with the given email, full name, and settings."""
        query = f"""
        INSERT INTO {self.table_name} (email, full_name, settings)
        VALUES (:email, :full_name, :settings)
        RETURNING id
        """
        row = await self.connection.fetch_one(
            query,
            {
                'email': email,
                'full_name': full_name,
                'settings': settings.to_db() if settings else None,
            },
        )

        if row is None:
            raise ValueError('Failed to create user')

        return row['id']

    async def update(
        self,
        user_id: int,
        email: str,
        full_name: Optional[str] = None,
        settings: Optional[UserSettingsInput] = None,
    ):
        """Update the user's full name and/or settings by user ID."""
        updates = []
        params: dict[str, Any] = {'user_id': user_id}
        if email is not None:
            updates.append('email = :email')
            params['email'] = email

        if full_name is not None:
            updates.append('full_name = :full_name')
            params['full_name'] = full_name

        if settings is not None:
            updates.append('settings = :settings')
            params['settings'] = settings.to_db()

        if not updates:
            return

        query = f"UPDATE {self.table_name} SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE id = :user_id"
        await self.connection.execute(query, params)

    async def delete(self, user_id: int):
        """Delete a user by their unique user ID."""
        query = f'DELETE FROM {self.table_name} WHERE id = :user_id'
        await self.connection.execute(query, {'user_id': user_id})
