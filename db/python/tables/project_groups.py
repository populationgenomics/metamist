import json
from typing import Optional

from db.python.tables.base import DbBase
from models.models.project_groups import ProjectGroup


class ProjectGroupsTable(DbBase):
    """Table operations and queries for the project_groups table"""

    __tablename__ = 'project_groups'

    async def get_all(self):
        """Retrieve all project groups with their associated project IDs."""
        sql = """
            SELECT g.id, g.user_id, g.group_id, g.group_name, g.description, g.created_at, g.updated_at,
                COALESCE(json_arrayagg(pgi.project_id), JSON_ARRAY()) AS project_ids
            FROM project_groups g
            LEFT JOIN project_group_items pgi ON g.id = pgi.group_id
            GROUP BY g.id
        """
        rows = await self.connection.fetch_all(sql)
        return [self._row_to_model(row) for row in rows]

    async def get_by_id(self, pg_id: int):
        """Retrieve a project group by its unique ID with associated project IDs."""
        sql = """
            SELECT g.id, g.user_id, g.group_id, g.group_name, g.description, g.created_at, g.updated_at,
                COALESCE(json_arrayagg(pgi.project_id), JSON_ARRAY()) AS project_ids
            FROM project_groups g
            LEFT JOIN project_group_items pgi ON g.id = pgi.group_id
            WHERE g.id = :id
            GROUP BY g.id
        """
        row = await self.connection.fetch_one(sql, {'id': pg_id})
        return self._row_to_model(row) if row else None

    async def create(
        self,
        user_id: int,
        group_id: int,
        group_name: str,
        description: Optional[str] = None,
    ):
        """Create a new project group with the specified parameters."""
        sql = 'INSERT INTO project_groups (user_id, group_id, group_name, description) VALUES (:user_id, :group_id, :group_name, :description)'
        new_id = await self.connection.execute(
            sql,
            {
                'user_id': user_id,
                'group_id': group_id,
                'group_name': group_name,
                'description': description,
            },
        )
        return await self.get_by_id(new_id)

    async def get_projects_in_group(self, group_id: int):
        """Retrieve all project IDs associated with a specific project group."""
        sql = 'SELECT project_id FROM project_group_items WHERE group_id = :group_id'
        rows = await self.connection.fetch_all(sql, {'group_id': group_id})
        return [row['project_id'] for row in rows]

    def _row_to_model(self, row):
        """Convert a database row to a ProjectGroup model."""
        return ProjectGroup(
            id=row['id'],
            user_id=row['user_id'],
            group_id=row['group_id'],
            group_name=row['group_name'],
            description=row['description'],
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            project_ids=json.loads(row['project_ids']) if row['project_ids'] else [],
        )
