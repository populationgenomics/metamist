from api.utils.db import get_projectless_db_connection
from db.python.layers.base import BaseLayer
from db.python.tables.project_groups import ProjectGroupsTable
from models.models.project_groups import ProjectGroup


class ProjectGroupsLayer(BaseLayer):
    """Layer for managing project groups in the database."""

    def __init__(self, connection=get_projectless_db_connection):
        """Initialize the ProjectGroupsLayer with a database connection."""
        super().__init__(connection)
        self.table = ProjectGroupsTable(connection)

    async def get_all(self) -> list[ProjectGroup]:
        """Retrieve all project groups from the database."""
        return await self.table.get_all()

    async def get_by_id(self, group_id: int) -> ProjectGroup | None:
        """Retrieve a project group by its unique ID."""
        return await self.table.get_by_id(group_id)

    async def create(
        self,
        user_id: int,
        group_id: int,
        group_name: str,
        description: str | None = None,
    ) -> ProjectGroup:
        """Create a new project group with the specified parameters."""
        return await self.table.create(user_id, group_id, group_name, description)

    async def get_projects_in_group(self, group_id: int) -> list[int]:
        """Retrieve all project IDs associated with a specific project group."""
        return await self.table.get_projects_in_group(group_id)
