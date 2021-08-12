# from datetime import datetime
from typing import Optional, Dict


class ProjectTable:
    """
    Capture project operations and queries
    """

    table_name = 'project'

    cache_expiry = None
    cached_project_names: Dict[Optional[str], Optional[int]] = None

    def __init__(self, connection):

        self.connection = connection

    async def get_project_id_from_name_and_user(
        self, user: str, project: Optional[int]
    ) -> int:
        """
        Get projectId from project name and user (email address)
        TODO: implement project permissions here
        TODO: implement smarter cache (or remove cache)

        """
        if not user:
            raise Exception('An internal error occurred during authorization')
        if (
            not ProjectTable.cached_project_names
        ):  # or ProjectTable.cache_expiry < datetime.utcnow():
            ProjectTable.cached_project_names = {
                **(await self.get_project_name_id_map()),
                None: None,
            }
            # ProjectTable.cache_expiry = datetime.utcnow() + 5 minutes

        return ProjectTable.cached_project_names.get(project, False)

    async def get_project_name_id_map(self) -> Dict[str, int]:
        """Get {name: id} project map"""
        _query = 'SELECT name, id FROM project'
        rows = await self.connection.fetch_all(_query)
        return dict(rows)
