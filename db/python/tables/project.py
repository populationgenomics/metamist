from datetime import datetime, timedelta
from typing import Optional, Dict, Union


class ProjectTable:
    """
    Capture project operations and queries
    """

    table_name = 'project'

    cache_expiry = None
    cached_project_names: Dict[Optional[str], Optional[int]] = None

    def __init__(self, connection):

        self.connection = connection

    async def get_project_id_map(self) -> Dict[Optional[str], Optional[int]]:
        """(CACHED) Get map of project names to project IDs"""
        if (
            not ProjectTable.cached_project_names
            or ProjectTable.cache_expiry < datetime.utcnow()
        ):
            ProjectTable.cached_project_names = {
                **(await self.get_project_name_id_map()),
                None: None,
            }
            ProjectTable.cache_expiry = datetime.utcnow() + timedelta(minutes=1)

        return ProjectTable.cached_project_names

    async def get_project_id_from_name_and_user(
        self, user: str, project: Optional[int]
    ) -> Optional[Union[int, bool]]:
        """
        Get projectId from project name and user (email address)
        TODO: implement project permissions here
        TODO: implement smarter cache (or remove cache)

        """
        if not user:
            raise Exception('An internal error occurred during authorization')
        m = await self.get_project_id_map()
        return m.get(project, False)

    async def get_project_name_id_map(self) -> Dict[str, int]:
        """Get {name: id} project map"""
        _query = 'SELECT name, id FROM project'
        rows = await self.connection.fetch_all(_query)
        return dict(rows)
