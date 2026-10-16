from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers.base import BaseLayer
from db.python.tables.analysis_runner import AnalysisRunnerFilter, AnalysisRunnerTable
from db.python.utils import NotFoundError
from models.models.analysis_runner import AnalysisRunnerInternal
from models.models.project import ProjectId, ReadAccessRoles


class AnalysisRunnerLayer(BaseLayer):
    """Layer for analysis logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)

        self.at = AnalysisRunnerTable(connection)

    # GETS

    async def query(
        self, filter_: AnalysisRunnerFilter
    ) -> list[AnalysisRunnerInternal]:
        """Get analysis runner logs"""
        logs = await self.at.query(filter_)
        if not logs:
            return []

        project_ids = set(log.project for log in logs)

        self.connection.check_access_to_projects_for_ids(
            project_ids, allowed_roles=ReadAccessRoles
        )

        return logs

    # INSERTS

    async def insert_analysis_runner_entry(
        self, analysis_runner: AnalysisRunnerInternal
    ) -> str:
        """Insert analysis runner log"""
        return await self.at.insert_analysis_runner_entry(analysis_runner)

    # UPDATES

    async def update_analysis_runner_entry(
        self,
        project: ProjectId,
        ar_guid: str,
        config_path: str,
    ) -> None:
        """Update the config path of an AR log"""

        ar_entry = await self.at.query(
            AnalysisRunnerFilter(
                ar_guid=GenericFilter(eq=ar_guid), project=GenericFilter(eq=project)
            )
        )

        if not ar_entry:
            raise NotFoundError(
                f"Couldn't find analysis runner log with ar_guid = {ar_guid}"
            )

        await self.at.update_analysis_runner_entry(
            project, ar_guid, config_path=config_path
        )
