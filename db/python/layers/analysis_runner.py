from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.tables.analysis_runner import AnalysisRunnerFilter, AnalysisRunnerTable
from models.models.analysis_runner import AnalysisRunnerInternal


class AnalysisRunnerLayer(BaseLayer):
    """Layer for analysis logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)

        self.at = AnalysisRunnerTable(connection)

    # GETS

    async def query(
        self, filter_: AnalysisRunnerFilter, check_project_ids: bool = True
    ) -> list[AnalysisRunnerInternal]:
        """Get analysis runner logs"""
        logs = await self.at.query(filter_)
        if not logs:
            return []

        if check_project_ids:
            project_ids = set(log.project for log in logs)

            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=True
            )

        return logs

    # INSERTS

    async def insert_analysis_runner_entry(
        self, analysis_runner: AnalysisRunnerInternal
    ) -> str:
        """Insert analysis runner log"""
        return await self.at.insert_analysis_runner_entry(analysis_runner)
