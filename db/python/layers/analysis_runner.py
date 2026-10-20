from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers.base import BaseLayer
from db.python.tables.analysis_runner import AnalysisRunnerFilter, AnalysisRunnerTable
from db.python.utils import NotFoundError
from models.models.analysis_runner import (
    AnalysisRunnerInternal,
    AnalysisRunnerUpdateBody,
)
from models.models.project import FullWriteAccessRoles, ReadAccessRoles


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
        self, ar_update_body: AnalysisRunnerUpdateBody
    ) -> None:
        """Update an AnalysisRunner entry, only the given fields are updated"""
        ar_guid = ar_update_body.ar_guid

        if (
            ar_update_body.config_path is None
            and ar_update_body.batch_url is None
            and ar_update_body.meta is None
        ):
            raise ValueError('Must provide config_path, batch_url or meta')

        ar_entries = await self.at.query(
            AnalysisRunnerFilter(ar_guid=GenericFilter(eq=ar_guid))
        )

        if not ar_entries:
            raise NotFoundError(
                f"Couldn't find analysis runner log with ar_guid = {ar_guid}"
            )

        project = ar_entries[0].project
        self.connection.check_access_to_projects_for_ids(
            [project], allowed_roles=FullWriteAccessRoles
        )

        await self.at.update_analysis_runner_entry(
            ar_guid,
            config_path=ar_update_body.config_path,
            batch_url=ar_update_body.batch_url,
            meta=ar_update_body.meta,
        )
