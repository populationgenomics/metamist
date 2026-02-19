import dataclasses
import datetime
from string.templatelib import Template

from psycopg.types.json import Jsonb

from db.python.filters import GenericFilter, GenericFilterModel
from db.python.tables.base import DbBase
from models.models.analysis_runner import AnalysisRunnerInternal
from models.models.project import ProjectId


@dataclasses.dataclass
class AnalysisRunnerFilter(GenericFilterModel):
    """Filter model for AR records"""

    project: GenericFilter[ProjectId] | None = None
    ar_guid: GenericFilter[str] | None = None
    submitting_user: GenericFilter[str] | None = None
    repository: GenericFilter[str] | None = None
    access_level: GenericFilter[str] | None = None
    environment: GenericFilter[str] | None = None


class AnalysisRunnerTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    async def query(
        self, filter_: AnalysisRunnerFilter
    ) -> list[AnalysisRunnerInternal]:
        """
        Get analysis runner logs
        """

        where_params: Template = filter_.to_sql()

        if not list(where_params):
            raise ValueError(f'Invalid filter: {filter_}')

        _query = t"""
        SELECT
            project, ar_guid, timestamp, access_level, repository, commit, script,
            description, driver_image, config_path, cwd, environment,
            hail_version, batch_url, submitting_user, meta, output_path, audit_log_id
        FROM analysis_runner
        WHERE {where_params:q}
    """
        rows = await (await self.connection.pg_connection.execute(_query)).fetchall()

        analysis_runner_internal_list = []
        for row in rows:
            row['meta'] = row.get('meta') or {}
            analysis_runner_internal_list.append(AnalysisRunnerInternal(**row))

        return analysis_runner_internal_list

    async def insert_analysis_runner_entry(
        self, analysis_runner: AnalysisRunnerInternal
    ) -> str:
        """
        Insert analysis runner log
        """
        audit_log_id = await self.audit_log_id()
        meta_param = Jsonb(analysis_runner.meta)

        _query = t"""
        INSERT INTO analysis_runner (project, ar_guid, timestamp, access_level, repository, commit, script,
        description, driver_image, config_path, cwd, environment, hail_version, batch_url, submitting_user, meta, output_path, audit_log_id)
        VALUES ({self.project_id}, {analysis_runner.ar_guid}, {datetime.datetime.now()}, {analysis_runner.access_level}, {analysis_runner.repository},
        {analysis_runner.commit}, {analysis_runner.script}, {analysis_runner.description}, {analysis_runner.driver_image}, {analysis_runner.config_path},
        {analysis_runner.cwd}, {analysis_runner.environment}, {analysis_runner.hail_version}, {analysis_runner.batch_url}, {analysis_runner.submitting_user},
        {meta_param}, {analysis_runner.output_path}, {audit_log_id})
        """

        await self.connection.pg_connection.execute(_query)
        return analysis_runner.ar_guid
