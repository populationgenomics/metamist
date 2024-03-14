import dataclasses
import datetime

from db.python.tables.base import DbBase
from db.python.utils import GenericFilter, GenericFilterModel, to_db_json
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

    table_name = 'analysis_runner'

    async def query(
        self, filter_: AnalysisRunnerFilter
    ) -> list[AnalysisRunnerInternal]:
        """
        Get analysis runner logs
        """

        where_str, values = filter_.to_sql()

        _query = f"""
SELECT
    project, ar_guid, timestamp, access_level, repository, commit, script,
    description, driver_image, config_path, cwd, environment,
    hail_version, batch_url, submitting_user, meta, output_path, audit_log_id
FROM analysis_runner
WHERE {where_str}
    """
        rows = await self.connection.fetch_all(_query, values)
        return [AnalysisRunnerInternal.from_db(**dict(row)) for row in rows]

    async def insert_analysis_runner_entry(
        self, analysis_runner: AnalysisRunnerInternal
    ) -> str:
        """
        Insert analysis runner log
        """

        _query = """
INSERT INTO analysis_runner (
    project, ar_guid, timestamp, access_level, repository, commit, script,
    description, driver_image, config_path, cwd, environment,
    hail_version, batch_url, submitting_user, meta, output_path, audit_log_id
)
VALUES (
    :project, :ar_guid, :timestamp, :access_level, :repository, :commit, :script,
    :description, :driver_image, :config_path, :cwd, :environment,
    :hail_version, :batch_url, :submitting_user, :meta, :output_path, :audit_log_id
)
"""
        values = {
            'ar_guid': analysis_runner.ar_guid,
            'timestamp': datetime.datetime.now(),
            'access_level': analysis_runner.access_level,
            'repository': analysis_runner.repository,
            'commit': analysis_runner.commit,
            'script': analysis_runner.script,
            'description': analysis_runner.description,
            'driver_image': analysis_runner.driver_image,
            'config_path': analysis_runner.config_path,
            'cwd': analysis_runner.cwd,
            'environment': analysis_runner.environment,
            'hail_version': analysis_runner.hail_version,
            'batch_url': analysis_runner.batch_url,
            'submitting_user': analysis_runner.submitting_user,
            'meta': to_db_json(analysis_runner.meta),
            'output_path': analysis_runner.output_path,
            'audit_log_id': await self.audit_log_id(),
            'project': self.project,
        }

        await self.connection.execute(_query, values)

        return analysis_runner.ar_guid
