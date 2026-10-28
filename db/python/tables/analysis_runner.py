import dataclasses
import datetime
from typing import Any

from pymysql.err import IntegrityError

from db.python.filters import GenericFilter, GenericFilterModel, GenericMetaFilter
from db.python.tables.base import DbBase
from db.python.utils import ConflictError, to_db_json
from models.models.analysis_runner import AnalysisRunnerInternal
from models.models.project import ProjectId


# MariaDB error code for a unique / primary key violation
DUPLICATE_ENTRY_ERROR_CODE = 1062


@dataclasses.dataclass
class AnalysisRunnerFilter(GenericFilterModel):
    """Filter model for AR records"""

    project: GenericFilter[ProjectId] | None = None
    ar_guid: GenericFilter[str] | None = None
    submitting_user: GenericFilter[str] | None = None
    repository: GenericFilter[str] | None = None
    access_level: GenericFilter[str] | None = None
    environment: GenericFilter[str] | None = None
    config_path: GenericFilter[str] | None = None
    meta: GenericMetaFilter | None = None


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
            'project': analysis_runner.project,
        }

        try:
            await self.connection.execute(_query, values)
        except IntegrityError as e:
            if e.args[0] != DUPLICATE_ENTRY_ERROR_CODE:
                raise
            raise ConflictError(
                f'Analysis runner log with ar_guid {analysis_runner.ar_guid} already exists'
            ) from e

        return analysis_runner.ar_guid

    async def update_analysis_runner_entry(
        self,
        ar_guid: str,
        config_path: str | None = None,
        batch_url: str | None = None,
        meta: dict[str, str | None] | None = None,
    ) -> None:
        """
        Update an existing AnalysisRunner entry, only the given fields are updated
        """
        fields: dict[str, Any] = {
            'ar_guid': ar_guid,
            'audit_log_id': await self.audit_log_id(),
        }
        updaters = ['audit_log_id = :audit_log_id']

        if config_path is not None:
            updaters.append('config_path = :config_path')
            fields['config_path'] = config_path

        if batch_url is not None:
            updaters.append('batch_url = :batch_url')
            fields['batch_url'] = batch_url

        if meta is not None:
            updaters.append('meta = JSON_MERGE_PATCH(COALESCE(meta, "{}"), :meta)')
            fields['meta'] = to_db_json(meta)

        fields_str = ', '.join(updaters)
        _query = f'UPDATE analysis_runner SET {fields_str} WHERE ar_guid = :ar_guid'

        await self.connection.execute(_query, fields)
