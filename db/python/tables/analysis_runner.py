import dataclasses
import datetime

from psycopg import sql
from psycopg.errors import UniqueViolation
from psycopg.rows import class_row
from psycopg.types.json import Jsonb

from db.python.filters import (
    GenericFilter,
    GenericFilterModel,
    GenericMetaFilter,
    is_literally_TRUE,
)
from db.python.tables.base import DbBase
from db.python.utils import ConflictError
from models.models.analysis_runner import (
    AnalysisRunnerInternal,
    AnalysisRunnerUpdateBody,
)
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
    config_path: GenericFilter[str] | None = None
    meta: GenericMetaFilter | None = None


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

        where_params = filter_.to_sql()
        wheres_query = (
            t'WHERE {where_params:q}' if not is_literally_TRUE(where_params) else t''
        )

        _query = t"""
        SELECT
            project, ar_guid, timestamp, access_level, repository, commit, script,
            description, driver_image, config_path, cwd, environment,
            hail_version, batch_url, submitting_user, COALESCE(meta, {'{}'}::jsonb) as meta, output_path, audit_log_id
        FROM analysis_runner
        {wheres_query:q}
        """
        async with self.connection.pg_connection.cursor(
            row_factory=class_row(AnalysisRunnerInternal)
        ) as cur:
            analysis_runner_internal_list = await (await cur.execute(_query)).fetchall()

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
        INSERT INTO analysis_runner (project, ar_guid, timestamp, access_level,
        repository, commit, script, description,
        driver_image, config_path, cwd, environment,
        hail_version, batch_url, submitting_user, meta,
        output_path, audit_log_id
        )
        VALUES (
        {analysis_runner.project}, {analysis_runner.ar_guid}, {datetime.datetime.now()}, {analysis_runner.access_level},
        {analysis_runner.repository}, {analysis_runner.commit}, {analysis_runner.script}, {analysis_runner.description},
        {analysis_runner.driver_image}, {analysis_runner.config_path}, {analysis_runner.cwd}, {analysis_runner.environment},
        {analysis_runner.hail_version}, {analysis_runner.batch_url}, {analysis_runner.submitting_user}, {meta_param},
        {analysis_runner.output_path}, {audit_log_id}
        )
        """

        try:
            await self.connection.pg_connection.execute(_query)
        except UniqueViolation as e:
            raise ConflictError(
                f'Analysis runner log with ar_guid {analysis_runner.ar_guid} already exists'
            ) from e
        return analysis_runner.ar_guid

    async def update_analysis_runner_entry(
        self, ar_update_body: AnalysisRunnerUpdateBody
    ) -> None:
        """
        Update an existing AnalysisRunner entry, only the given fields are updated.
        clear_config_path sets config_path to null.
        """
        audit_log_id = await self.audit_log_id()
        setters = [t'audit_log_id = {audit_log_id}']

        if ar_update_body.clear_config_path:
            setters.append(t'config_path = NULL')
        elif ar_update_body.config_path is not None:
            setters.append(t'config_path = {ar_update_body.config_path}')

        if ar_update_body.batch_url is not None:
            setters.append(t'batch_url = {ar_update_body.batch_url}')

        if ar_update_body.submitting_user is not None:
            setters.append(t'submitting_user = {ar_update_body.submitting_user}')

        if ar_update_body.meta is not None:
            meta = Jsonb(ar_update_body.meta)
            setters.append(
                t"meta = json_merge_patch(COALESCE(meta, '{{}}'::jsonb), {meta})"
            )

        ar_guid = ar_update_body.ar_guid
        fields_str = sql.SQL(', ').join(setters)
        _query = t'UPDATE analysis_runner SET {fields_str:q} WHERE ar_guid = {ar_guid}'

        await self.connection.pg_connection.execute(_query)
