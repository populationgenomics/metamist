import dataclasses
import datetime
from collections import defaultdict
from typing import Any

from psycopg import sql
from psycopg.types.json import Jsonb

from db.python.filters import (
    GenericFilter,
    GenericFilterModel,
    GenericMetaFilter,
    join_sql_with_AND,
)
from db.python.tables.base import DbBase
from db.python.utils import NotFoundError, to_db_json
from models.enums import AnalysisStatus
from models.models import PRIMARY_EXTERNAL_ORG
from models.models.analysis import AnalysisInternal
from models.models.audit_log import AuditLogInternal
from models.models.output_file import OutputFileInternal, RecursiveDict
from models.models.project import ProjectId


@dataclasses.dataclass
class AnalysisFilter(GenericFilterModel):
    """Filter for analysis"""

    id: GenericFilter[int] | None = None
    sequencing_group_id: GenericFilter[int] | None = None
    cohort_id: GenericFilter[int] | None = None
    project: GenericFilter[int] | None = None
    type: GenericFilter[str] | None = None
    status: GenericFilter[AnalysisStatus] | None = None
    meta: GenericMetaFilter | None = None
    output: GenericFilter[str] | None = None
    active: GenericFilter[bool] | None = None
    timestamp_completed: GenericFilter[datetime.datetime] | None = None

    def __hash__(self):
        return super().__hash__()


class AnalysisTable(DbBase):
    """Capture Analysis table operations and queries"""

    table_name = 'analysis'

    async def get_project_ids_for_analysis_ids(
        self, analysis_ids: list[int]
    ) -> set[ProjectId]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = t"""
            SELECT project FROM analysis WHERE id = ANY({analysis_ids}) GROUP BY project
        """
        acur = await self.connection.pg_connection.execute(_query)
        rows = await acur.fetchall()
        return set(r['project'] for r in rows)

    async def create_analysis(
        self,
        analysis_type: str,
        status: AnalysisStatus,
        sequencing_group_ids: list[int] | None = None,
        cohort_ids: list[int] | None = None,
        meta: dict[str, Any] | None = None,
        active: bool | None = True,
        timestamp_completed: datetime.datetime | None = None,
        project: ProjectId | None = None,
    ) -> int:
        """Create a new sample, and add it to database"""
        async with self.connection.transaction():
            kv_pairs: dict[str, Any] = {
                'type': analysis_type,
                'status': status.value,
                'meta': to_db_json(meta or {}),
                'audit_log_id': await self.audit_log_id(),
                'project': project or self.project_id,
                'active': active if active is not None else True,
            }

            if status == AnalysisStatus.COMPLETED:
                kv_pairs['timestamp_completed'] = (
                    timestamp_completed or datetime.datetime.now(datetime.UTC)
                )

            ordered_keys = sorted(kv_pairs.keys())
            cs_keys = sql.SQL(', ').join(sql.Identifier(k) for k in ordered_keys)
            cs_values = sql.SQL(', ').join(
                sql.Literal(kv_pairs[k]) for k in ordered_keys
            )

            _query = t"""
                INSERT INTO analysis ({cs_keys:q})
                VALUES ({cs_values:q})
                RETURNING id
            """

            row = await self.connection.execute_must_fetch_one(_query)
            id_of_new_analysis = row['id']

            if sequencing_group_ids:
                await self.add_sequencing_groups_to_analysis(
                    id_of_new_analysis, sequencing_group_ids
                )

            if cohort_ids:
                await self.add_cohorts_to_analysis(id_of_new_analysis, cohort_ids)

        return id_of_new_analysis

    async def add_sequencing_groups_to_analysis(
        self, analysis_id: int, sequencing_group_ids: list[int]
    ):
        """Add samples to an analysis (through the linked table)"""
        _query = """
            INSERT INTO analysis_sequencing_group
                (analysis_id, sequencing_group_id, audit_log_id)
            VALUES (%(aid)s, %(sid)s, %(audit_log_id)s)
        """

        audit_log_id = await self.audit_log_id()
        values = [
            {
                'aid': analysis_id,
                'sid': sid,
                'audit_log_id': audit_log_id,
            }
            for sid in sequencing_group_ids
        ]
        async with self.connection.pg_connection.cursor() as acur:
            await acur.executemany(_query, values)

    async def add_cohorts_to_analysis(self, analysis_id: int, cohort_ids: list[int]):
        """Add cohorts to an analysis (through the linked table)"""
        _query = """
            INSERT INTO analysis_cohort
                (analysis_id, cohort_id, audit_log_id)
            VALUES (:aid, :cid, :audit_log_id)
        """

        audit_log_id = await self.audit_log_id()
        values = [
            {
                'aid': analysis_id,
                'cid': cid,
                'audit_log_id': audit_log_id,
            }
            for cid in cohort_ids
        ]
        async with self.connection.pg_connection.cursor() as acur:
            await acur.executemany(_query, values)

    async def find_sgs_in_joint_call_or_es_index_up_to_date(
        self, date: datetime.date
    ) -> set[int]:
        """Find all the sequencing groups that have been in a joint-call or es-index up to a date"""
        _query = t"""
            SELECT DISTINCT asg.sequencing_group_id
            FROM analysis_sequencing_group asg
            INNER JOIN analysis a ON asg.analysis_id = a.id
            WHERE
                a.type IN ('joint-calling', 'es-index')
                AND a.timestamp_completed <= {date}
        """

        acur = await self.connection.pg_connection.execute(_query)
        results = await acur.fetchall()
        return {r['sequencing_group_id'] for r in results}

    async def update_analysis(
        self,
        analysis_id: int,
        status: AnalysisStatus | None = None,
        meta: dict[str, Any] | None = None,
        active: bool | None = None,
    ):
        """Update the status of an analysis, set timestamp_completed if relevant"""
        audit_log_id = await self.audit_log_id()
        setters = [
            t'audit_log_id = {audit_log_id}',
            t'on_behalf_of = {self.author}',
        ]

        setters.append(t'status = {status.value}') if status else None
        setters.append(t'active = {active}') if active is not None else None

        if status == AnalysisStatus.COMPLETED:
            now = datetime.datetime.now(datetime.UTC)
            setters.append(
                t'timestamp_completed = CASE WHEN timestamp_completed IS NULL THEN {now} ELSE timestamp_completed END',
            )

        if meta is not None and len(meta) > 0:
            meta_value = Jsonb(meta)
            setters.append(
                t"meta = JSON_MERGE_PATCH(COALESCE(meta, '{{}}'), {meta_value})"
            )

        fields_str = sql.SQL(', ').join(setters)
        _query = t'UPDATE analysis SET {fields_str:q} WHERE id = {analysis_id}'

        await self.connection.pg_connection.execute(_query)

    async def query(self, filter_: AnalysisFilter) -> list[AnalysisInternal]:
        """Get analysis by various (AND'd) criteria"""
        required_fields = [
            filter_.id,
            filter_.sequencing_group_id,
            filter_.project,
            filter_.cohort_id,
        ]

        if not any(required_fields):
            raise ValueError(
                'Must provide at least one of id, sequencing_group_id, cohort_id '
                'or project to filter on'
            )

        where_condition = filter_.to_sql(
            {
                'id': 'a.id',
                'sequencing_group_id': 'a_sg.sequencing_group_id',
                'project': 'a.project',
                'type': 'a.type',
                'status': 'a.status',
                'meta': 'a.meta',
                'output': 'a.output',
                'active': 'a.active',
                'cohort_id': 'a_c.cohort_id',
            },
        )

        _query = t"""
            SELECT
                a.id,
                a.type,
                a.status,
                a.project,
                a.timestamp_completed,
                a.active,
                a.author,
                a.meta,
                string_agg(distinct a_sg.sequencing_group_id::text, ',') as _sequencing_group_ids,
                string_agg(distinct a_c.cohort_id::text, ',') as _cohort_ids
            FROM analysis a
            LEFT JOIN analysis_sequencing_group a_sg ON a.id = a_sg.analysis_id
            LEFT JOIN analysis_cohort a_c ON a.id = a_c.analysis_id
            WHERE {where_condition:q}
            GROUP BY a.id
        """
        acur = await self.connection.pg_connection.execute(_query)
        rows = await acur.fetchall()
        result: list[AnalysisInternal] = []

        if not rows:
            return result

        analysis_outputs_by_aid = await self.get_file_outputs_by_analysis_ids(
            [r['id'] for r in rows]
        )

        for row in rows:
            analysis_data = dict(row)
            analysis_output_for_id = analysis_outputs_by_aid.get(
                analysis_data['id'], None
            )
            if analysis_output_for_id:
                analysis_data['output'] = analysis_output_for_id.get('output', None)
                analysis_data['outputs'] = analysis_output_for_id.get('outputs', {})

            analysis = AnalysisInternal.from_db(**analysis_data)

            if row['_sequencing_group_ids']:
                analysis.sequencing_group_ids = [
                    int(sg) for sg in row['_sequencing_group_ids'].split(',')
                ]

            if row['_cohort_ids']:
                analysis.cohort_ids = [int(co) for co in row['_cohort_ids'].split(',')]

            result.append(analysis)

        return result

    async def get_file_outputs_by_analysis_ids(
        self, analysis_ids: list[int]
    ) -> dict[int, dict[str, RecursiveDict]]:
        """Fetches all output files for a list of analysis IDs"""
        _query = t"""
            SELECT DISTINCT ao.analysis_id, f.*, ao.json_structure, ao.output
            FROM analysis_outputs ao
            LEFT JOIN output_file f ON ao.file_id = f.id
            WHERE ao.analysis_id = ANY({analysis_ids})
        """
        acur = await self.connection.pg_connection.execute(_query)
        rows = await acur.fetchall()

        # Preparing to accumulate analysis files
        analysis_files: dict[
            int, dict[str, list[tuple[OutputFileInternal, str] | str] | str]
        ] = defaultdict(lambda: defaultdict(list))

        for row in rows:
            file_id = row['id']
            if file_id:
                # Building OutputFileInternal object with secondary files if available
                file_internal = OutputFileInternal.from_db(**dict(row))
                # If no json_structure, just set to the output.

                if row['json_structure'] is None:
                    analysis_files[row['analysis_id']]['output'] = row['path']
                else:
                    analysis_files[row['analysis_id']]['output'] = ''

                # if analysis_files[row['analysis_id']]['outputs'] is a str, we set it to a list and append the str to it:
                outputs = analysis_files[row['analysis_id']]['outputs']
                new_output = (file_internal, row['json_structure'] or '')
                if isinstance(outputs, str):
                    outputs = [outputs, new_output]
                else:
                    outputs.append(new_output)

                analysis_files[row['analysis_id']]['outputs'] = outputs
            else:
                # If no file_id, just set to the output.
                analysis_files[row['analysis_id']]['output'] = row['output']
                analysis_files[row['analysis_id']]['outputs'] = row['output']

        # Transforming analysis_files into the desired output format
        analysis_output_files = {
            a_id: {
                'output': files['output'],
                'outputs': OutputFileInternal.reconstruct_json(files['outputs']),
            }
            for a_id, files in analysis_files.items()
        }

        return analysis_output_files  # type: ignore [return-value]

    async def get_latest_complete_analysis_for_type(
        self,
        project: ProjectId,
        analysis_type: str,
        meta: dict[str, Any] | None = None,
    ):
        """Find the most recent completed analysis for some analysis type"""
        values = {'project': project, 'type': analysis_type}

        meta_query = t''
        if meta:
            for k, v in meta.items():
                # k_replacer = f'meta_{k}'
                meta_query += t" AND json_extract(meta, '$.{k}') = {v}"
                # if v is None:
                #     # mariadb does a bad cast for NULL
                #     v = 'null'  # noqa: PLW2901
                # values[k_replacer] = v

        _query = t"""
            SELECT a.id as id, a.type as type, a.status as status,
                    a_sg.sequencing_group_id as sequencing_group_id,
                    a.project as project, a.timestamp_completed as timestamp_completed,
                    a.meta as meta
            FROM analysis_sequencing_group a_sg
            INNER JOIN analysis a ON a_sg.analysis_id = a.id
            INNER JOIN sequencing_group sg ON a_sg.sequencing_group_id = sg.id
            WHERE a.id = (
                SELECT id FROM analysis
                WHERE active AND type = LOWER({analysis_type.lower()}) AND project = {project} AND status = 'completed' AND timestamp_completed IS NOT NULL {meta_query:q}
                ORDER BY timestamp_completed DESC
                LIMIT 1
            )
        """
        acur = await self.connection.pg_connection.execute(_query)
        rows = await acur.fetchall()

        if len(rows) == 0:
            raise NotFoundError(f"Couldn't find any analysis with type {analysis_type}")

        latest_analysis_data = rows[0]
        analysis_outputs_by_aid = await self.get_file_outputs_by_analysis_ids(
            [row['id'] for row in rows]
        )
        analysis_output_for_id = analysis_outputs_by_aid.get(
            latest_analysis_data['id'], None
        )

        if analysis_output_for_id:
            latest_analysis_data['output'] = analysis_output_for_id.get('output', None)
            latest_analysis_data['outputs'] = analysis_output_for_id.get('outputs', {})

        analysis = AnalysisInternal.from_db(**latest_analysis_data)
        # .from_db maps 'sequencing_group_id' -> sequencing_group_ids

        if analysis.sequencing_group_ids is None:
            analysis.sequencing_group_ids = []

        for row in rows[1:]:
            analysis.sequencing_group_ids.append(row['sequencing_group_id'])

        return analysis

    async def get_all_sequencing_group_ids_without_analysis_type(
        self, analysis_type: str, project: ProjectId
    ) -> list[int]:
        """Find all the samples in the sample_id list that a"""
        project_id = project or self.project_id

        _query = t"""
            SELECT sg.id as id
            FROM sequencing_group sg
            WHERE sg.project = {project_id} AND
                id NOT IN (
                    SELECT a_sg.sequencing_group_id FROM analysis_sequencing_group a_sg
                    LEFT JOIN analysis a ON a_sg.analysis_id = a.id
                    WHERE a.type = LOWER({analysis_type.lower()}) AND a.active
                )
        """

        acur = await self.connection.pg_connection.execute(_query)
        rows = await acur.fetchall()
        return [row['id'] for row in rows]

    async def get_latest_complete_analysis_for_sequencing_group_ids_by_type(
        self, analysis_type: str, sequencing_group_ids: list[int]
    ) -> list[AnalysisInternal]:
        """Get the latest complete analysis for samples (one per sample)"""

        expected_type = ('gvcf', 'cram', 'qc')
        if analysis_type not in expected_type:
            expected_types_str = ', '.join(a for a in expected_type)
            raise ValueError(
                f'Received analysis type {analysis_type!r}", expected {expected_types_str!r}'
            )

        _query = t"""
            SELECT
                a.id AS id, a.type as type, a.status as status,
                a.project as project, a_sg.sequencing_group_id,
                a.timestamp_completed as timestamp_completed, a.meta as meta
            FROM analysis a
            LEFT JOIN analysis_sequencing_group a_sg ON a_sg.analysis_id = a.id
            WHERE
                a.active AND
                a.type = LOWER({analysis_type.lower()}) AND
                a.timestamp_completed IS NOT NULL AND
                a_sg.sequencing_group_id = ANY({sequencing_group_ids})
            ORDER BY a.timestamp_completed DESC
        """

        acur = await self.connection.pg_connection.execute(_query)
        rows = await acur.fetchall()
        seen_sequencing_group_ids = set()
        analyses: list[AnalysisInternal] = []
        analysis_outputs_by_aid = await self.get_file_outputs_by_analysis_ids(
            [r['id'] for r in rows]
        )

        for row in rows:
            if row['sequencing_group_id'] in seen_sequencing_group_ids:
                continue

            analysis_data = dict(row)
            analysis_output_for_id = analysis_outputs_by_aid.get(
                analysis_data['id'], None
            )

            if analysis_output_for_id:
                analysis_data['output'] = analysis_output_for_id.get('output', None)
                analysis_data['outputs'] = analysis_output_for_id.get('outputs', {})

            analysis = AnalysisInternal.from_db(**analysis_data)
            analyses.append(analysis)

            seen_sequencing_group_ids.add(row['sequencing_group_id'])

        # reverse after timestamp_completed
        return analyses[::-1]

    async def get_analysis_by_id(
        self, analysis_id: int
    ) -> tuple[ProjectId, AnalysisInternal]:
        """Get analysis object by analysis_id"""
        _query = t"""
            SELECT
                a.id as id, a.type as type, a.status as status,
                a.project as project,
                a_sg.sequencing_group_id as sequencing_group_id,
                a.timestamp_completed as timestamp_completed, a.meta as meta, a.active as active
            FROM analysis a
            LEFT JOIN analysis_sequencing_group a_sg ON a_sg.analysis_id = a.id
            WHERE a.id = {analysis_id}
        """

        acur = await self.connection.pg_connection.execute(_query)
        rows = await acur.fetchall()

        if len(rows) == 0:
            raise NotFoundError(f"Couldn't find analysis with id = {analysis_id}")

        project = rows[0]['project']

        analysis_data = dict(rows[0])
        analysis_outputs_by_aid = await self.get_file_outputs_by_analysis_ids(
            [analysis_data['id']]
        )

        analysis_output_for_id = analysis_outputs_by_aid.get(analysis_data['id'], None)

        if analysis_output_for_id:
            analysis_data['output'] = analysis_output_for_id.get('output', None)
            analysis_data['outputs'] = analysis_output_for_id.get('outputs', {})

        analysis = AnalysisInternal.from_db(**analysis_data)
        if analysis.sequencing_group_ids is None:
            analysis.sequencing_group_ids = []

        for row in rows[1:]:
            analysis.sequencing_group_ids.append(row['sequencing_group_id'])

        return project, analysis

    async def get_sample_cram_path_map_for_seqr(
        self,
        project: ProjectId,
        sequencing_types: list[str],
        participant_ids: list[int] | None = None,
    ) -> list[dict[str, str]]:
        """Get (ext_sample_id, cram_path, internal_id) map"""
        where_conditions = [
            t'a.active',
            t"a.type = 'cram'",
            t"a.status = 'completed'",
            t'peid.project = {project}',
            t'peid.name = {PRIMARY_EXTERNAL_ORG}',
        ]
        if sequencing_types:
            st_case_insensitive = [t.lower() for t in sequencing_types]
            where_conditions.append(
                t"LOWER(a.meta->>'sequencing_type') = ANY({st_case_insensitive})"
            )

        if participant_ids:
            where_conditions.append(t'peid.participant_id = ANY({participant_ids})')

        where_clause = join_sql_with_AND(where_conditions)

        _query = t"""
            SELECT a.id, peid.external_id as participant_id, a.output as output, sg.id as sequencing_group_id
            FROM analysis a
            INNER JOIN analysis_sequencing_group a_sg ON a_sg.analysis_id = a.id
            INNER JOIN sequencing_group sg ON a_sg.sequencing_group_id = sg.id
            INNER JOIN sample s ON sg.sample_id = s.id
            INNER JOIN participant_external_id peid ON s.participant_id = peid.participant_id
            WHERE {where_clause:q}
            ORDER BY a.timestamp_completed DESC;
        """

        acur = await self.connection.pg_connection.execute(_query)
        rows = await acur.fetchall()
        results: list[dict] = []
        if not rows or len(rows) == 0:
            return results

        analysis_outputs_by_aid = await self.get_file_outputs_by_analysis_ids(
            [r['id'] for r in rows]
        )
        for row in rows:
            analysis_data = dict(row)
            analysis_output_for_id = analysis_outputs_by_aid.get(
                analysis_data['id'], None
            )

            if analysis_output_for_id:
                analysis_data['output'] = analysis_output_for_id.get('output', None)
                analysis_data['outputs'] = analysis_output_for_id.get('outputs', {})

            analysis_data.pop('id')
            results.append(analysis_data)
        # many per analysis
        return results

    # region STATS

    async def get_number_of_crams_by_sequencing_type(
        self, project: ProjectId
    ) -> dict[str, int]:
        """Get number of crams, grouped by sequence type (one per sample per sequence type)"""
        # Only count crams for ACTIVE sequencing groups
        _query = t"""
            SELECT sg.type as seq_type, COUNT(*) as number_of_crams
            FROM analysis a
            INNER JOIN analysis_sequencing_group asga ON a.id = asga.analysis_id
            INNER JOIN sequencing_group sg ON asga.sequencing_group_id = sg.id
            WHERE
                a.project = {project}
                AND a.status = 'completed'
                AND a.type = 'cram'
                AND NOT sg.archived
            GROUP BY seq_type
        """

        acur = await self.connection.pg_connection.execute(_query)
        rows = await acur.fetchall()

        # do it like this until I select lowercase value w/ JSON_EXTRACT
        n_counts: dict[str, int] = defaultdict(int)
        for r in rows:
            if seq_type := r['seq_type']:
                n_counts[str(seq_type).lower()] += r['number_of_crams']

        return n_counts

    async def get_seqr_stats_by_sequencing_type(
        self, project: ProjectId
    ) -> dict[str, int]:
        """Get number of samples in seqr (in latest es-index), grouped by sequence type"""
        _query = t"""
            SELECT sg.type as seq_type, COUNT(*) as n
            FROM analysis a
            INNER JOIN analysis_sequencing_group asga ON a.id = asga.analysis_id
            INNER JOIN sequencing_group sg ON asga.sequencing_group_id = sg.id
            INNER JOIN sample s on sg.sample_id = s.id
            WHERE
                s.project = {project}
                AND a.status = 'completed'
                AND a.type = 'es-index'
                AND NOT sg.archived
            GROUP BY seq_type
        """

        acur = await self.connection.pg_connection.execute(_query)
        rows = await acur.fetchall()
        return {r['seq_type']: r['n'] for r in rows}

    # endregion STATS

    async def get_sg_add_to_project_es_index(
        self, sg_ids: list[int]
    ) -> dict[int, datetime.date]:
        """Get all the sequencing groups that should be added to seqr joint calls"""
        _query = t"""
            SELECT
                a_sg.sequencing_group_id as sg_id,
                MIN(a.timestamp_completed) as timestamp_completed::date
            FROM analysis a
            INNER JOIN analysis_sequencing_group a_sg ON a.id = a_sg.analysis_id
            WHERE
                a.status = 'completed'
                AND a.type = 'es-index'
                AND a_sg.sequencing_group_id = ANY({sg_ids})
            GROUP BY a_sg.sequencing_group_id
        """

        acur = await self.connection.pg_connection.execute(_query)
        rows = await acur.fetchall()
        return {r['sg_id']: r['timestamp_completed'] for r in rows}

    async def get_audit_log_for_analysis_ids(
        self, analysis_ids: list[int]
    ) -> dict[int, list[AuditLogInternal]]:
        """Get audit logs for analysis IDs"""
        return await self.get_all_audit_logs_for_table('analysis', analysis_ids)
