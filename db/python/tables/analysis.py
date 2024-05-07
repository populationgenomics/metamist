# pylint: disable=too-many-instance-attributes
import dataclasses
import datetime
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

from db.python.tables.base import DbBase
from db.python.utils import (
    GenericFilter,
    GenericFilterModel,
    GenericMetaFilter,
    NotFoundError,
    to_db_json,
)
from models.enums import AnalysisStatus
from models.models.analysis import AnalysisInternal
from models.models.audit_log import AuditLogInternal
from models.models.project import ProjectId


@dataclasses.dataclass
class AnalysisFilter(GenericFilterModel):
    """Filter for analysis"""

    id: GenericFilter[int] | None = None
    sample_id: GenericFilter[int] | None = None
    sequencing_group_id: GenericFilter[int] | None = None
    cohort_id: GenericFilter[int] | None = None
    project: GenericFilter[int] | None = None
    type: GenericFilter[str] | None = None
    status: GenericFilter[AnalysisStatus] | None = None
    meta: GenericMetaFilter | None = None
    output: GenericFilter[str] | None = None
    active: GenericFilter[bool] | None = None
    timestamp_completed: GenericFilter[datetime.datetime] | None = None

    def __hash__(self):  # pylint: disable=useless-parent-delegation
        return super().__hash__()


class AnalysisTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    table_name = 'analysis'

    async def get_project_ids_for_analysis_ids(
        self, analysis_ids: List[int]
    ) -> Set[ProjectId]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = (
            'SELECT project FROM analysis WHERE id in :analysis_ids GROUP BY project'
        )
        rows = await self.connection.fetch_all(_query, {'analysis_ids': analysis_ids})
        return set(r['project'] for r in rows)

    async def create_analysis(
        self,
        analysis_type: str,
        status: AnalysisStatus,
        sequencing_group_ids: List[int] | None = None,
        cohort_ids: List[int] | None = None,
        meta: Optional[Dict[str, Any]] = None,
        output: str | None = None,
        active: bool = True,
        project: ProjectId | None = None,
    ) -> int:
        """
        Create a new sample, and add it to database
        """

        async with self.connection.transaction():
            kv_pairs = [
                ('type', analysis_type),
                ('status', status.value),
                ('meta', to_db_json(meta or {})),
                ('output', output),
                ('audit_log_id', await self.audit_log_id()),
                ('project', project or self.project),
                ('active', active if active is not None else True),
            ]

            if status == AnalysisStatus.COMPLETED:
                kv_pairs.append(('timestamp_completed', datetime.datetime.utcnow()))

            kv_pairs = [(k, v) for k, v in kv_pairs if v is not None]
            keys = [k for k, _ in kv_pairs]
            cs_keys = ', '.join(keys)
            cs_id_keys = ', '.join(f':{k}' for k in keys)
            _query = f"""\
INSERT INTO analysis
    ({cs_keys})
VALUES ({cs_id_keys}) RETURNING id;"""

            id_of_new_analysis = await self.connection.fetch_val(
                _query,
                dict(kv_pairs),
            )

            if sequencing_group_ids:
                await self.add_sequencing_groups_to_analysis(
                    id_of_new_analysis, sequencing_group_ids
                )

            if cohort_ids:
                await self.add_cohorts_to_analysis(id_of_new_analysis, cohort_ids)

        return id_of_new_analysis

    async def add_sequencing_groups_to_analysis(
        self, analysis_id: int, sequencing_group_ids: List[int]
    ):
        """Add samples to an analysis (through the linked table)"""
        _query = """
            INSERT INTO analysis_sequencing_group
                (analysis_id, sequencing_group_id, audit_log_id)
            VALUES (:aid, :sid, :audit_log_id)
        """

        audit_log_id = await self.audit_log_id()
        values = map(
            lambda sid: {
                'aid': analysis_id,
                'sid': sid,
                'audit_log_id': audit_log_id,
            },
            sequencing_group_ids,
        )
        await self.connection.execute_many(_query, list(values))

    async def add_cohorts_to_analysis(self, analysis_id: int, cohort_ids: list[int]):
        """Add cohorts to an analysis (through the linked table)"""
        _query = """
            INSERT INTO analysis_cohort
                (analysis_id, cohort_id, audit_log_id)
            VALUES (:aid, :cid, :audit_log_id)
        """

        audit_log_id = await self.audit_log_id()
        values = map(
            lambda cid: {
                'aid': analysis_id,
                'cid': cid,
                'audit_log_id': audit_log_id,
            },
            cohort_ids,
        )
        await self.connection.execute_many(_query, list(values))

    async def find_sgs_in_joint_call_or_es_index_up_to_date(
        self, date: datetime.date
    ) -> set[int]:
        """Find all the sequencing groups that have been in a joint-call or es-index up to a date"""
        _query = """
        SELECT DISTINCT asg.sequencing_group_id
        FROM analysis_sequencing_group asg
        INNER JOIN analysis a ON asg.analysis_id = a.id
        WHERE
            a.type IN ('joint-calling', 'es-index')
            AND a.timestamp_completed <= :date
        """

        results = await self.connection.fetch_all(_query, {'date': date})
        return {r['sequencing_group_id'] for r in results}

    async def update_analysis(
        self,
        analysis_id: int,
        status: AnalysisStatus,
        meta: Dict[str, Any] = None,
        active: bool = None,
        output: Optional[str] = None,
    ):
        """
        Update the status of an analysis, set timestamp_completed if relevant
        """

        fields: Dict[str, Any] = {
            'analysis_id': analysis_id,
            'on_behalf_of': self.author,
            'audit_log_id': await self.audit_log_id(),
        }
        setters = ['audit_log_id = :audit_log_id', 'on_behalf_of = :on_behalf_of']
        if status:
            setters.append('status = :status')
            fields['status'] = status.value

        if active is not None:
            setters.append('active = :active')
            fields['active'] = active

        if status == AnalysisStatus.COMPLETED:
            fields['timestamp_completed'] = datetime.datetime.utcnow()
            setters.append('timestamp_completed = :timestamp_completed')

        if output:
            fields['output'] = output
            setters.append('output = :output')

        if meta is not None and len(meta) > 0:
            fields['meta'] = to_db_json(meta)
            setters.append('meta = JSON_MERGE_PATCH(COALESCE(meta, "{}"), :meta)')

        fields_str = ', '.join(setters)
        _query = f'UPDATE analysis SET {fields_str} WHERE id = :analysis_id'

        await self.connection.execute(_query, fields)

    async def query(self, filter_: AnalysisFilter) -> List[AnalysisInternal]:
        """
        Get analysis by various (AND'd) criteria
        """

        required_fields = [
            filter_.id,
            filter_.sequencing_group_id,
            filter_.project,
            filter_.sample_id,
            filter_.cohort_id,
        ]

        if not any(required_fields):
            raise ValueError(
                'Must provide at least one of id, sample_id, sequencing_group_id, cohort_id '
                'or project to filter on'
            )

        sg_where_str, sg_values = filter_.to_sql(
            {
                'id': 'a.id',
                'sample_id': 'a_sg.sample_id',
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

        cohort_where_str, cohort_values = filter_.to_sql(
            {
                'id': 'a.id',
                'project': 'a.project',
                'type': 'a.type',
                'status': 'a.status',
                'meta': 'a.meta',
                'output': 'a.output',
                'active': 'a.active',
                'cohort_id': 'a_c.cohort_id',
            },
        )

        retvals: Dict[int, AnalysisInternal] = {}

        if filter_.cohort_id and filter_.sequencing_group_id:
            raise ValueError('Cannot filter on both cohort_id and sequencing_group_id')

        if filter_.cohort_id:
            _cohort_query = f"""
            SELECT a.id as id, a.type as type, a.status as status,
                    a.output as output, a_c.cohort_id as cohort_id,
                    a.project as project, a.timestamp_completed as timestamp_completed,
                    a.active as active, a.meta as meta, a.author as author
            FROM analysis a
            LEFT JOIN analysis_cohort a_c ON a.id = a_c.analysis_id
            WHERE {cohort_where_str}
            """
            rows = await self.connection.fetch_all(_cohort_query, cohort_values)
            for row in rows:
                key = row['id']
                if key in retvals:
                    retvals[key].cohort_ids.append(row['cohort_id'])
                else:
                    retvals[key] = AnalysisInternal.from_db(**dict(row))

            if retvals:
                _query_sg_ids = """
                SELECT sequencing_group_id, analysis_id
                FROM analysis_sequencing_group
                WHERE analysis_id IN :analysis_ids
                """
                sg_ids = await self.connection.fetch_all(
                    _query_sg_ids, {'analysis_ids': list(retvals.keys())}
                )

                for row in sg_ids:
                    retvals[row['analysis_id']].sequencing_group_ids.append(
                        row['sequencing_group_id']
                    )

        else:
            _query = f"""
            SELECT a.id as id, a.type as type, a.status as status,
                    a.output as output, a_sg.sequencing_group_id as sequencing_group_id,
                    a.project as project, a.timestamp_completed as timestamp_completed,
                    a.active as active, a.meta as meta, a.author as author
            FROM analysis a
            LEFT JOIN analysis_sequencing_group a_sg ON a.id = a_sg.analysis_id
            WHERE {sg_where_str}
            """
            rows = await self.connection.fetch_all(_query, sg_values)
            for row in rows:
                key = row['id']
                if key in retvals:
                    retvals[key].sequencing_group_ids.append(row['sequencing_group_id'])
                else:
                    retvals[key] = AnalysisInternal.from_db(**dict(row))

            if retvals:
                _query_cohort_ids = """
                SELECT analysis_id, cohort_id
                FROM analysis_cohort
                WHERE analysis_id IN :analysis_ids
                """
                cohort_ids = await self.connection.fetch_all(
                    _query_cohort_ids, {'analysis_ids': list(retvals.keys())}
                )

                for row in cohort_ids:
                    retvals[row['analysis_id']].cohort_ids.append(row['cohort_id'])

        return list(retvals.values())

    async def get_latest_complete_analysis_for_type(
        self,
        project: ProjectId,
        analysis_type: str,
        meta: Dict[str, Any] = None,
    ):
        """Find the most recent completed analysis for some analysis type"""

        values = {'project': project, 'type': analysis_type}

        meta_str = ''
        if meta:
            for k, v in meta.items():
                k_replacer = f'meta_{k}'
                meta_str += f" AND json_extract(meta, '$.{k}') = :{k_replacer}"
                if v is None:
                    # mariadb does a bad cast for NULL
                    v = 'null'
                values[k_replacer] = v

        _query = f"""
SELECT a.id as id, a.type as type, a.status as status,
        a.output as output, a_sg.sample_id as sample_id,
        a.project as project, a.timestamp_completed as timestamp_completed,
        a.meta as meta
FROM analysis_sequencing_group a_sg
INNER JOIN analysis a ON a_sg.analysis_id = a.id
WHERE a.id = (
    SELECT id FROM analysis
    WHERE active AND type = :type AND project = :project AND status = 'completed' AND timestamp_completed IS NOT NULL{meta_str}
    ORDER BY timestamp_completed DESC
    LIMIT 1
)
"""
        rows = await self.connection.fetch_all(_query, values)
        if len(rows) == 0:
            raise NotFoundError(f"Couldn't find any analysis with type {analysis_type}")
        a = AnalysisInternal.from_db(**dict(rows[0]))
        # .from_db maps 'sample_id' -> sample_ids
        for row in rows[1:]:
            a.sample_ids.append(row['sample_id'])

        return a

    async def get_all_sequencing_group_ids_without_analysis_type(
        self, analysis_type: str, project: ProjectId
    ) -> list[int]:
        """
        Find all the samples in the sample_id list that a
        """
        _query = """
SELECT sg.id FROM sequencing_group sg
WHERE s.project = :project AND
      id NOT IN (
          SELECT a_sg.sequencing_group_id FROM analysis_sequencing_group a_sg
          LEFT JOIN analysis a ON a_sg.analysis_id = a.id
          WHERE a.type = :analysis_type AND a.active
      )
;"""

        rows = await self.connection.fetch_all(
            _query,
            {'analysis_type': analysis_type, 'project': project or self.project},
        )
        return [row[0] for row in rows]

    async def get_incomplete_analyses(
        self, project: ProjectId
    ) -> List[AnalysisInternal]:
        """
        Gets details of analysis with status queued or in-progress
        """
        _query = """
SELECT a.id as id, a.type as type, a.status as status,
        a.output as output, a_sg.sequencing_group_id as sequencing_group_id,
        a.project as project, a.meta as meta
FROM analysis_sequencing_group a_sg
INNER JOIN analysis a ON a_sg.analysis_id = a.id
WHERE a.project = :project AND a.active AND (a.status='queued' OR a.status='in-progress')
"""
        rows = await self.connection.fetch_all(
            _query, {'project': project or self.project}
        )
        analysis_by_id = {}
        for row in rows:
            aid = row['id']
            if aid not in analysis_by_id:
                analysis_by_id[aid] = AnalysisInternal.from_db(**dict(row))
            else:
                analysis_by_id[aid].sample_ids.append(row['sequencing_group_id'])

        return list(analysis_by_id.values())

    async def get_latest_complete_analysis_for_sequencing_group_ids_by_type(
        self, analysis_type: str, sequencing_group_ids: List[int]
    ) -> List[AnalysisInternal]:
        """Get the latest complete analysis for samples (one per sample)"""
        _query = """
SELECT
    a.id AS id, a.type as type, a.status as status, a.output as output,
    a.project as project, a_sg.sequencing_group_id as sample_id,
    a.timestamp_completed as timestamp_completed, a.meta as meta
FROM analysis a
LEFT JOIN analysis_sequencing_group a_sg ON a_sg.analysis_id = a.id
WHERE
    a.active AND
    a.type = :type AND
    a.timestamp_completed IS NOT NULL AND
    a_sg.sequencing_group_id in :sequencing_group_ids
ORDER BY a.timestamp_completed DESC
        """
        expected_type = ('gvcf', 'cram', 'qc')
        if analysis_type not in expected_type:
            expected_types_str = ', '.join(a for a in expected_type)
            raise ValueError(
                f'Received analysis type {analysis_type!r}", expected {expected_types_str!r}'
            )

        values = {'sequencing_group_ids': sequencing_group_ids, 'type': analysis_type}
        rows = await self.connection.fetch_all(_query, values)
        seen_sequencing_group_ids = set()
        analyses: List[AnalysisInternal] = []
        for row in rows:
            if row['sequencing_group_id'] in seen_sequencing_group_ids:
                continue
            seen_sequencing_group_ids.add(row['sequencing_group_id'])
            analyses.append(AnalysisInternal.from_db(**dict(row)))

        # reverse after timestamp_completed
        return analyses[::-1]

    async def get_analysis_by_id(
        self, analysis_id: int
    ) -> Tuple[ProjectId, AnalysisInternal]:
        """Get analysis object by analysis_id"""
        _query = """
SELECT
    a.id as id, a.type as type, a.status as status,
    a.output as output, a.project as project,
    a_sg.sequencing_group_id as sequencing_group_id,
    a.timestamp_completed as timestamp_completed, a.meta as meta
FROM analysis a
LEFT JOIN analysis_sequencing_group a_sg ON a_sg.analysis_id = a.id
WHERE a.id = :analysis_id
"""
        rows = await self.connection.fetch_all(_query, {'analysis_id': analysis_id})
        if len(rows) == 0:
            raise NotFoundError(f"Couldn't find analysis with id = {analysis_id}")

        project = rows[0]['project']

        a = AnalysisInternal.from_db(**dict(rows[0]))
        for row in rows[1:]:
            a.sample_ids.append(row['sequencing_group_id'])

        return project, a

    async def get_analyses_for_samples(
        self,
        sample_ids: list[int],
        analysis_type: str | None,
        status: AnalysisStatus | None,
    ) -> tuple[set[ProjectId], list[AnalysisInternal]]:
        """
        Get relevant analyses for a sample, optional type / status filters
        map_sample_ids will map the Analysis.sample_ids component,
        not required for GraphQL sources.
        """
        values: dict[str, Any] = {'sample_ids': sample_ids}
        wheres = ['a_sg.sequencing_group_id IN :sequencing_group_ids']

        if analysis_type:
            wheres.append('a.type = :atype')
            values['atype'] = analysis_type

        if status:
            wheres.append('a.status = :status')
            values['status'] = status.value

        _query = f"""
    SELECT
        a.id as id, a.type as type, a.status as status,
        a.output as output, a.project as project,
        a_sg.sequencing_group_id as sequencing_group_id,
        a.timestamp_completed as timestamp_completed, a.meta as meta
    FROM analysis a
    INNER JOIN analysis_sequencing_group a_sg ON a_sg.analysis_id = a.id
    WHERE {' AND '.join(wheres)}
        """

        rows = await self.connection.fetch_all(_query, values)
        analyses = {}
        projects: set[ProjectId] = set()
        for a in rows:
            a_id = a['id']
            if a_id not in analyses:
                analyses[a_id] = AnalysisInternal.from_db(**dict(a))
                projects.add(a['project'])

            analyses[a_id].sample_ids.append(a['sample_id'])

        return projects, list(analyses.values())

    async def get_sample_cram_path_map_for_seqr(
        self,
        project: ProjectId,
        sequencing_types: list[str],
        participant_ids: list[int] = None,
    ) -> List[dict[str, str]]:
        """Get (ext_sample_id, cram_path, internal_id) map"""

        values: dict[str, Any] = {'project': project}
        filters = [
            'a.active',
            'a.type = "cram"',
            'a.status = "completed"',
            'p.project = :project',
        ]
        if sequencing_types:
            if len(sequencing_types) == 1:
                seq_check = '= :seq_type'
                values['seq_type'] = sequencing_types[0]
            else:
                seq_check = 'IN :seq_types'
                values['seq_types'] = sequencing_types

            filters.append('JSON_VALUE(a.meta, "$.sequencing_type") ' + seq_check)

        if participant_ids:
            filters.append('p.id IN :pids')
            values['pids'] = list(participant_ids)

        _query = f"""
SELECT p.external_id as participant_id, a.output as output, sg.id as sequencing_group_id
FROM analysis a
INNER JOIN analysis_sequencing_group a_sg ON a_sg.analysis_id = a.id
INNER JOIN sequencing_group sg ON a_sg.sequencing_group_id = sg.id
INNER JOIN sample s ON sg.sample_id = s.id
INNER JOIN participant p ON s.participant_id = p.id
WHERE
    {' AND '.join(filters)}
ORDER BY a.timestamp_completed DESC;
"""

        rows = await self.connection.fetch_all(_query, values)
        # many per analysis
        return [dict(d) for d in rows]

    async def get_analysis_runner_log(
        self,
        project_ids: List[int] = None,
        # author: str = None,
        output_dir: str = None,
        ar_guid: str = None,
    ) -> List[AnalysisInternal]:
        """
        Get log for the analysis-runner, useful for checking this history of analysis
        """
        values: dict[str, Any] = {}
        wheres = [
            "type = 'analysis-runner'",
            'active',
        ]
        if project_ids:
            wheres.append('project in :project_ids')
            values['project_ids'] = project_ids

        if output_dir:
            wheres.append('(output = :output OR output LIKE :output_like)')
            values['output'] = output_dir
            values['output_like'] = f'%{output_dir}'

        if ar_guid:
            wheres.append('JSON_EXTRACT(meta, "$.ar_guid") = :ar_guid')
            values['ar_guid'] = ar_guid

        wheres_str = ' AND '.join(wheres)
        _query = f'SELECT * FROM analysis WHERE {wheres_str}'
        rows = await self.connection.fetch_all(_query, values)
        return [AnalysisInternal.from_db(**dict(r)) for r in rows]

    # region STATS

    async def get_number_of_crams_by_sequencing_type(
        self, project: ProjectId
    ) -> dict[str, int]:
        """
        Get number of crams, grouped by sequence type (one per sample per sequence type)
        """

        # Only count crams for ACTIVE sequencing groups
        _query = """
SELECT sg.type as seq_type, COUNT(*) as number_of_crams
FROM analysis a
INNER JOIN analysis_sequencing_group asga ON a.id = asga.analysis_id
INNER JOIN sequencing_group sg ON asga.sequencing_group_id = sg.id
WHERE
    a.project = :project
    AND a.status = 'completed'
    AND a.type = 'cram'
    AND NOT sg.archived
GROUP BY seq_type
        """

        rows = await self.connection.fetch_all(_query, {'project': project})

        # do it like this until I select lowercase value w/ JSON_EXTRACT
        n_counts: dict[str, int] = defaultdict(int)
        for r in rows:
            if seq_type := r['seq_type']:
                n_counts[str(seq_type).lower()] += r['number_of_crams']

        return n_counts

    async def get_seqr_stats_by_sequencing_type(
        self, project: ProjectId
    ) -> dict[str, int]:
        """
        Get number of samples in seqr (in latest es-index), grouped by sequence type
        """
        _query = """
SELECT sg.type as seq_type, COUNT(*) as n
FROM analysis a
INNER JOIN analysis_sequencing_group asga ON a.id = asga.analysis_id
INNER JOIN sequencing_group sg ON asga.sequencing_group_id = sg.id
INNER JOIN sample s on sg.sample_id = s.id
WHERE
    s.project = :project
    AND a.status = 'completed'
    AND a.type = 'es-index'
    AND NOT sg.archived
GROUP BY seq_type
        """

        rows = await self.connection.fetch_all(_query, {'project': project})
        return {r['seq_type']: r['n'] for r in rows}

    # endregion STATS

    async def get_sg_add_to_project_es_index(
        self, sg_ids: list[int]
    ) -> dict[int, datetime.date]:
        """
        Get all the sequencing groups that should be added to seqr joint calls
        """
        _query = """
        SELECT
            a_sg.sequencing_group_id as sg_id,
            MIN(a.timestamp_completed) as timestamp_completed
        FROM analysis a
        INNER JOIN analysis_sequencing_group a_sg ON a.id = a_sg.analysis_id
        WHERE
            a.status = 'completed'
            AND a.type = 'es-index'
            AND a_sg.sequencing_group_id IN :sg_ids
        GROUP BY a_sg.sequencing_group_id
        """

        rows = await self.connection.fetch_all(_query, {'sg_ids': sg_ids})
        return {r['sg_id']: r['timestamp_completed'].date() for r in rows}

    async def get_audit_log_for_analysis_ids(
        self, analysis_ids: list[int]
    ) -> dict[int, list[AuditLogInternal]]:
        """
        Get audit logs for analysis IDs
        """
        return await self.get_all_audit_logs_for_table('analysis', analysis_ids)
