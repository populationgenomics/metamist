from collections import defaultdict
from datetime import date
from string.templatelib import Template
from typing import Any
from dateutil.relativedelta import relativedelta

from psycopg import AsyncConnection, sql
from psycopg.pq import TransactionStatus
from psycopg.rows import DictRow

from db.python.filters.generic import GenericFilter
from db.python.filters.sequencing_group import SequencingGroupFilter
from db.python.tables.base import DbBase
from db.python.utils import NoOpAenter, to_db_json
from models.models.project import ProjectId
from models.models.sequencing_group import (
    SequencingGroupInternal,
    SequencingGroupInternalId,
)


class SequencingGroupTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sequencing_group'

    @staticmethod
    def construct_query(
        filter_: SequencingGroupFilter,
        keys: list[str],
        skip: int | None = None,
        limit: int | None = None,
        external_id_table_alias: str | None = None,
    ) -> Template:
        """
        Construct a query for sequencing_group
        """
        sql_overrides = {
            'project': 's.project',
            'id': 'sg.id',
            'meta': 'sg.meta',
            'type': 'sg.type',
            'technology': 'sg.technology',
            'platform': 'sg.platform',
            'active_only': 'NOT sg.archived',
            # this is on the inner query, so won't conflict with the provided alias
            'external_id': 'sgexid.external_id',
        }

        _query: list[Template] = []
        where_templates: list[Template] = []

        # Base query
        _query.append(
            t"""\
            SELECT DISTINCT sg.id
            FROM sequencing_group AS sg
            LEFT JOIN sample s ON s.id = sg.sample_id
            LEFT JOIN sequencing_group_external_id sgexid ON sg.id = sgexid.sequencing_group_id"""
        )

        if filter_.sample:
            sample_where_template = filter_.sample.to_sql(
                {
                    'id': 's.id',
                    'meta': 's.meta',
                    'type': 's.type',
                    'external_id': 'sexid.external_id',
                }
            )
            if filter_.sample.external_id:
                _query.append(t'LEFT JOIN sample_external_id sexid ON s.id = sexid.sample_id')

            where_templates.append(sample_where_template)

        if filter_.assay is not None:
            a_overrides = {
                'id': 'a.id',
                'meta': 'a.meta',
                'type': 'a.type',
                'external_id': 'aexid.external_id',
            }
            assay_where_template = filter_.assay.to_sql(a_overrides)
            _query.append(
                t"""\
                INNER JOIN sequencing_group_assay sga ON sg.id = sga.sequencing_group_id'
                INNER JOIN assay a ON sga.assay_id = a.id"""
            )

            where_templates.append(assay_where_template)

        if filter_.created_on is not None:
            created_on_condition = filter_.to_sql(
                {'created_on': 'DATE(created_on)'}, only=['created_on']
            )
            _query.append(
                t"""\
                INNER JOIN (
                    SELECT id, TIMESTAMP(min(row_start)) AS created_on
                    FROM sequencing_group FOR SYSTEM_TIME ALL
                    GROUP BY id
                    HAVING {created_on_condition:q}
                ) AS sg_timequery ON sg.id = sg_timequery.id"""
            )

        if filter_.has_cram is not None or filter_.has_gvcf is not None:
            cram_where_template = filter_.to_sql(
                sql_overrides, only=['has_cram', 'has_gvcf']
            )
            _query.append(
                f"""\
                INNER JOIN (
                    SELECT
                        sequencing_group_id,
                        FIND_IN_SET('cram', GROUP_CONCAT(LOWER(anlysis_query.type))) > 0 AS has_cram,
                        FIND_IN_SET('gvcf', GROUP_CONCAT(LOWER(anlysis_query.type))) > 0 AS has_gvcf
                    FROM
                        analysis_sequencing_group
                        INNER JOIN (
                            SELECT
                                id, type
                            FROM
                                analysis
                        ) AS anlysis_query ON analysis_sequencing_group.analysis_id = anlysis_query.id
                    GROUP BY
                        sequencing_group_id
                    HAVING
                        {cram_where_template:q}
                ) AS sg_filequery ON sg.id = sg_filequery.sequencing_group_id"""
            )

        # Add the rest of the filters
        filter_template = filter_.to_sql(
            sql_overrides,
            exclude=[
                'assay',
                'created_on',
                'has_cram',
                'has_gvcf',
                'sample',
            ],
        )
        where_templates.append(filter_template)

        where = t'WHERE ' + sql.SQL(' AND ').join(where_templates)

        _query.append(where)

        if limit:
            _query.append(t'LIMIT {limit}')

        if skip:
            _query.append(t'OFFSET {skip}')

        _query_str = sql.SQL('\n').join(_query)

        ex_id_join = t''
        if external_id_table_alias:
            ex_id_join = t'LEFT JOIN sequencing_group_external_id {external_id_table_alias:i} ON sg.id = {external_id_table_alias:i}.sequencing_group_id'

        _outer_query = (
            t"""\
            SELECT {', '.join(keys)}
            FROM sequencing_group sg
            LEFT JOIN sample s ON s.id = sg.sample_id
            {ex_id_join:q}
            INNER JOIN (
                {_query_str:q}
            ) AS sg_query ON sg.id = sg_query.id
            GROUP BY sg.id"""
        )

        return _outer_query

    async def query(
        self, filter_: SequencingGroupFilter, limit: int | None = None, skip: int = 0
    ) -> tuple[set[ProjectId], list[SequencingGroupInternal]]:
        """Query samples"""

        keys = [
            'sg.id',
            's.project',
            'jsonb_object_agg(sgexid.name, sgexid.external_id) as external_ids',
            'sg.sample_id',
            'sg.type',
            'sg.technology',
            'sg.platform',
            'sg.meta',
            'sg.archived',
        ]

        query = SequencingGroupTable.construct_query(
            filter_,
            keys=keys,
            external_id_table_alias='sgexid',
            limit=limit,
            skip=skip,
        )

        conn: AsyncConnection[DictRow] = self.connection.pg_connection
        async with conn.cursor() as cur:
            await cur.execute(query)
            rows = await cur.fetchall()

        sgs = [SequencingGroupInternal.from_db(**dict(r)) for r in rows]
        projects = set(sg.project for sg in sgs if sg.project)
        return projects, sgs

    async def get_sequencing_groups_by_ids(
        self, ids: list[int]
    ) -> tuple[set[ProjectId], list[SequencingGroupInternal]]:
        """
        Get sequence groups by internal identifiers
        """

        query = SequencingGroupFilter(id=GenericFilter(in_=ids))
        projects, sgs = await self.query(query)

        return projects, sgs

    async def get_assay_ids_by_sequencing_group_ids(
        self, ids: list[int]
    ) -> dict[int, list[int]]:
        """
        Get sequence IDs in a sequencing_group
        """
        _query = t"""\
            SELECT sga.sequencing_group_id, sga.assay_id
            FROM sequencing_group_assay sga
            WHERE sga.sequencing_group_id = ANY({ids})
        """
        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()

        sequencing_groups: dict[int, list[int]] = defaultdict(list)
        for row in rows:
            sequencing_groups[row['sequencing_group_id']].append(row['assay_id'])

        return dict(sequencing_groups)

    async def get_all_sequencing_group_ids_by_sample_ids_by_type(
        self,
    ) -> dict[int, dict[str, list[int]]]:
        """
        Get all sequencing group IDs by sample IDs by type
        """
        _query = t"""\
        SELECT s.id as sid, sg.id as sgid, sg.type as sgtype
        FROM sample s
        INNER JOIN sequencing_group sg ON s.id = sg.sample_id
        WHERE project = {self.project_id}
        """
        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()
        
        sequencing_group_ids_by_sample_ids_by_type: dict[int, dict[str, list[int]]] = (
            defaultdict(lambda: defaultdict(list))
        )
        for row in rows:
            sample_id = row['sid']
            sg_id = row['sgid']
            sg_type = row['sgtype']
            sequencing_group_ids_by_sample_ids_by_type[sample_id][sg_type].append(sg_id)

        return sequencing_group_ids_by_sample_ids_by_type

    async def get_participant_ids_and_sequencing_group_ids_for_sequencing_type(
        self, sequencing_type: str
    ) -> tuple[set[ProjectId], dict[int, list[int]]]:
        """
        Get participant IDs for a specific sequence type.
        Particularly useful for seqr like cases
        """
        _query = t"""\
        SELECT s.project as project, sg.id as sid, s.participant_id as pid
        FROM sequencing_group sg
        INNER JOIN sample s ON sg.sample_id = s.id
        WHERE sg.type = {sequencing_type} AND project = {self.project_id}
        """
        cur = self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()

        projects = set(r['project'] for r in rows)
        participant_id_to_sids: dict[int, list[int]] = defaultdict(list)
        for r in rows:
            participant_id_to_sids[r['pid']].append(r['sid'])

        return projects, participant_id_to_sids

    async def get_sequencing_groups_create_date(
        self, sequencing_group_ids: list[int]
    ) -> dict[int, date]:
        """Get a map of {internal_sample_id: date_created} for list of sample_ids"""
        if len(sequencing_group_ids) == 0:
            return {}
        
        _query = t"""\
            SELECT id, MIN(lower(sys_period)) as min_row_start
            FROM (
                SELECT id, sys_period
                FROM sequencing_group
                WHERE id = ANY({sequencing_group_ids})
                
                UNION ALL
                
                SELECT id, sys_period  
                FROM sequencing_group_history
                WHERE id = ANY({sequencing_group_ids})
            ) AS all_versions
            GROUP BY id"""
        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()

        return {r['id']: r['min_row_start'].date() for r in rows}

    async def get_samples_create_date_from_sgs(
        self, sequencing_group_ids: list[int]
    ) -> dict[SequencingGroupInternalId, date]:
        """
        Get a map of {internal_sg_id: sample_date_created} for list of sg_ids
        """
        if len(sequencing_group_ids) == 0:
            return {}

        _query = t"""\
        SELECT sg.id, MIN(lower(s.sys_period)) as min_row_start
        FROM sequencing_group sg
        INNER JOIN (
            SELECT id, sys_period FROM sample
            UNION ALL
            SELECT id, sys_period FROM sample_history
        ) s ON s.id = sg.sample_id
        WHERE sg.id = ANY({sequencing_group_ids})
        GROUP BY sg.id
        """
        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()

        return {r['id']: r['min_row_start'].date() for r in rows}

    async def get_sequencing_groups_by_analysis_ids(
        self, analysis_ids: list[int]
    ) -> tuple[set[ProjectId], dict[int, list[SequencingGroupInternal]]]:
        """Get map of samples by analysis_ids"""
        keys = [
            'sg.id',
            's.project',
            'jsonb_object_agg(sgexid.name, sgexid.external_id) as external_ids',
            'sg.sample_id',
            'sg.type',
            'sg.technology',
            'sg.platform',
            'sg.meta',
            'sg.archived',
        ]
        _query = t"""\
        SELECT {sql.SQL(', ').join(keys):q}, asg.analysis_id
        FROM analysis_sequencing_group asg
        INNER JOIN sequencing_group sg ON sg.id = asg.sequencing_group_id
        INNER JOIN sample s ON s.id = sg.sample_id
        LEFT JOIN sequencing_group_external_id sgexid ON sg.id = sgexid.sequencing_group_id
        WHERE asg.analysis_id = ANY({analysis_ids})
        GROUP BY sg.id, asg.analysis_id
        """
        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()

        mapped_analysis_to_sequencing_group_id: dict[int, list[int]] = defaultdict(list)
        sg_map: dict[int, SequencingGroupInternal] = {}
        projects: set[int] = set()
        for row in rows:
            sid = row['id']
            analysis_id = row.pop('analysis_id')
            mapped_analysis_to_sequencing_group_id[analysis_id].append(sid)
            projects.add(row['project'])

            if sid not in sg_map:
                sg_map[sid] = SequencingGroupInternal.from_db(**row)

        analysis_map: dict[int, list[SequencingGroupInternal]] = {
            analysis_id: [sg_map.get(sgid) for sgid in sgids]
            for analysis_id, sgids in mapped_analysis_to_sequencing_group_id.items()
        }

        return projects, analysis_map

    async def create_sequencing_group(
        self,
        sample_id: int,
        type_: str,
        technology: str,
        platform: str,
        assay_ids: list[int],
        meta: dict | None = None,
    ) -> int:
        """Create sequence group"""
        assert sample_id is not None
        assert type_ is not None
        assert technology is not None
        assert platform is not None

        values = {
            'sample_id': sample_id,
            'type': type_.lower(),
            'technology': technology.lower(),
            'platform': platform.lower(),
            'meta': to_db_json(meta or {}),
        }
        # check if any values are None and raise an exception if so
        bad_keys = [k for k, v in values.items() if v is None]
        if bad_keys:
            raise ValueError(f'Must provide values for {", ".join(bad_keys)}')

        get_existing_query = t"""\
        SELECT id
        FROM sequencing_group
        WHERE
            sample_id = {sample_id}
            AND type = {values['type']}
            AND technology = {values['technology']}
            AND platform = {values['platform']}
            AND NOT archived
        """
        conn = self.connection.pg_connection

        cur = await conn.execute(get_existing_query)
        existing_sg_ids = await cur.fetchall()

        audit_log_id = await self.audit_log_id()
        _sq_insert_query = t"""\
        INSERT INTO sequencing_group
            (sample_id, type, technology, platform, meta, audit_log_id, archived)
        VALUES
            ({sample_id}, {values['type']}, {values['technology']}, {values['platform']}, {values['meta']}, {audit_log_id}, false)
        RETURNING id;
        """

        _sg_assay_linker = """
        INSERT INTO sequencing_group_assay
            (sequencing_group_id, assay_id, audit_log_id)
        VALUES
            (%(seqgroup)s, %(assayid)s, %(audit_log_id)s)
        """

        in_transaction = conn.info.transaction_status == TransactionStatus.INTRANS
        with_function = conn.transaction if not in_transaction else NoOpAenter

        async with with_function():
            if existing_sg_ids:
                await self.archive_sequencing_groups([s['id'] for s in existing_sg_ids])

            cur = await conn.execute(_sq_insert_query)
            new_sg_id = await cur.fetchone()

            if assay_ids:
                assay_id_insert_values = [
                    {
                        'seqgroup': new_sg_id['id'],
                        'assayid': s,
                        'audit_log_id': await self.audit_log_id(),
                    }
                    for s in assay_ids
                ]

                await conn.executemany(_sg_assay_linker, assay_id_insert_values)

            return new_sg_id['id']

    async def update_sequencing_group(
        self, sequencing_group_id: int, meta: dict, platform: str
    ):
        """
        Update meta / platform on sequencing_group
        """
        audit_log_id = await self.audit_log_id()

        updaters = [t'audit_log_id = {audit_log_id}']

        if meta:
            updaters.append(t'meta = json_merge_patch(COALESCE(meta, \'{{}}\'::jsonb), {to_db_json(meta)})')

        if platform:
            updaters.append(t'platform = {platform}')

        _query = t"""\
        UPDATE sequencing_group
        SET {sql.SQL(', ').join(updaters):q}
        WHERE id = {sequencing_group_id}
        """

        conn = self.connection.pg_connection
        await conn.execute(_query)

    async def archive_sequencing_groups(self, sequencing_group_ids: list[int]):
        """
        Archive sequence group by setting archive flag to TRUE
        """
        audit_log_id = await self.audit_log_id()

        _query = t"""\
        UPDATE sequencing_group
        SET archived = true, audit_log_id = {audit_log_id}
        WHERE id = ANY({sequencing_group_ids});
        """
        # do this so we can reuse the sequencing_group_ids
        _external_id_query = t"""\
        UPDATE sequencing_group_external_id
        SET null_if_archived = NULL, audit_log_id = {audit_log_id}
        WHERE sequencing_group_id = ANY({sequencing_group_ids});
        """

        conn = self.connection.pg_connection
        async with conn.transaction():
            await conn.execute(_query)
            await conn.execute(_external_id_query)

    async def get_type_numbers_for_project(self, project) -> dict[str, int]:
        """
        Get number of sequencing groups for each type for a project
        Useful for the web layer
        """
        _query = """
SELECT sg.type, COUNT(*) as n
FROM sequencing_group sg
INNER JOIN sample s ON s.id = sg.sample_id
WHERE s.project = :project AND NOT sg.archived
GROUP BY sg.type
        """
        rows = await self.connection.fetch_all(_query, {'project': project})
        return {r['type']: r['n'] for r in rows}

    async def get_sequencing_group_counts_by_month(
        self, project_ids: list[ProjectId]
    ) -> dict[ProjectId, dict[date, dict[str, int]]]:
        """
        Returns the history of the number of each sequencing groups of each type for a list of projects.
        """
        _query = f"""
        WITH sg AS (
            SELECT id, sample_id, type, technology, min(row_start) as sg_first_date
            FROM sequencing_group FOR SYSTEM_TIME ALL
            GROUP BY id
        )
        SELECT project, sg.type, sg.technology, CONVERT(sg_first_date, DATE) as sg_date, COUNT(sg.id) as num_sg
        FROM sample INNER JOIN sg ON sample.id = sg.sample_id
        WHERE project in :project_ids
        GROUP BY project, sg_date, sg.type, sg.technology
        """
        values = {'project_ids': project_ids}

        rows = await self.connection.fetch_all(_query, values)

        if not rows:
            return defaultdict(dict)

        # Organise the data by month into a dictionary, grouping sequencing group types together by month.
        project_histories: dict[ProjectId, dict[date, dict[str, int]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        for row in rows:
            project = row['project']
            month_created: date = row['sg_date'].replace(day=1)
            sg_type = row['type']
            sg_tech = row['technology']
            num_sg = row['num_sg']

            project_histories[project][month_created][f'{sg_type}|||{sg_tech}'] = num_sg

        # We want the total number of each sg type over time, so we need to accumulate and
        # fill in the missing months.
        todays_month = date.today().replace(day=1)
        for history in project_histories.values():
            iteration_month = min(
                history.keys()
            )  # The month currently being filled in.
            type_totals: dict[str, int] = defaultdict(lambda: 0)

            # By starting at the earliest month and working towards today, we won't skip any dates.
            while iteration_month <= todays_month:
                iteration_counts = history.get(iteration_month, {})

                # The result from the database provides the sq types added in a given month,
                # but we want the total number.
                for sg_key, count in iteration_counts.items():
                    type_totals[sg_key] += count

                iteration_counts.update(type_totals)
                history[iteration_month] = iteration_counts

                iteration_month += relativedelta(months=1)

        return project_histories
