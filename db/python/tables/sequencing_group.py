from collections import defaultdict
from datetime import date
from string.templatelib import Template

from dateutil.relativedelta import relativedelta
from psycopg import sql
from psycopg.rows import class_row

from db.python.filters.generic import GenericFilter, join_sql_with_AND
from db.python.filters.sequencing_group import SequencingGroupFilter
from db.python.tables.base import DbBase
from db.python.utils import InternalError, to_db_json
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
        sg_filter: SequencingGroupFilter,
        skip: int | None = None,
        limit: int | None = None,
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
            'active_only': t'NOT sg.archived',
            'has_cram': t'has_cram',
            # this is on the inner query, so won't conflict with the provided alias
            'external_id': 'sgexid.external_id',
        }

        base_query_components: list[Template] = []
        where_templates: list[Template] = []

        # Base query
        base_query_components.append(
            t"""
            SELECT DISTINCT sg.id
            FROM sequencing_group AS sg
            LEFT JOIN sample s ON s.id = sg.sample_id
            LEFT JOIN sequencing_group_external_id sgexid ON sg.id = sgexid.sequencing_group_id"""
        )

        if sg_filter.sample:
            sample_where_condition = sg_filter.sample.to_sql(
                {
                    'id': 's.id',
                    'meta': 's.meta',
                    'type': 's.type',
                    'external_id': 'sexid.external_id',
                }
            )
            # Ensure that only non-empty sample filters are used
            if sample_where_condition:
                if sg_filter.sample.external_id:
                    base_query_components.append(
                        t'LEFT JOIN sample_external_id sexid ON s.id = sexid.sample_id'
                    )

                where_templates.append(sample_where_condition)

        if sg_filter.assay is not None:
            a_overrides = {
                'id': 'a.id',
                'meta': 'a.meta',
                'type': 'a.type',
                'external_id': 'aexid.external_id',
            }
            assay_where_condition = sg_filter.assay.to_sql(a_overrides)
            # Ensure that only non-empty assay filters are used
            if assay_where_condition:
                base_query_components.append(
                    t"""
                    INNER JOIN sequencing_group_assay sga ON sg.id = sga.sequencing_group_id
                    INNER JOIN assay a ON sga.assay_id = a.id"""
                )

                where_templates.append(assay_where_condition)

        if sg_filter.created_on is not None:
            created_on_condition = sg_filter.to_sql(
                {'created_on': t'MIN(LOWER(sys_period))::date'}, only=['created_on']
            )
            base_query_components.append(
                t"""
                INNER JOIN (
                    SELECT id, MIN(LOWER(sys_period)) AS created_on
                    FROM (
                        SELECT id, sys_period
                        FROM sequencing_group
                        UNION ALL
                        SELECT id, sys_period
                        FROM sequencing_group_history
                    )
                    GROUP BY id
                    HAVING {created_on_condition:q}
                ) AS sg_timequery ON sg.id = sg_timequery.id"""
            )

        if sg_filter.has_cram is not None or sg_filter.has_gvcf is not None:
            cram_where_condition = sg_filter.to_sql(
                sql_overrides, only=['has_cram', 'has_gvcf']
            )
            base_query_components.append(
                t"""
                INNER JOIN (
                    SELECT
                        asg.sequencing_group_id,
                        bool_or(a.type = 'cram') AS has_cram,
                        bool_or(a.type = 'gvcf') AS has_gvcf
                    FROM
                        analysis_sequencing_group asg JOIN analysis a ON a.id = asg.analysis_id
                    GROUP BY asg.sequencing_group_id
                ) AS sg_filequery ON sg.id = sg_filequery.sequencing_group_id"""
            )
            where_templates.append(cram_where_condition)

        # Add the rest of the filters
        remaining_filters = sg_filter.to_sql(
            sql_overrides,
            exclude=[
                'assay',
                'created_on',
                'has_cram',
                'has_gvcf',
                'sample',
            ],
        )
        where_templates.append(remaining_filters)

        where = t'WHERE {join_sql_with_AND(where_templates):q}'

        base_query_components.append(where)

        if limit:
            base_query_components.append(t'LIMIT {limit}')

        if skip:
            base_query_components.append(t'OFFSET {skip}')

        base_query = sql.SQL('\n').join(base_query_components)

        outer_query = t"""
            SELECT
                sg.id,
                s.project,
                coalesce(jsonb_object_agg(sgexid.name, sgexid.external_id) FILTER (WHERE sgexid.name IS NOT NULL), '{{}}'::jsonb) AS external_ids,
                sg.sample_id,
                sg.type,
                sg.technology,
                sg.platform,
                sg.meta,
                sg.archived
            FROM sequencing_group sg
            LEFT JOIN sample s ON s.id = sg.sample_id
            LEFT JOIN sequencing_group_external_id sgexid ON sg.id = sgexid.sequencing_group_id
            INNER JOIN (
                {base_query:q}
            ) AS sg_query ON sg.id = sg_query.id
            GROUP BY s.id, sg.id"""

        return outer_query

    async def query(
        self, filter_: SequencingGroupFilter, limit: int | None = None, skip: int = 0
    ) -> tuple[set[ProjectId], list[SequencingGroupInternal]]:
        """Query samples"""

        query = SequencingGroupTable.construct_query(
            filter_,
            limit=limit,
            skip=skip,
        )

        async with self.connection.pg_connection.cursor(
            row_factory=class_row(SequencingGroupInternal)
        ) as cur:
            await cur.execute(query)
            sgs_internal = await cur.fetchall()

        projects = set(sg.project for sg in sgs_internal if sg.project)
        return projects, sgs_internal

    async def get_sequencing_groups_by_ids(
        self, ids: list[int]
    ) -> tuple[set[ProjectId], list[SequencingGroupInternal]]:
        """
        Get sequence groups by internal identifiers
        """

        query = SequencingGroupFilter(id=GenericFilter(in_=ids), active_only=None)
        projects, sgs = await self.query(query)

        return projects, sgs

    async def get_assay_ids_by_sequencing_group_ids(
        self, ids: list[int]
    ) -> dict[int, list[int]]:
        """
        Get sequence IDs in a sequencing_group
        """
        _query = t"""
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
        _query = t"""
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
        _query = t"""
        SELECT s.project as project, sg.id as sid, s.participant_id as pid
        FROM sequencing_group sg
        INNER JOIN sample s ON sg.sample_id = s.id
        WHERE sg.type = {sequencing_type} AND project = {self.project_id}
        """

        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()

        projects = set(r['project'] for r in rows)
        participant_id_to_sids: dict[int, list[int]] = defaultdict(list)
        for r in rows:
            participant_id_to_sids[r['pid']].append(r['sid'])

        return projects, participant_id_to_sids

    async def get_samples_create_date_from_sgs(
        self, sequencing_group_ids: list[int]
    ) -> dict[SequencingGroupInternalId, date]:
        """
        Get a map of {internal_sg_id: sample_date_created} for list of sg_ids
        """
        if len(sequencing_group_ids) == 0:
            return {}

        _query = t"""
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
        _query = t"""
        SELECT
            sg.id,
            s.project,
            coalesce(jsonb_object_agg(sgexid.name, sgexid.external_id) FILTER (WHERE sgexid.name IS NOT NULL), '{{}}'::jsonb) AS external_ids,
            sg.sample_id,
            sg.type,
            sg.technology,
            sg.platform,
            sg.meta,
            sg.archived,
            asg.analysis_id
        FROM analysis_sequencing_group asg
        INNER JOIN sequencing_group sg ON sg.id = asg.sequencing_group_id
        INNER JOIN sample s ON s.id = sg.sample_id
        LEFT JOIN sequencing_group_external_id sgexid ON sg.id = sgexid.sequencing_group_id
        WHERE asg.analysis_id = ANY({analysis_ids})
        GROUP BY sg.id, s.project, asg.analysis_id
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
                sg_map[sid] = SequencingGroupInternal(**row)

        analysis_map: dict[int, list[SequencingGroupInternal]] = {
            analysis_id: [sg_map[sgid] for sgid in sgids]
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
        external_ids: dict[str, str] | None = None,
        meta: dict | None = None,
    ) -> int:
        """Create sequence group"""
        values = {
            'sample_id': sample_id,
            'type': type_,
            'technology': technology,
            'platform': platform,
            'meta': to_db_json(meta or {}),
        }
        # check if any values are None and raise an exception if so
        bad_keys = [k for k, v in values.items() if v is None]
        if bad_keys:
            raise ValueError(f'Must provide values for {", ".join(bad_keys)}')

        # Ensure that capitalisation is consistent
        values['type'] = values['type'].lower()
        values['technology'] = values['technology'].lower()
        values['platform'] = values['platform'].lower()

        get_existing_query = t"""
        SELECT id
        FROM sequencing_group
        WHERE
            sample_id = {sample_id}
            AND type = {values['type'].lower()}
            AND technology = {values['technology'].lower()}
            AND platform = {values['platform'].lower()}
            AND NOT archived
        """
        conn = self.connection.pg_connection

        cur = await conn.execute(get_existing_query)
        existing_sg_ids = await cur.fetchall()

        audit_log_id = await self.audit_log_id()
        _sq_insert_query = t"""
        INSERT INTO sequencing_group
            (sample_id, type, technology, platform, meta, audit_log_id, archived)
        VALUES
            ({sample_id}, {values['type']}, {values['technology']}, {values['platform']}, {values['meta']}, {audit_log_id}, false)
        RETURNING id;
        """

        external_id_query = """
        INSERT INTO sequencing_group_external_id
            (project, sequencing_group_id, external_id, name, null_if_archived, audit_log_id)
        VALUES
            (%(project)s, %(sequencing_group_id)s, %(external_id)s, %(name)s, %(null_if_archived)s, %(audit_log_id)s)
        """

        _sg_assay_linker = """
        INSERT INTO sequencing_group_assay
            (sequencing_group_id, assay_id, audit_log_id)
        VALUES
            (%(seqgroup)s, %(assayid)s, %(audit_log_id)s)
        """

        async with self.connection.transaction():
            if existing_sg_ids:
                await self.archive_sequencing_groups([s['id'] for s in existing_sg_ids])

            cur = await conn.execute(_sq_insert_query)
            new_sg_id = await cur.fetchone()
            if not new_sg_id:
                raise InternalError('A new sequencing_group row was not created')
            
            if external_ids:
                eid_values = [
                    {
                        'project': self.connection.project_id,
                        'sequencing_group_id': new_sg_id,
                        'name': name.lower(),
                        'external_id': eid,
                        'audit_log_id': audit_log_id,
                    }
                    for name, eid in external_ids.items()
                    if eid is not None
                ]
                async with conn.cursor() as cur:
                    await cur.executemany(external_id_query, eid_values)

            if assay_ids:
                assay_id_insert_values = [
                    {
                        'seqgroup': new_sg_id['id'],
                        'assayid': s,
                        'audit_log_id': await self.audit_log_id(),
                    }
                    for s in assay_ids
                ]
                async with conn.cursor() as cur:
                    await cur.executemany(_sg_assay_linker, assay_id_insert_values)

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
            updaters.append(
                t"meta = json_merge_patch(COALESCE(meta, '{{}}'::jsonb), {to_db_json(meta)})"
            )

        if platform:
            updaters.append(t'platform = {platform}')

        _query = t"""
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

        _query = t"""
        UPDATE sequencing_group
        SET archived = true, audit_log_id = {audit_log_id}
        WHERE id = ANY({sequencing_group_ids});
        """
        # do this so we can reuse the sequencing_group_ids
        _external_id_query = t"""
        UPDATE sequencing_group_external_id
        SET null_if_archived = NULL, audit_log_id = {audit_log_id}
        WHERE sequencing_group_id = ANY({sequencing_group_ids});
        """

        async with self.connection.transaction():
            await self.connection.pg_connection.execute(_query)
            await self.connection.pg_connection.execute(_external_id_query)

    async def get_type_numbers_for_project(self, project) -> dict[str, int]:
        """
        Get number of sequencing groups for each type for a project
        Useful for the web layer
        """
        _query = t"""
            SELECT sg.type, COUNT(*) as n
            FROM sequencing_group sg
            INNER JOIN sample s ON s.id = sg.sample_id
            WHERE s.project = {project} AND NOT sg.archived
            GROUP BY sg.type
        """
        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()
        return {r['type']: r['n'] for r in rows}

    async def get_sequencing_group_counts_by_month(
        self, project_ids: list[ProjectId]
    ) -> dict[ProjectId, dict[date, dict[str, int]]]:
        """
        Returns the history of the number of each sequencing groups of each type for a list of projects.
        """
        _query = t"""
        WITH sg AS (
            SELECT id, sample_id, type, technology, MIN(LOWER(sys_period))::date as sg_first_date
            FROM (
                SELECT id, sample_id, type, technology, sys_period
                FROM sequencing_group
                UNION ALL
                SELECT id, sample_id, type, technology, sys_period
                FROM sequencing_group_history
            )
            GROUP BY id, sample_id, type, technology
        )
        SELECT project, sg.type, sg.technology, sg_first_date as sg_date, COUNT(sg.id) as num_sg
        FROM sample INNER JOIN sg ON sample.id = sg.sample_id
        WHERE project = ANY({project_ids})
        GROUP BY project, sg_date, sg.type, sg.technology
        """
        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()

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
