import asyncio
from collections import defaultdict
from collections.abc import Iterable
from datetime import date
from string.templatelib import Template
from typing import Any

from dateutil.relativedelta import relativedelta
from psycopg import DatabaseError, sql
from psycopg.rows import class_row
from psycopg.types.json import Jsonb

from db.python.filters import GenericFilter
from db.python.filters.sample import SampleFilter
from db.python.tables.base import DbBase
from db.python.tables.meta_table import MetaTable
from db.python.utils import NotFoundError, escape_like_term
from models.base import parse_sql_bool
from models.models import PRIMARY_EXTERNAL_ORG, ProjectId
from models.models.sample import SampleInternal, sample_id_format


class SampleTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sample'

    # Convert all str in keys to sql Identifiers, and leave sql.Composable as is
    @staticmethod
    def format_keys(keys: list[str | sql.SQL]) -> list[sql.Composable]:
        return [
            sql.Identifier(*key.split('.')) if isinstance(key, str) else key
            for key in keys
        ]

    @staticmethod
    def construct_query(
        filter_: SampleFilter,
        keys: list[str | sql.SQL],
        sample_eid_table_alias: str | None = None,
        skip: int | None = None,
        limit: int | None = None,
    ):
        """
        Construct a nested sample query
        """
        query_template = t"""
            SELECT DISTINCT ss.id
            FROM sample ss
        """

        wheres: list[Template | None] = []

        # Mandatory filters that apply to the sample table
        wheres.append(
            filter_.to_sql(
                {
                    'project': 'ss.project',
                    'id': 'ss.id',
                    'meta': 'ss.meta',
                    'sample_root_id': 'ss.sample_root_id',
                    'sample_parent_id': 'ss.sample_parent_id',
                },
                exclude=['sequencing_group', 'assay', 'external_id'],
            )
        )

        if filter_.external_id:
            wheres.append(
                filter_.to_sql(
                    {
                        'external_id': 'seid.external_id',
                    },
                    only=['external_id'],
                )
            )

            # 2024-06-15 mfranklin: left join, inner join, doesn't matter as there
            #       should always be an external_id
            query_template += (
                t' LEFT JOIN sample_external_id seid ON seid.sample_id = ss.id'
            )

        if filter_.sequencing_group or filter_.assay:
            if filter_.sequencing_group:
                wheres.append(
                    filter_.sequencing_group.to_sql(
                        {
                            'id': 'sg.id',
                            'meta': 'sg.meta',
                            'type': 'sg.type',
                            'technology': 'sg.technology',
                            'platform': 'sg.platform',
                        }
                    )
                )

            query_template += t' INNER JOIN sequencing_group sg ON sg.sample_id = ss.id'

        if filter_.assay:
            wheres.append(
                filter_.assay.to_sql(
                    {
                        'id': 'a.id',
                        'meta': 'a.meta',
                        'type': 'a.type',
                    }
                )
            )

            query_template += t' INNER JOIN assay a ON a.sample_id = ss.id'

        # WHERE
        wheres = [w for w in wheres if w is not None]
        wheres_sql = sql.SQL(' AND ').join(wheres)
        query_template += t' WHERE {wheres_sql:q}' if len(wheres) > 0 else t''

        # ORDER BY, LIMIT, OFFSET
        query_template += t' ORDER BY pp.id' if limit or skip else t''
        query_template += t' LIMIT {limit}' if limit else t''
        query_template += t' OFFSET {skip}' if skip else t''

        # INNER JOIN query with sample table to get the requested keys
        sample_eid_join = (
            t'INNER JOIN sample_external_id {sample_eid_table_alias:i} ON {sample_eid_table_alias:i}.sample_id = s.id'
            if sample_eid_table_alias
            else t''
        )

        # Turn the keys into sql and construct the group by if needed
        keys_sql = sql.SQL(', ').join(SampleTable.format_keys(keys))

        final_query = t"""
            SELECT {keys_sql:q}
            FROM sample s
            {sample_eid_join:q}
            INNER JOIN (
                {query_template:q}
            ) as inner_query ON inner_query.id = s.id
            GROUP BY s.id
        """

        return final_query

    # region GETS

    async def get_project_ids_for_sample_ids(self, sample_ids: list[int]) -> set[int]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        if not sample_ids:
            return set()
        _query = t'SELECT DISTINCT project FROM sample WHERE id = ANY({sample_ids})'

        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()

        return set(r['project'] for r in rows)

    async def query(
        self, filter_: SampleFilter
    ) -> tuple[set[ProjectId], list[SampleInternal]]:
        """Query samples"""
        keys = [
            's.id',
            sql.SQL('json_object_agg(sexid.name, sexid.external_id) AS external_ids'),
            's.participant_id',
            's.meta',
            's.active',
            's.type',
            's.project',
            's.sample_root_id',
            's.sample_parent_id',
        ]

        _query = self.construct_query(filter_, keys, sample_eid_table_alias='sexid')

        async with self.connection.pg_connection.cursor(
            row_factory=class_row(SampleInternal)
        ) as cur:
            await cur.execute(_query)
            samples = await cur.fetchall()

        projects = set(s.project for s in samples)
        return projects, samples

    async def get_sample_by_id(
        self, internal_id: int
    ) -> tuple[ProjectId, SampleInternal]:
        """Get a Sample by its internal_id"""
        projects, samples = await self.query(
            SampleFilter(id=GenericFilter(eq=internal_id))
        )
        if not samples:
            raise NotFoundError(
                f"Couldn't find sample with internal id {internal_id} (CPG id: {sample_id_format(internal_id)})"
            )

        return projects.pop(), samples.pop()

    async def get_single_by_external_id(
        self, external_id, project: ProjectId, check_active=True
    ) -> SampleInternal:
        """
        Get a single sample by (any of) its external_id(s)
        """
        _, samples = await self.query(
            SampleFilter(
                external_id=GenericFilter(eq=external_id),
                project=GenericFilter(eq=project),
                active=GenericFilter(eq=check_active),
            )
        )

        if not samples:
            raise NotFoundError(
                f"Couldn't find sample with external id {external_id} in project {project}"
            )

        return samples.pop()

    async def get_sample_id_to_project_map(
        self, project_ids: list[int], active_only: bool = True
    ) -> dict[int, ProjectId]:
        """
        Get active sample IDs given project IDs
        :return: {sample_id: project_id}
        """
        _query = t'SELECT id, project FROM sample WHERE project = ANY({project_ids})'
        _query += t' AND active IS TRUE' if active_only else t''

        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()
        return {row['id']: row['project'] for row in rows}

    async def export_sample_table(self, project: int):
        """Export the sample table, joined with external_ids"""
        mt = MetaTable(self.connection)
        mt_exid_query = mt.external_id_query('seid')
        query = t"""
            SELECT
                s.id,
                s.participant_id,
                s.active,
                s.type,
                s.sample_root_id,
                s.sample_parent_id,
                s.meta::text as meta,
                {mt_exid_query:q}
            FROM sample s
            LEFT JOIN sample_external_id seid
            ON seid.sample_id = s.id
            WHERE s.project = {project}
            GROUP BY s.id
        """

        return await mt.entity_meta_table(
            query=query,
            row_getter=lambda row: {
                'sample_id': sample_id_format(row['id']),
                'participant_id': row['participant_id'],
                'type': row['type'],
                'active': parse_sql_bool(row['active']),
                'sample_root_id': (
                    sample_id_format(row['sample_root_id'])
                    if row['sample_root_id']
                    else None
                ),
                'sample_parent_id': (
                    sample_id_format(row['sample_parent_id'])
                    if row['sample_parent_id']
                    else None
                ),
            },
            has_external_ids=True,
            has_meta=True,
        )

    # endregion GETS

    # region INSERTS

    async def insert_sample(
        self,
        external_ids: dict[str, str],
        sample_type: str,
        active: bool,
        meta: dict | None,
        participant_id: int | None,
        sample_parent_id: int | None,
        sample_root_id: int | None,
        project=None,
    ) -> int:
        """
        Create a new sample, and add it to database
        """
        if not external_ids or external_ids.get(PRIMARY_EXTERNAL_ORG) is None:
            raise ValueError('Sample must have primary external_id')

        audit_log_id = await self.audit_log_id()
        meta_value = Jsonb(meta or {})
        project_value = project or self.project_id

        _query = t"""
            INSERT INTO sample (participant_id, meta, type, active,
                audit_log_id, sample_parent_id, sample_root_id, project)
            VALUES ({participant_id}, {meta_value}, {sample_type}, {active},
                {audit_log_id}, {sample_parent_id}, {sample_root_id}, {project_value})
            RETURNING id;
        """

        async with self.connection.transaction():
            cur = await self.connection.pg_connection.execute(_query)
            row = await cur.fetchone()

            if row is None or 'id' not in row:
                raise DatabaseError('Failed to insert sample, no ID returned')

            id_of_new_sample = row['id']

            _eid_query = """
                INSERT INTO sample_external_id (project, sample_id, name,
                    external_id, audit_log_id)
                VALUES (%(project)s, %(id_of_new_sample)s, %(name)s,
                    %(external_id)s, %(audit_log_id)s)
            """
            _eid_values = [
                {
                    'name': name.lower(),
                    'external_id': eid,
                    'audit_log_id': audit_log_id,
                    'project': project_value,
                    'id_of_new_sample': id_of_new_sample,
                }
                for name, eid in external_ids.items()
                if eid is not None
            ]

            async with self.connection.pg_connection.cursor() as cur:
                await cur.executemany(_eid_query, _eid_values)

            return id_of_new_sample

    async def update_sample(
        self,
        id_: int,
        meta: dict | None,
        participant_id: int | None,
        external_ids: dict[str, str | None] | None,
        type_: str | None,
        active: bool | None = None,
        sample_parent_id: int | None = None,
        sample_root_id: int | None = None,
    ):
        """Update a single sample"""

        audit_log_id = await self.audit_log_id()
        fields = [t'audit_log_id = {audit_log_id}']

        # Participant ID
        fields.append(t'participant_id = {participant_id}') if participant_id else None

        # External IDs
        if external_ids:
            to_delete = [k.lower() for k, v in external_ids.items() if v is None]
            to_update = {k.lower(): v for k, v in external_ids.items() if v is not None}

            if PRIMARY_EXTERNAL_ORG in to_delete:
                raise ValueError("Can't remove sample's primary external_id")

            if to_delete:
                # Set audit_log_id to this transaction before deleting the rows
                _audit_update_query = t"""
                    UPDATE sample_external_id
                    SET audit_log_id = {audit_log_id}
                    WHERE sample_id = {id_} AND name = ANY({to_delete})
                """
                await self.connection.pg_connection.execute(_audit_update_query)

                _delete_query = t"""
                    DELETE FROM sample_external_id
                    WHERE sample_id = {id_} AND name = ANY({to_delete})
                """
                await self.connection.pg_connection.execute(_delete_query)

            if to_update:
                _query = t'SELECT project FROM sample WHERE id = {id_}'

                cur = await self.connection.pg_connection.execute(_query)
                row = await cur.fetchone()

                if row is None or 'project' not in row:
                    raise NotFoundError(
                        f"Couldn't find sample with id {id_} to update external ids"
                    )

                project = row['project']

                # Use MERGE to handle both the primary key (sample_id, name) and
                # the unique index (project, external_id) conflicts.
                # Mimics MariaDB ON DUPLICATE KEY UPDATE behavior.
                _update_query = """
                    MERGE INTO sample_external_id AS target
                    USING (SELECT %(project)s AS project, %(id)s AS sample_id, %(name)s AS name,
                                  %(external_id)s AS external_id, %(audit_log_id)s AS audit_log_id) AS source
                    ON (target.sample_id = source.sample_id AND target.name = source.name)
                       OR (target.project = source.project AND target.external_id = source.external_id)
                    WHEN MATCHED THEN
                        UPDATE SET external_id = source.external_id,
                                   audit_log_id = source.audit_log_id
                    WHEN NOT MATCHED THEN
                        INSERT (project, sample_id, name, external_id, audit_log_id)
                        VALUES (source.project, source.sample_id, source.name, source.external_id, source.audit_log_id)
                """
                _eid_values = [
                    {
                        'name': name,
                        'external_id': eid,
                        'project': project,
                        'id': id_,
                        'audit_log_id': audit_log_id,
                    }
                    for name, eid in to_update.items()
                ]

                async with self.connection.pg_connection.cursor() as cur:
                    await cur.executemany(_update_query, _eid_values)

        # Type, Active, Sample Parent ID, Sample Root ID
        meta_value = Jsonb(meta or {})
        fields.append(t'type = {type_}') if type_ else None
        fields.append(
            t"meta = json_merge_patch(COALESCE(meta, '{{}}'::jsonb), {meta_value})"
        ) if meta else None
        fields.append(t'active = {active}') if active is not None else None
        fields.append(
            t'sample_parent_id = {sample_parent_id}'
        ) if sample_parent_id is not None else None
        fields.append(
            t'sample_root_id = {sample_root_id}'
        ) if sample_root_id is not None else None

        # means you can't set to null
        fields_str = sql.SQL(', ').join(fields)
        _query = t'UPDATE sample SET {fields_str:q} WHERE id = {id_}'
        await self.connection.pg_connection.execute(_query)
        return id_

    async def merge_samples(
        self,
        id_keep: int,
        id_merge: int,
    ):
        """Merge two samples together"""
        sid_merge = sample_id_format(id_merge)
        (_, sample_keep), (_, sample_merge) = await asyncio.gather(
            self.get_sample_by_id(id_keep),
            self.get_sample_by_id(id_merge),
        )

        def list_merge(l1: Any, l2: Any) -> list:  # noqa: PLR0911
            if l1 is None:
                return l2
            if l2 is None:
                return l1
            if l1 == l2:
                return l1
            if isinstance(l1, list) and isinstance(l2, list):
                return l1 + l2
            if isinstance(l1, list):
                return l1 + [l2]
            if isinstance(l2, list):
                return [l1] + l2
            return [l1, l2]

        def dict_merge(meta1, meta2):
            d = dict(meta1)
            d.update(meta2)
            for key, value in meta2.items():
                if key not in meta1 or meta1[key] is None or value is None:
                    continue

                d[key] = list_merge(meta1[key], value)

            return d

        # this handles merging a sample that has already been merged
        meta_original = sample_keep.meta
        meta_original['merged_from'] = list_merge(
            meta_original.get('merged_from'), sid_merge
        )
        meta: dict[str, Any] = dict_merge(meta_original, sample_merge.meta)
        audit_log_id = await self.audit_log_id()

        # Get the appropriate queries in order
        queries = []

        # Query to update the kept sample with the merged meta and audit log id
        meta_value = Jsonb(meta)
        queries.append(t"""
            UPDATE sample
            SET audit_log_id = {audit_log_id},
                meta = {meta_value}
            WHERE id = {id_keep}
        """)

        # Query to update the assay table replacing the merge sid with
        # the id to keep
        queries.append(t"""
            UPDATE assay
            SET sample_id = {id_keep}, audit_log_id = {audit_log_id}
            WHERE sample_id = {id_merge}
        """)

        # Query to update the sequencing group table replacing the merge sid with
        # the id to keep
        queries.append(t"""
            UPDATE analysis_sequencing_group
            SET sample_id = {id_keep}, audit_log_id = {audit_log_id}
            WHERE sample_id = {id_merge}
        """)

        # Query to update the sample table audit log id for the merged sample
        queries.append(t"""
            UPDATE sample
            SET audit_log_id = {audit_log_id}
            WHERE id = {id_merge}
        """)

        # Query to delete the merged sample
        queries.append(t"""
            DELETE FROM sample
            WHERE id = {id_merge}
        """)

        # Execute the queries in a transaction in the order above
        async with self.connection.pg_connection.transaction():
            for query in queries:
                await self.connection.pg_connection.execute(query)

        project, new_sample = await self.get_sample_by_id(id_keep)
        new_sample.project = project

        return new_sample

    async def update_many_participant_ids(
        self, ids: list[int], participant_ids: list[int]
    ):
        """
        Update participant IDs for many samples
        Expected len(ids) == len(participant_ids)
        """

        audit_log_id = await self.audit_log_id()
        _query = """
            UPDATE sample
            SET participant_id = %(participant_id)s, audit_log_id = %(audit_log_id)s
            WHERE id = %(id)s
        """

        values = [
            {'id': i, 'participant_id': pid, 'audit_log_id': audit_log_id}
            for i, pid in zip(ids, participant_ids, strict=False)
        ]

        async with self.connection.pg_connection.cursor() as cur:
            await cur.executemany(_query, values)

    # region SEARCH

    async def search(
        self, query, project_ids: list[ProjectId], limit=5
    ) -> list[tuple[ProjectId, int, int, str]]:
        """
        Search by some term, return [ProjectId, SampleInternalId, ParticipantId, ExternalId]
        """

        search_pattern = escape_like_term(query) + '%'
        _query = t"""
            SELECT s.project, s.id, seid.external_id, s.participant_id
            FROM sample s
            INNER JOIN sample_external_id seid ON s.id = seid.sample_id
            WHERE s.project = ANY({project_ids}) AND seid.external_id ILIKE {search_pattern}
            LIMIT {limit}
        """

        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()

        return [
            (r['project'], r['id'], r['participant_id'], r['external_id']) for r in rows
        ]

    # endregion SEARCH

    # region ID MAPS

    async def get_sample_id_map_by_external_ids(
        self,
        external_ids: list[str],
        project: ProjectId,
    ) -> dict[str, int]:
        """Get map of external sample id to internal id"""
        if not project:
            raise ValueError('Must specify project when getting by external ids')

        project_id = project or self.project_id

        if project_id is None:
            raise ValueError(
                'Project must be provided to get sample id map by external ids'
            )

        _query = t"""
            SELECT sample_id AS id, external_id
            FROM sample_external_id
            WHERE external_id = ANY({external_ids}) AND project = {project_id}
        """

        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()
        sample_id_map = {el['external_id']: el['id'] for el in rows}

        return sample_id_map

    async def get_sample_id_map_by_internal_ids(
        self, raw_internal_ids: list[int]
    ) -> tuple[Iterable[ProjectId], dict[int, str]]:
        """Get map of (primary) external sample id by internal id"""

        _query = t"""
            SELECT sample_id AS id, external_id, project
            FROM sample_external_id
            WHERE sample_id = ANY({raw_internal_ids}) AND name = {PRIMARY_EXTERNAL_ORG}
        """
        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()

        sample_id_map = {el['id']: el['external_id'] for el in rows}
        projects = set(el['project'] for el in rows)

        return projects, sample_id_map

    async def get_all_sample_id_map_by_internal_ids(
        self, project: ProjectId
    ) -> dict[int, str]:
        """Get sample id map for all samples"""
        project_id = project or self.project_id

        _query = t"""
            SELECT s.id as id, seid.external_id as external_id
            FROM sample s
            INNER JOIN sample_external_id seid ON s.id = seid.sample_id
            WHERE s.project = {project_id} AND name = {PRIMARY_EXTERNAL_ORG}
        """

        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()

        return {el['id']: el['external_id'] for el in rows}

    # endregion ID MAPS

    # region HISTORY

    async def get_samples_create_date(self, sample_ids: list[int]) -> dict[int, date]:
        """Get a map of {internal_sample_id: date_created} for list of sample_ids"""
        if len(sample_ids) == 0:
            return {}
        _query = t"""
            SELECT id, MIN(lower(sys_period)) as date_created
            FROM (
                SELECT id, sys_period FROM sample
                UNION ALL
                SELECT id, sys_period FROM sample_history
            ) AS sample_hist
            WHERE id = ANY({sample_ids})
            GROUP BY id
        """
        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()
        return {r['id']: r['date_created'].date() for r in rows}

    async def get_history_of_sample(self, id_: int):
        """Get all versions (history) of a sample"""
        # TODO Re-implement this for the separate sample_external_id table. Doing a join and/or aggregating
        # the external_ids wreaks havoc with FOR SYSTEM_TIME ALL queries, collapsing the history we want to
        # see into one aggregate record. For now, leave the query as is, with external_ids unavailable.
        keys = [
            'id',
            sql.SQL("'{}'::jsonb AS external_ids"),
            'participant_id',
            'meta',
            'active',
            'type',
            'project',
            'author',
            'sample_root_id',
            'sample_parent_id',
            # 'audit_log_id',  Not in SampleInternal model
            # 'sys_period',  Not in SampleInternal model
        ]

        keys_str = sql.SQL(', ').join(SampleTable.format_keys(keys))
        _query = t"""
            SELECT {keys_str:q}
            FROM (
                SELECT *
                FROM sample
                WHERE id = {id_}

                UNION ALL

                SELECT *
                FROM history.sample_history
                WHERE id = {id_}
            )
            ORDER BY lower(sys_period)
        """

        async with self.connection.pg_connection.cursor(
            row_factory=class_row(SampleInternal)
        ) as cur:
            await cur.execute(_query)
            samples = await cur.fetchall()

        return samples

    # endregion HISTORY

    async def get_samples_with_missing_participants_by_internal_id(
        self, project: ProjectId
    ) -> list[SampleInternal]:
        """Get samples with missing participants"""
        _, samples = await self.query(
            SampleFilter(
                participant_id=GenericFilter(isnull=True),
                project=GenericFilter(eq=project or self.project_id),
            )
        )
        return samples

    async def get_monthly_samples_count_per_project(
        self, project_ids: list[int] | None = None
    ) -> dict[int, dict[date, int]]:
        """
        Get a map of {project_id: {month:count} for list of project_ids
        If project_ids is empty, return all projects
        """

        where_str = t'WHERE project = ANY({project_ids})' if project_ids else t''

        _query = t"""
        WITH t AS(
            SELECT project, id, MIN(lower(s.sys_period)) as sample_first_date
            FROM sample
            INNER JOIN (
                SELECT id, sys_period FROM sample
                UNION ALL
                SELECT id, sys_period FROM sample_history
            ) sample_hist ON sample.id = sample_hist.id
            {where_str:q}
            GROUP BY project,id
        )
        SELECT project,
        CAST(EXTRACT(YEAR FROM sample_first_date) AS INTEGER) AS year,
        CAST(EXTRACT(MONTH FROM sample_first_date) AS INTEGER) AS month,
        COUNT(*) AS count
        FROM t
        GROUP BY project, year, month
        ORDER BY project, year, month
        """

        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()
        result: dict[int, dict[date, int]] = defaultdict(lambda: defaultdict(int))
        accumulated_sample_count: dict[int, int] = {}

        # results are sorted by project, year, month
        # accumulate projects previous months into the processed month
        for r in rows:
            proj = r['project']
            # append previous months count to currently processed month
            accumulated_sample_count[proj] = (
                accumulated_sample_count.get(proj, 0) + r['count']
            )
            result[proj][date.fromisoformat(f'{r["year"]}-{r["month"]:02d}-01')] = (
                accumulated_sample_count[proj]
            )

        # append all the months up to the current month
        this_month = date.today().replace(day=1)
        for proj, month_counts in result.items():  # noqa: B007
            # in case there is no previous months available, then skip
            if not month_counts:
                continue

            # Fill in missing months between recorded dates.
            current_month = min(month_counts.keys())
            current_count = month_counts[current_month]
            while current_month <= this_month:
                if current_month in month_counts:
                    current_count = month_counts[current_month]

                month_counts[current_month] = current_count
                current_month = (current_month + relativedelta(months=1)).replace(day=1)

        return result

    async def _get_sample_count_per_project_by_seq_group(
        self, sequencing_group_ids: list[int]
    ) -> dict[int, int]:
        """Get the count of samples per project based on provided seq groups"""
        # if no sequencing groups, return empty dict
        if not sequencing_group_ids:
            return {}

        _query = t"""
            SELECT s.project, COUNT(DISTINCT s.id) AS sample_count
            FROM sequencing_group sg
            INNER JOIN sample s ON s.id = sg.sample_id
            WHERE sg.id = ANY({sequencing_group_ids})
            GROUP BY s.project
        """
        cur = await self.connection.pg_connection.execute(_query)
        rows = await cur.fetchall()
        return {r['project']: r['sample_count'] for r in rows}

    async def get_sample_count_per_project_per_month(
        self, sg_compute_per_month: dict[date, list[int]] | None
    ) -> dict[int, dict[date, int]]:
        """
        Match passed month->list of sequencing groups to sample counts per project
        used in compute for those sequencing groups
        Return a map of {metamist project_id: {month: count of samples used in compute}}
        """
        # Map to hold the results
        result: dict[int, dict[date, int]] = defaultdict(lambda: defaultdict(int))

        if not sg_compute_per_month:
            return result

        for month, seq_groups in sg_compute_per_month.items():
            project_sample_count = (
                await self._get_sample_count_per_project_by_seq_group(seq_groups)
            )
            for project_id, sample_count in project_sample_count.items():
                result[project_id][month] += sample_count

        return result
