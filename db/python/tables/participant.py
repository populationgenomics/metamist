from collections import defaultdict
from string.templatelib import Template

from psycopg import sql
from psycopg.types.json import Jsonb

from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.filters.participant import ParticipantFilter
from db.python.tables.meta_table import MetaTable
from db.python.utils import NotFoundError, escape_like_term
from models.models import PRIMARY_EXTERNAL_ORG, ParticipantInternal, ProjectId


class ParticipantTable:
    """
    Capture Analysis table operations and queries
    """

    def __init__(self, connection: Connection):
        self.connection = connection

    keys = [
        'p.id',
        sql.SQL('jsonb_object_agg(peid.name, peid.external_id) AS external_ids'),
        'p.reported_sex',
        'p.reported_gender',
        'p.karyotype',
        'p.meta',
        'p.project',
        'p.audit_log_id',
    ]

    table_name = 'participant'

    async def get_project_ids_for_participant_ids(self, participant_ids: list[int]):
        """Get project IDs for participant_ids (mostly for checking auth)"""
        _query = t"""
            SELECT project
            FROM participant
            WHERE id = ANY({participant_ids})
            GROUP BY project
        """

        conn = self.connection.pg_connection
        cur = await conn.execute(_query)
        rows = await cur.fetchall()

        return set(r['project'] for r in rows)

    @staticmethod
    async def _construct_participant_query(  # noqa: C901, PLR0912, RUF100
        filter_: ParticipantFilter,
        keys: list[str | sql.Composable],
        skip: int | None = None,
        limit: int | None = None,
        participant_eid_table_alias: str | None = None,
        group_result_by_id: bool = True,
    ) -> Template:
        """Construct a participant query"""
        if not keys:
            raise ValueError('Must provide keys to construct participant query')

        # Collection of where conditions
        wheres = []

        # Query constructer
        query_template = t"""
            SELECT DISTINCT pp.id
            FROM participant pp
        """

        # Join on the participant_external_id table
        # always join, query optimiser can figure it out
        # Start by getting the project name, id, meta and external_id
        wheres.append(
            filter_.to_sql(
                {
                    'project': 'pp.project',
                    'id': 'pp.id',
                    'meta': 'pp.meta',
                    'external_id': 'peid.external_id',
                },
                exclude=['family', 'sample', 'sequencing_group', 'assay'],
            )
        )

        query_template += (
            t' INNER JOIN participant_external_id peid ON pp.id = peid.participant_id'
        )

        # Check filter for the sample table and sample_external_id table
        if filter_.sample or filter_.sequencing_group or filter_.assay:
            if filter_.sample:
                wheres.append(
                    filter_.sample.to_sql(
                        {
                            'id': 's.id',
                            'type': 's.type',
                            'meta': 's.meta',
                            'sample_root_id': 's.sample_root_id',
                            'sample_parent_id': 's.sample_parent_id',
                        },
                        exclude=['external_id'],
                    )
                )

            query_template += t' INNER JOIN sample s ON s.participant_id = pp.id'

            if filter_.sample and filter_.sample.external_id:
                wheres.append(
                    filter_.sample.to_sql(
                        {'external_id': 'seid.external_id'}, only=['external_id']
                    )
                )

                query_template += (
                    t' INNER JOIN sample_external_id seid ON seid.sample_id = s.id'
                )

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

            query_template += t' INNER JOIN sequencing_group sg ON sg.sample_id = s.id'

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

            query_template += t' INNER JOIN assay a ON a.sample_id = s.id'

        if filter_.family:
            wheres.append(
                filter_.family.to_sql(
                    {
                        'id': 'f.id',
                        'meta': 'f.meta',
                    },
                    exclude=['external_id'],
                )
            )

            query_template += t"""
                INNER JOIN family_participant fp ON fp.participant_id = pp.id
                INNER JOIN family f ON f.id = fp.family_id
            """

            if filter_.family.external_id:
                wheres.append(
                    filter_.family.to_sql(
                        {'external_id': 'feid.external_id'}, only=['external_id']
                    )
                )

                query_template += t"""
                    INNER JOIN family_external_id feid ON feid.family_id = f.id
                """

        # WHERE, ORDER BY, LIMIT, OFFSET
        wheres_filtered = [w for w in wheres if w is not None]
        query_template += (
            t' WHERE {sql.SQL(" AND ").join(wheres_filtered):q}'
            if wheres_filtered
            else t''
        )
        query_template += t' ORDER BY pp.id' if (limit or skip) else t''
        query_template += t' LIMIT {limit}' if limit else t''
        query_template += t' OFFSET {skip}' if skip else t''

        # External table join in order to ge the participant id aliases
        ex_table_join = (
            t"""
            LEFT JOIN participant_external_id {participant_eid_table_alias:i}
                ON p.id = {participant_eid_table_alias:i}.participant_id
            """
            if participant_eid_table_alias
            else t''
        )

        def format_key(key: str | sql.Composable) -> sql.Composable:
            if isinstance(key, str):
                return sql.Identifier(*key.split('.'))
            return key

        # Turn the keys into sql and construct the group by if needed
        formatted_keys = [format_key(k) for k in keys]
        keys_sql = sql.SQL(', ').join(formatted_keys)
        optional_group_by = sql.SQL('GROUP BY p.id') if group_result_by_id else t''

        # Final query template
        outer_query = t"""
            SELECT {keys_sql:q}
            FROM participant p
            {ex_table_join:q}
            INNER JOIN ({query_template:q}) as inner_query ON inner_query.id = p.id
            {optional_group_by:q}
        """

        return outer_query

    async def query(  # noqa: D417
        self,
        filter_: ParticipantFilter,
        limit: int | None = None,
        skip: int | None = None,
    ) -> tuple[set[ProjectId], list[ParticipantInternal]]:
        """
        Query for participants

        Args:
            filter_ (ParticipantFilter): _description_

        Returns:
            list[ParticipantInternal]: _description_
        """
        keys = [
            'p.id',
            sql.SQL('jsonb_object_agg(peid.name, peid.external_id) AS external_ids'),
            'p.reported_sex',
            'p.reported_gender',
            'p.karyotype',
            'p.meta',
            'p.project',
        ]
        query = await self._construct_participant_query(
            filter_,
            keys=keys,
            skip=skip,
            limit=limit,
            participant_eid_table_alias='peid',
        )

        conn = self.connection.pg_connection
        cur = await conn.execute(query)
        rows = await cur.fetchall()

        projects = set(r['project'] for r in rows)
        return projects, [ParticipantInternal.from_db(dict(r)) for r in rows]

    async def query_count(self, filter_: ParticipantFilter) -> int:
        """Query for participants count"""
        query = await self._construct_participant_query(
            filter_, keys=[sql.SQL('COUNT(*) AS cnt')], group_result_by_id=False
        )

        conn = self.connection.pg_connection
        cur = await conn.execute(query)
        row = await cur.fetchone()

        if not row:
            return 0
        return row['cnt']

    async def get_participants_by_ids(
        self, ids: list[int]
    ) -> tuple[set[ProjectId], list[ParticipantInternal]]:
        """Get participants by IDs"""
        return await self.query(ParticipantFilter(id=GenericFilter(in_=ids)))

    async def get_participants(
        self, project: int, internal_participant_ids: list[int] | None = None
    ) -> list[ParticipantInternal]:
        """
        Get participants for a project
        """
        _, particicpants = await self.query(
            ParticipantFilter(
                project=GenericFilter(in_=[project]),
                id=(
                    GenericFilter(in_=internal_participant_ids)
                    if internal_participant_ids
                    else None
                ),
            )
        )
        return particicpants

    async def export_participant_table(self, project: int):
        """Export a parquet table of participants, including external_ids and meta"""
        mt = MetaTable(self.connection)
        query = t"""
            SELECT
                p.id,
                p.reported_sex,
                p.reported_gender,
                p.karyotype,
                p.meta::text as meta,
                {mt.external_id_query('peid'):q}
            FROM participant p
            LEFT JOIN participant_external_id peid
            ON peid.participant_id = p.id
            WHERE p.project = {project}
            GROUP BY p.id
        """

        return await mt.entity_meta_table(
            query=query,
            row_getter=lambda row: {
                'participant_id': row['id'],
                'reported_sex': row['reported_sex'],
                'reported_gender': row['reported_gender'],
                'karyotype': row['karyotype'],
            },
            has_external_ids=True,
            has_meta=True,
        )

    async def create_participant(
        self,
        external_ids: dict[str, str | None],
        reported_sex: int | None,
        reported_gender: str | None,
        karyotype: str | None,
        meta: dict | None,
        project: ProjectId | None = None,
    ) -> int:
        """
        Create a new sample, and add it to database
        """
        if not external_ids or external_ids.get(PRIMARY_EXTERNAL_ORG) is None:
            raise ValueError('Participant must have primary external_id')

        audit_log_id = await self.connection.audit_log_id()

        meta_value = Jsonb(meta or {})
        project_value = project or self.connection.project_id

        if not project_value:
            raise ValueError('Project must be specified to create participant')

        async with self.connection.transaction():
            conn = self.connection.pg_connection

            _query = t"""
                INSERT INTO participant
                    (reported_sex, reported_gender, karyotype, meta, audit_log_id, project)
                VALUES
                    ({reported_sex}, {reported_gender}, {karyotype}, {meta_value}, {audit_log_id}, {project_value})
                RETURNING id
            """

            cur = await conn.execute(_query)
            row = await cur.fetchone()
            if not row:
                raise ValueError('Failed to create participant')
            new_id = row['id']

            _eid_query = """
                INSERT INTO participant_external_id
                    (project, participant_id, name, external_id, audit_log_id)
                VALUES
                    (%(project)s, %(participant_id)s, %(name)s, %(external_id)s, %(audit_log_id)s)
                RETURNING participant_id
            """

            eid_values = [
                {
                    'project': project_value,
                    'participant_id': new_id,
                    'name': name.lower(),
                    'external_id': external_id,
                    'audit_log_id': audit_log_id,
                }
                for name, external_id in external_ids.items()
                if external_id is not None
            ]

            async with self.connection.pg_connection.cursor() as cur:
                await cur.executemany(_eid_query, eid_values)

            return new_id

        return None

    async def update_participant(
        self,
        participant_id: int,
        external_ids: dict[str, str | None] | None,
        reported_sex: int | None,
        reported_gender: str | None,
        karyotype: str | None,
        meta: dict | None,
    ):
        """
        Update participant
        """
        audit_log_id = await self.connection.audit_log_id()
        conn = self.connection.pg_connection

        async with self.connection.transaction():
            if external_ids:
                to_delete = [k.lower() for k, v in external_ids.items() if v is None]
                any_to_update = any(v is not None for v in external_ids.values())

                if PRIMARY_EXTERNAL_ORG in to_delete:
                    raise ValueError("Can't remove participant's primary external_id")

                if to_delete:
                    # Set audit_log_id to this transaction before deleting the rows
                    _audit_update_query = t"""
                        UPDATE participant_external_id
                        SET audit_log_id = {audit_log_id}
                        WHERE participant_id = {participant_id} AND LOWER(name) = ANY({to_delete})
                    """
                    await conn.execute(_audit_update_query)

                    _delete_query = t"""
                        DELETE FROM participant_external_id
                        WHERE participant_id = {participant_id} AND LOWER(name) = ANY({to_delete})
                    """
                    await conn.execute(_delete_query)

                if any_to_update:
                    _query = (
                        t'SELECT project FROM participant WHERE id = {participant_id}'
                    )
                    cur = await conn.execute(_query)
                    row = await cur.fetchone()
                    project = row['project']

                    to_update = [
                        {
                            'name': k.lower(),
                            'external_id': v,
                            'audit_log_id': audit_log_id,
                            'project': project,
                            'participant_id': participant_id,
                        }
                        for k, v in external_ids.items()
                        if v is not None
                    ]

                    # Batch update
                    # Use MERGE to handle both the primary key (participant_id, name) and
                    # the unique index (project, external_id) conflicts.
                    # Mimics MariaDB ON DUPLICATE KEY UPDATE behavior.
                    async with conn.cursor() as cur:
                        await cur.executemany(
                            """
                                MERGE INTO participant_external_id AS target
                                USING (VALUES (%(project)s, %(participant_id)s, %(name)s, %(external_id)s, %(audit_log_id)s))
                                    AS source (project, participant_id, name, external_id, audit_log_id)
                                ON (target.participant_id = source.participant_id AND LOWER(target.name) = LOWER(source.name))
                                   OR (target.project = source.project AND target.external_id = source.external_id)
                                WHEN MATCHED THEN
                                    UPDATE SET external_id = source.external_id, audit_log_id = source.audit_log_id
                                WHEN NOT MATCHED THEN
                                    INSERT (project, participant_id, name, external_id, audit_log_id)
                                    VALUES (source.project, source.participant_id, source.name, source.external_id, source.audit_log_id)
                            """,
                            to_update,
                        )

            updates: list[Template | None] = [t'audit_log_id = {audit_log_id}']

            updates.append(t'reported_sex = {reported_sex}' if reported_sex else None)
            updates.append(
                t'reported_gender = {reported_gender}' if reported_gender else None
            )
            updates.append(t'karyotype = {karyotype}' if karyotype else None)
            meta_value = Jsonb(meta or {})
            updates.append(
                t"meta = JSON_MERGE_PATCH(COALESCE(meta, '{{}}'), {meta_value})"
                if meta
                else None
            )

            updates = [u for u in updates if u is not None]
            updates_str = sql.SQL(', ').join(updates)

            _query = (
                t'UPDATE participant SET {updates_str:q} WHERE id = {participant_id}'
            )

            await conn.execute(_query)
            return True

        return False

    async def get_id_map_by_external_ids(
        self,
        external_participant_ids: list[str],
        project: ProjectId | None,
    ) -> dict[str, int]:
        """Get map of {external_id: internal_participant_id}"""
        _project = project or self.connection.project_id
        if not _project:
            raise ValueError(
                'Must provide project to get participant id map by external'
            )

        if len(external_participant_ids) == 0:
            return {}

        eids_case_insensitive = [eid.lower() for eid in external_participant_ids]

        _query = t"""
            SELECT external_id, participant_id AS id
            FROM participant_external_id
            WHERE LOWER(external_id) = ANY({eids_case_insensitive}) AND project = {project}
        """

        conn = self.connection.pg_connection
        cur = await conn.execute(_query)
        results = await cur.fetchall()
        id_map = {r['external_id']: r['id'] for r in results}

        return id_map

    async def get_id_map_by_internal_ids(
        self, internal_participant_ids: list[int], allow_missing=False
    ) -> dict[int, str]:
        """Get map of {internal_id: primary_external_participant_id}"""
        if len(internal_participant_ids) == 0:
            return {}

        _query = t"""
            SELECT participant_id AS id, external_id
            FROM participant_external_id
            WHERE participant_id = ANY({internal_participant_ids}) AND name = {PRIMARY_EXTERNAL_ORG}
        """
        conn = self.connection.pg_connection
        cur = await conn.execute(_query)
        results = await cur.fetchall()

        id_map: dict[int, str] = {r['id']: r['external_id'] for r in results}

        if not allow_missing and len(id_map) != len(internal_participant_ids):
            provided_internal_ids = set(internal_participant_ids)
            # do the check again, but use the set this time
            # (in case we're provided a list with duplicates)
            if len(id_map) != len(provided_internal_ids):
                # we have families missing from the map, so we'll 404 the whole thing
                missing_participant_ids = provided_internal_ids - set(id_map.keys())

                raise NotFoundError(
                    f"Couldn't find participants with internal IDS: {', '.join(str(s) for s in missing_participant_ids)}"
                )

        return id_map

    async def get_participants_by_families(
        self, family_ids: list[int]
    ) -> tuple[set[ProjectId], dict[int, list[ParticipantInternal]]]:
        """Get list of participants keyed by families, duplicates results"""

        _query = t"""
            SELECT fp.family_id, p.id, jsonb_object_agg(peid.name, peid.external_id) as external_ids,
            p.reported_sex, p.reported_gender, p.karyotype, p.meta, p.project, p.audit_log_id
            FROM participant p
            INNER JOIN family_participant fp ON fp.participant_id = p.id
            INNER JOIN participant_external_id peid ON p.id = peid.participant_id
            WHERE fp.family_id = ANY({family_ids})
            GROUP BY p.id, fp.family_id
        """
        conn = self.connection.pg_connection
        cur = await conn.execute(_query)
        rows = await cur.fetchall()

        retmap = defaultdict(list)
        projects: set[ProjectId] = set()
        for row in rows:
            drow = dict(row)
            projects.add(row['project'])
            fid = drow.pop('family_id')
            retmap[fid].append(ParticipantInternal.from_db(drow))

        return projects, retmap

    async def update_many_participant_external_ids(
        self, internal_to_external_id: dict[int, str]
    ):
        """Update many participant primary external_ids through the {internal: external} map"""
        audit_log_id = await self.connection.audit_log_id()

        _query = """
            UPDATE participant_external_id
            SET external_id = %(external_id)s, audit_log_id = %(audit_log_id)s
            WHERE participant_id = %(participant_id)s
            AND LOWER(name) = %(name)s
        """

        updates = [
            {
                'external_id': external_id,
                'participant_id': participant_id,
                'name': PRIMARY_EXTERNAL_ORG.lower(),
                'audit_log_id': audit_log_id,
            }
            for participant_id, external_id in internal_to_external_id.items()
        ]

        conn = self.connection.pg_connection
        async with conn.cursor() as cur:
            await cur.executemany(_query, updates)

        return True

    async def get_external_ids_by_participant(
        self, participant_ids: list[int]
    ) -> dict[int, list[str]]:
        """
        Get lists of external IDs per participant
        """
        if not participant_ids:
            return {}

        _query = t"""
            SELECT participant_id AS id, array_agg(external_id) AS external_ids_list
            FROM participant_external_id
            WHERE participant_id = ANY({participant_ids})
            GROUP BY participant_id
        """

        conn = self.connection.pg_connection
        cur = await conn.execute(_query)
        rows = await cur.fetchall()
        return {r['id']: r['external_ids_list'] for r in rows}

    async def get_external_participant_id_to_internal_sequencing_group_id_map(
        self, project: ProjectId, sequencing_type: str | None = None
    ) -> list[tuple[str, int]]:
        """
        Get a map of {external_participant_id} -> {internal_sequencing_group_id}
        useful to match joint-called sequencing groups in the matrix table to the participant

        Return a list not dictionary, because dict could lose
        participants with multiple sequencing groups.
        """
        wheres = [t'p.project = {project}']
        if sequencing_type:
            wheres.append(t'sg.type = {sequencing_type.lower()}')

        where_str = sql.SQL(' AND ').join(wheres) if wheres else t''

        _query = t"""
            SELECT peid.external_id as eid, sg.id as sgid
            FROM participant p
            INNER JOIN participant_external_id peid ON p.id = peid.participant_id
            INNER JOIN sample s ON p.id = s.participant_id
            INNER JOIN sequencing_group sg ON sg.sample_id = s.id
            WHERE {where_str:q}
        """

        conn = self.connection.pg_connection
        cur = await conn.execute(_query)
        rows = await cur.fetchall()

        return [(r['eid'], int(r['sgid'])) for r in rows]

    async def search(
        self, query, project_ids: list[ProjectId], limit: int = 5
    ) -> list[tuple[ProjectId, int, str]]:
        """
        Search by some term, return [ProjectId, ParticipantId, ExternalId]
        """

        search_literal = escape_like_term(query) + '%'
        _query = t"""
            SELECT project, participant_id AS id, external_id
            FROM participant_external_id
            WHERE project = ANY({project_ids}) AND external_id ILIKE {search_literal}
            LIMIT {limit}
        """
        conn = self.connection.pg_connection
        cur = await conn.execute(_query)
        rows = await cur.fetchall()

        return [(r['project'], r['id'], r['external_id']) for r in rows]
