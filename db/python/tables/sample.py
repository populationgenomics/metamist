import asyncio
from datetime import date
from typing import Any, Iterable

from db.python.filters import GenericFilter
from db.python.filters.sample import SampleFilter
from db.python.tables.base import DbBase
from db.python.utils import NotFoundError, escape_like_term, to_db_json
from models.models import PRIMARY_EXTERNAL_ORG, ProjectId
from models.models.sample import SampleInternal, sample_id_format


class SampleTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sample'

    @staticmethod
    def construct_query(
        filter_: SampleFilter,
        keys: list[str],
        sample_eid_table_alias: str | None = None,
        skip: int | None = None,
        limit: int | None = None,
    ):
        """
        Construct a nested sample query
        """
        needs_eid = False
        needs_sequencing_group = False
        needs_assay = False

        _wheres, values = filter_.to_sql(
            {
                'project': 'ss.project',
                'id': 'ss.id',
                'meta': 'ss.meta',
                'sample_root_id': 'ss.sample_root_id',
                'sample_parent_id': 'ss.sample_parent_id',
            },
            exclude=['sequencing_group', 'assay', 'external_id'],
        )
        wheres = [_wheres]

        if filter_.external_id:
            needs_eid = True
            seid_wheres, seid_values = filter_.to_sql(
                {
                    'external_id': 'seid.external_id',
                },
                only=['external_id'],
            )
            if seid_wheres:
                wheres.append(seid_wheres)
            values.update(seid_values)

        if filter_.sequencing_group:
            needs_sequencing_group = True
            swheres, svalues = filter_.sequencing_group.to_sql(
                {
                    'id': 'sg.id',
                    'meta': 'sg.meta',
                    'type': 'sg.type',
                    'technology': 'sg.technology',
                    'platform': 'sg.platform',
                }
            )
            values.update(svalues)
            if swheres:
                wheres.append(swheres)

        if filter_.assay:
            needs_sequencing_group = True
            needs_assay = True

            awheres, avalues = filter_.assay.to_sql(
                {
                    'id': 'a.id',
                    'meta': 'a.meta',
                    'type': 'a.type',
                }
            )
            values.update(avalues)
            if awheres:
                wheres.append(awheres)

        query_lines = [
            'SELECT DISTINCT ss.id',
            'FROM sample ss',
        ]

        if needs_eid:
            # 2024-06-15 mfranklin: left join, inner join, doesn't matter as there
            #       should always be an external_id
            query_lines.append(
                'LEFT JOIN sample_external_id seid ON seid.sample_id = ss.id'
            )
        if needs_sequencing_group:
            query_lines.append('INNER JOIN sequencing_group sg ON sg.sample_id = ss.id')
        if needs_assay:
            query_lines.append('INNER JOIN assay a ON a.sample_id = ss.id')

        if wheres:
            query_lines.append('WHERE \n' + ' AND '.join(wheres))

        if limit or skip:
            query_lines.append('ORDER BY pp.id')

        if limit:
            query_lines.append('LIMIT :limit')
            values['limit'] = limit

        if skip:
            query_lines.append('OFFSET :offset')
            values['offset'] = skip

        query = '\n'.join('  ' + line.strip() for line in query_lines)
        sample_eid_join = ''
        if sample_eid_table_alias:
            sample_eid_join = f'INNER JOIN sample_external_id {sample_eid_table_alias} ON {sample_eid_table_alias}.sample_id = s.id'

        outer_query = f"""
            SELECT {', '.join(keys)}
            FROM sample s
            {sample_eid_join}
            INNER JOIN (
            {query}
            ) as inner_query ON inner_query.id = s.id
            GROUP BY s.id
        """

        return outer_query, values

    # region GETS

    async def get_project_ids_for_sample_ids(self, sample_ids: list[int]) -> set[int]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = 'SELECT DISTINCT project FROM sample WHERE id in :sample_ids'
        if not sample_ids:
            return set()

        rows = await self.connection.fetch_all(_query, {'sample_ids': sample_ids})
        return set(r['project'] for r in rows)

    async def query(
        self, filter_: SampleFilter
    ) -> tuple[set[ProjectId], list[SampleInternal]]:
        """Query samples"""
        keys = [
            's.id',
            'JSON_OBJECTAGG(sexid.name, sexid.external_id) AS external_ids',
            's.participant_id',
            's.meta',
            's.active',
            's.type',
            's.project',
            's.sample_root_id',
            's.sample_parent_id',
        ]
        _query, values = self.construct_query(
            filter_, keys, sample_eid_table_alias='sexid'
        )
        rows = await self.connection.fetch_all(_query, values)
        samples = [SampleInternal.from_db(dict(r)) for r in rows]
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
                f'Couldn\'t find sample with internal id {internal_id} (CPG id: {sample_id_format(internal_id)})'
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
                f'Couldn\'t find sample with external id {external_id} in project {project}'
            )

        return samples.pop()

    async def get_sample_id_to_project_map(
        self, project_ids: list[int], active_only: bool = True
    ) -> dict[int, ProjectId]:
        """
        Get active sample IDs given project IDs
        :return: {sample_id: project_id}
        """
        _query = 'SELECT id, project FROM sample WHERE project in :project_ids'

        if active_only:
            _query += ' AND active IS TRUE'

        rows = await self.connection.fetch_all(_query, {'project_ids': project_ids})
        return {row['id']: row['project'] for row in rows}

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
        if not external_ids or external_ids.get(PRIMARY_EXTERNAL_ORG, None) is None:
            raise ValueError('Sample must have primary external_id')

        audit_log_id = await self.audit_log_id()
        kv_pairs = [
            ('participant_id', participant_id),
            ('meta', to_db_json(meta or {})),
            ('type', sample_type),
            ('active', active),
            ('audit_log_id', audit_log_id),
            ('sample_parent_id', sample_parent_id),
            ('sample_root_id', sample_root_id),
            ('project', project or self.project),
        ]

        keys = [k for k, _ in kv_pairs]
        cs_keys = ', '.join(keys)
        cs_id_keys = ', '.join(f':{k}' for k in keys)
        _query = f"""\
        INSERT INTO sample
            ({cs_keys})
        VALUES ({cs_id_keys}) RETURNING id;
        """

        id_of_new_sample = await self.connection.fetch_val(
            _query,
            dict(kv_pairs),
        )

        _eid_query = """
        INSERT INTO sample_external_id (project, sample_id, name, external_id, audit_log_id)
        VALUES (:project, :id, :name, :external_id, :audit_log_id)
        """
        _eid_values = [
            {
                'project': project or self.project,
                'id': id_of_new_sample,
                'name': name.lower(),
                'external_id': eid,
                'audit_log_id': audit_log_id,
            }
            for name, eid in external_ids.items()
            if eid is not None
        ]
        await self.connection.execute_many(_eid_query, _eid_values)

        return id_of_new_sample

    async def update_sample(
        self,
        id_: int,
        meta: dict | None,
        participant_id: int | None,
        external_ids: dict[str, str | None] | None,
        type_: str | None,
        active: bool = None,
    ):
        """Update a single sample"""

        audit_log_id = await self.audit_log_id()
        values: dict[str, Any] = {'audit_log_id': audit_log_id}
        fields = [
            'audit_log_id = :audit_log_id',
        ]
        if participant_id:
            values['participant_id'] = participant_id
            fields.append('participant_id = :participant_id')

        if external_ids:
            to_delete = [k.lower() for k, v in external_ids.items() if v is None]
            to_update = {k.lower(): v for k, v in external_ids.items() if v is not None}

            if PRIMARY_EXTERNAL_ORG in to_delete:
                raise ValueError("Can't remove sample's primary external_id")

            if to_delete:
                # Set audit_log_id to this transaction before deleting the rows
                _audit_update_query = """
                UPDATE sample_external_id
                SET audit_log_id = :audit_log_id
                WHERE sample_id = :id AND name IN :names
                """
                await self.connection.execute(
                    _audit_update_query,
                    {'id': id_, 'names': to_delete, 'audit_log_id': audit_log_id},
                )

                _delete_query = """
                DELETE FROM sample_external_id
                WHERE sample_id = :id AND name IN :names
                """
                await self.connection.execute(
                    _delete_query, {'id': id_, 'names': to_delete}
                )

            if to_update:
                _query = 'SELECT project FROM sample WHERE id = :id'
                row = await self.connection.fetch_one(_query, {'id': id_})
                project = row['project']

                _update_query = """
                INSERT INTO sample_external_id (project, sample_id, name, external_id, audit_log_id)
                VALUES (:project, :id, :name, :external_id, :audit_log_id)
                ON DUPLICATE KEY UPDATE external_id = :external_id, audit_log_id = :audit_log_id
                """
                _eid_values = [
                    {
                        'project': project,
                        'id': id_,
                        'name': name,
                        'external_id': eid,
                        'audit_log_id': audit_log_id,
                    }
                    for name, eid in to_update.items()
                ]
                await self.connection.execute_many(_update_query, _eid_values)

        if type_:
            values['type'] = type_
            fields.append('type = :type')

        if meta is not None and len(meta) > 0:
            values['meta'] = to_db_json(meta)
            fields.append('meta = JSON_MERGE_PATCH(COALESCE(meta, "{}"), :meta)')

        if active is not None:
            values['active'] = 1 if active else 0
            fields.append('active = :active')

        # means you can't set to null
        fields_str = ', '.join(fields)
        _query = f'UPDATE sample SET {fields_str} WHERE id = :id'
        await self.connection.execute(_query, {**values, 'id': id_})
        return id_

    async def merge_samples(
        self,
        id_keep: int = None,
        id_merge: int = None,
    ):
        """Merge two samples together"""
        sid_merge = sample_id_format(id_merge)
        (_, sample_keep), (_, sample_merge) = await asyncio.gather(
            self.get_sample_by_id(id_keep),
            self.get_sample_by_id(id_merge),
        )

        def list_merge(l1: Any, l2: Any) -> list:
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
        values: dict[str, Any] = {
            'sample': {
                'id': id_keep,
                'audit_log_id': audit_log_id,
                'meta': to_db_json(meta),
            },
            'ids': {
                'id_keep': id_keep,
                'id_merge': id_merge,
                'audit_log_id': audit_log_id,
            },
        }

        _query = """
            UPDATE sample
            SET audit_log_id = :audit_log_id,
                meta = :meta
            WHERE id = :id
        """
        _query_seqs = """
            UPDATE assay
            SET sample_id = :id_keep, audit_log_id = :audit_log_id
            WHERE sample_id = :id_merge
        """
        # TODO: merge sequencing groups I guess?
        _query_analyses = """
            UPDATE analysis_sequencing_group
            SET sample_id = :id_keep, audit_log_id = :audit_log_id
            WHERE sample_id = :id_merge
        """
        _query_update_sample_with_audit_log = """
            UPDATE sample
            SET audit_log_id = :audit_log_id
            WHERE id = :id_merge
        """
        _del_sample = """
            DELETE FROM sample
            WHERE id = :id_merge
        """

        async with self.connection.transaction():
            await self.connection.execute(_query, {**values['sample']})
            await self.connection.execute(_query_seqs, {**values['ids']})
            await self.connection.execute(_query_analyses, {**values['ids']})
            await self.connection.execute(
                _query_update_sample_with_audit_log,
                {'id_merge': id_merge, 'audit_log_id': audit_log_id},
            )
            await self.connection.execute(
                _del_sample, {'id_merge': id_merge, 'audit_log_id': audit_log_id}
            )

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
        _query = """
        UPDATE sample
        SET participant_id=:participant_id, audit_log_id = :audit_log_id
        WHERE id = :id
        """
        audit_log_id = await self.audit_log_id()
        values = [
            {'id': i, 'participant_id': pid, 'audit_log_id': audit_log_id}
            for i, pid in zip(ids, participant_ids)
        ]
        await self.connection.execute_many(_query, values)

    # region SEARCH

    async def search(
        self, query, project_ids: list[ProjectId], limit=5
    ) -> list[tuple[ProjectId, int, int, str]]:
        """
        Search by some term, return [ProjectId, SampleInternalId, ParticipantId, ExternalId]
        """

        _query = """
            SELECT s.project, s.id, seid.external_id, s.participant_id
            FROM sample s
            INNER JOIN sample_external_id seid ON s.id = seid.sample_id
            WHERE s.project IN :project_ids AND seid.external_id LIKE :search_pattern
            LIMIT :limit
        """.strip()
        rows = await self.connection.fetch_all(
            _query,
            {
                'project_ids': project_ids,
                'search_pattern': escape_like_term(query) + '%',
                'limit': limit,
            },
        )
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

        _query = """\
            SELECT sample_id AS id, external_id
            FROM sample_external_id
            WHERE external_id IN :external_ids AND project = :project
        """
        rows = await self.connection.fetch_all(
            _query, {'external_ids': external_ids, 'project': project or self.project}
        )
        sample_id_map = {el[1]: el[0] for el in rows}

        return sample_id_map

    async def get_sample_id_map_by_internal_ids(
        self, raw_internal_ids: list[int]
    ) -> tuple[Iterable[ProjectId], dict[int, str]]:
        """Get map of (primary) external sample id by internal id"""
        _query = """
        SELECT sample_id AS id, external_id, project
        FROM sample_external_id
        WHERE sample_id IN :ids AND name = :PRIMARY_EXTERNAL_ORG
        """
        values = {'ids': raw_internal_ids, 'PRIMARY_EXTERNAL_ORG': PRIMARY_EXTERNAL_ORG}
        rows = await self.connection.fetch_all(_query, values)

        sample_id_map = {el['id']: el['external_id'] for el in rows}
        projects = set(el['project'] for el in rows)

        return projects, sample_id_map

    async def get_all_sample_id_map_by_internal_ids(
        self, project: ProjectId
    ) -> dict[int, str]:
        """Get sample id map for all samples"""
        _query = """
        SELECT s.id, seid.external_id
        FROM sample s
        INNER JOIN sample_external_id seid ON s.id = seid.sample_id
        WHERE s.project = :project AND name = :PRIMARY_EXTERNAL_ORG
        """
        rows = await self.connection.fetch_all(
            _query,
            {
                'project': project or self.project,
                'PRIMARY_EXTERNAL_ORG': PRIMARY_EXTERNAL_ORG,
            },
        )
        return {el[0]: el[1] for el in rows}

    # endregion ID MAPS

    # region HISTORY

    async def get_samples_create_date(self, sample_ids: list[int]) -> dict[int, date]:
        """Get a map of {internal_sample_id: date_created} for list of sample_ids"""
        if len(sample_ids) == 0:
            return {}
        _query = """
        SELECT id, min(row_start)
        FROM sample FOR SYSTEM_TIME ALL
        WHERE id in :sids
        GROUP BY id"""
        rows = await self.connection.fetch_all(_query, {'sids': sample_ids})
        return {r[0]: r[1].date() for r in rows}

    async def get_history_of_sample(self, id_: int):
        """Get all versions (history) of a sample"""
        # TODO Re-implement this for the separate sample_external_id table. Doing a join and/or aggregating
        # the external_ids wreaks havoc with FOR SYSTEM_TIME ALL queries, collapsing the history we want to
        # see into one aggregate record. For now, leave the query as is, with external_ids unavailable.
        keys = [
            'id',
            """'{"(not available)": "(not available)"}' AS external_ids""",
            'participant_id',
            'meta',
            'active+1 AS active',
            'type',
            'project',
            'author',
            # 'audit_log_id',  # TODO SampleInternal does not allow an audit_log_id field
        ]
        keys_str = ', '.join(keys)
        _query = f'SELECT {keys_str} FROM sample FOR SYSTEM_TIME ALL WHERE id = :id'

        rows = await self.connection.fetch_all(_query, {'id': id_})
        samples = [SampleInternal.from_db(dict(d)) for d in rows]

        return samples

    # endregion HISTORY

    async def get_samples_with_missing_participants_by_internal_id(
        self, project: ProjectId
    ) -> list[SampleInternal]:
        """Get samples with missing participants"""
        keys = [
            's.id',
            'JSON_OBJECTAGG(seid.name, seid.external_id) AS external_ids',
            's.participant_id',
            's.meta',
            's.active',
            's.type',
            's.project',
        ]
        _query = f"""
            SELECT {', '.join(keys)}
            FROM sample s
            INNER JOIN sample_external_id seid ON s.id = seid.sample_id
            WHERE s.participant_id IS NULL AND s.project = :project
            GROUP BY s.id
        """
        rows = await self.connection.fetch_all(
            _query, {'project': project or self.project}
        )
        return [SampleInternal.from_db(dict(d)) for d in rows]
