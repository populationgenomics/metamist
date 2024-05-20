import asyncio
import dataclasses
from datetime import date
from typing import Any, Iterable

from db.python.tables.base import DbBase
from db.python.utils import (
    GenericFilter,
    GenericFilterModel,
    GenericMetaFilter,
    NotFoundError,
    escape_like_term,
    to_db_json,
)
from models.models.project import ProjectId
from models.models.sample import SampleInternal, sample_id_format


@dataclasses.dataclass(kw_only=True)
class SampleFilter(GenericFilterModel):
    """
    Sample filter model
    """

    id: GenericFilter[int] | None = None
    type: GenericFilter[str] | None = None
    meta: GenericMetaFilter | None = None
    external_id: GenericFilter[str] | None = None
    participant_id: GenericFilter[int] | None = None
    project: GenericFilter[ProjectId] | None = None
    active: GenericFilter[bool] | None = None

    def __hash__(self):  # pylint: disable=useless-super-delegation
        return super().__hash__()


class SampleTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sample'

    common_get_keys = [
        'id',
        'external_id',
        'participant_id',
        'meta',
        'active',
        'type',
        'project',
    ]

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
        wheres, values = filter_.to_sql(field_overrides={})
        if not wheres:
            raise ValueError(f'Invalid filter: {filter_}')
        common_get_keys_str = ', '.join(self.common_get_keys)
        _query = f"""
        SELECT {common_get_keys_str}
        FROM sample
        WHERE {wheres}
        """
        rows = await self.connection.fetch_all(_query, values)
        samples = [SampleInternal.from_db(dict(r)) for r in rows]
        projects = set(s.project for s in samples)
        return projects, samples

    async def get_sample_by_id(
        self, internal_id: int
    ) -> tuple[ProjectId, SampleInternal]:
        """Get a Sample by its external_id"""
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
        Get a single sample by its external_id
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
        external_id: str,
        sample_type: str,
        active: bool,
        meta: dict | None,
        participant_id: int | None,
        project=None,
    ) -> int:
        """
        Create a new sample, and add it to database
        """

        kv_pairs = [
            ('external_id', external_id),
            ('participant_id', participant_id),
            ('meta', to_db_json(meta or {})),
            ('type', sample_type),
            ('active', active),
            ('audit_log_id', await self.audit_log_id()),
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

        return id_of_new_sample

    async def update_sample(
        self,
        id_: int,
        meta: dict | None,
        participant_id: int | None,
        external_id: str | None,
        type_: str | None,
        active: bool = None,
    ):
        """Update a single sample"""

        values: dict[str, Any] = {'audit_log_id': await self.audit_log_id()}
        fields = [
            'audit_log_id = :audit_log_id',
        ]
        if participant_id:
            values['participant_id'] = participant_id
            fields.append('participant_id = :participant_id')

        if external_id:
            values['external_id'] = external_id
            fields.append('external_id = :external_id')

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
            SELECT project, id, external_id, participant_id
            FROM sample
            WHERE project in :project_ids AND external_id LIKE :search_pattern
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
            SELECT id, external_id
            FROM sample
            WHERE external_id in :external_ids AND project = :project
        """
        rows = await self.connection.fetch_all(
            _query, {'external_ids': external_ids, 'project': project or self.project}
        )
        sample_id_map = {el[1]: el[0] for el in rows}

        return sample_id_map

    async def get_sample_id_map_by_internal_ids(
        self, raw_internal_ids: list[int]
    ) -> tuple[Iterable[ProjectId], dict[int, str]]:
        """Get map of external sample id to internal id"""
        _query = 'SELECT id, external_id, project FROM sample WHERE id in :ids'
        values = {'ids': raw_internal_ids}
        rows = await self.connection.fetch_all(_query, values)

        sample_id_map = {el['id']: el['external_id'] for el in rows}
        projects = set(el['project'] for el in rows)

        return projects, sample_id_map

    async def get_all_sample_id_map_by_internal_ids(
        self, project: ProjectId
    ) -> dict[int, str]:
        """Get sample id map for all samples"""
        _query = 'SELECT id, external_id FROM sample WHERE project = :project'
        rows = await self.connection.fetch_all(
            _query, {'project': project or self.project}
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
        keys = [
            'id',
            'external_id',
            'participant_id',
            'meta',
            'active+1 as active',
            'type',
            'project',
            'author',
            'audit_log_id',
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
            'id',
            'external_id',
            'participant_id',
            'meta',
            'active',
            'type',
            'project',
        ]
        _query = f"""
            SELECT {', '.join(keys)}
            FROM sample
            WHERE participant_id IS NULL AND project = :project
        """
        rows = await self.connection.fetch_all(
            _query, {'project': project or self.project}
        )
        return [SampleInternal.from_db(dict(d)) for d in rows]
