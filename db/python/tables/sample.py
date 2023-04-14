import asyncio
from collections import defaultdict
from datetime import date
from typing import Iterable, Any

from db.python.connect import DbBase, NotFoundError
from db.python.utils import to_db_json
from db.python.tables.project import ProjectId
from models.models.sample import SampleInternal, sample_id_format


class SampleTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sample'

    # region GETS

    async def get_project_ids_for_sample_ids(self, sample_ids: list[int]) -> set[int]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = 'SELECT DISTINCT project FROM sample WHERE id in :sample_ids'
        rows = await self.connection.fetch_all(_query, {'sample_ids': sample_ids})
        return set(r['project'] for r in rows)

    async def get_sample_by_id(
        self, internal_id: int
    ) -> tuple[ProjectId, SampleInternal]:
        """Get a Sample by its external_id"""
        keys = [
            'id',
            'external_id',
            'participant_id',
            'meta',
            'active',
            'type',
            'project',
        ]
        _query = f'SELECT {", ".join(keys)} from sample where id = :id LIMIT 1;'

        sample_row = await self.connection.fetch_one(_query, {'id': internal_id})

        if sample_row is None:
            raise NotFoundError(
                f'Couldn\'t find sample with internal id {internal_id} (CPG id: {sample_id_format(internal_id)})'
            )

        d = dict(sample_row)
        project = d.get('project')
        sample = SampleInternal.from_db(d)
        return project, sample

    async def get_single_by_external_id(
        self, external_id, project: ProjectId, check_active=True
    ) -> SampleInternal:
        """Get a Sample by its external_id"""
        keys = [
            'id',
            'external_id',
            'participant_id',
            'meta',
            'active',
            'type',
            'project',
            'author',
        ]
        wheres = ['external_id = :external_id', 'project = :project']
        proj = project or self.project
        values = {'external_id': external_id, 'project': proj}
        if check_active:
            wheres.append('active')

        wheres_str = ' AND '.join(wheres)
        _query = f"""\
            SELECT {", ".join(keys)} FROM sample
            WHERE {wheres_str}
            LIMIT 1;
        """

        sample_row = await self.connection.fetch_one(_query, values)

        if sample_row is None:
            raise NotFoundError(
                f'Couldn\'t find active sample with external id {external_id} in project {proj}'
            )

        sample_row = dict(sample_row)
        sample_row.update(values)

        return SampleInternal.from_db(sample_row)

    async def get_samples_by(
        self,
        sample_ids: list[int] = None,
        meta: dict[str, Any] = None,
        participant_ids: list[int] = None,
        project_ids=None,
        active=True,
    ) -> tuple[Iterable[ProjectId], list[SampleInternal]]:
        """Get samples by some criteria"""
        keys = [
            'id',
            'external_id',
            'participant_id',
            'meta',
            'active+1 as active',
            'type',
            'project',
        ]
        keys_str = ', '.join(keys)

        where = []
        replacements = {}

        if project_ids:
            where.append('project in :project_ids')
            replacements['project_ids'] = project_ids

        if sample_ids:
            where.append('id in :sample_ids')
            replacements['sample_ids'] = sample_ids

        if meta:
            for k, v in meta.items():
                k_replacer = f'meta_{k}'
                where.append(f"json_extract(meta, '$.{k}') = :{k_replacer}")
                replacements[k_replacer] = v

        if participant_ids:
            where.append('participant_id in :participant_ids')
            replacements['participant_ids'] = participant_ids

        if active is True:
            where.append('active')
        elif active is False:
            where.append('NOT active')

        _query = f'SELECT {keys_str} FROM sample'
        if where:
            _query += f' WHERE {" AND ".join(where)}'

        sample_rows = await self.connection.fetch_all(_query, replacements)

        sample_dicts = [dict(s) for s in sample_rows]
        projects: Iterable[int] = set(
            s['project'] for s in sample_dicts if s.get('project')
        )
        samples = list(map(SampleInternal.from_db, sample_dicts))
        return projects, samples

    async def get_samples_by_analysis_ids(
        self, analysis_ids: list[int]
    ) -> tuple[set[ProjectId], dict[int, list[SampleInternal]]]:
        """Get map of samples by analysis_ids"""
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
        SELECT {", ".join("s." + k for k in keys)}, a_s.analysis_id
        FROM analysis_sequencing_group asg
        INNER JOIN sequencing_group sg ON sg.id = asg.sequencing_group_id
        INNER JOIN sample s ON s.id = sg.sample_id
        WHERE asg.analysis_id IN :aids
        """
        rows = await self.connection.fetch_all(_query, {'aids': analysis_ids})

        ds = [dict(d) for d in rows]

        mapped_analysis_to_sample_id: dict[int, list[int]] = defaultdict(list)
        sample_map: dict[int, SampleInternal] = {}
        projects: set[int] = set()
        for row in ds:
            sid = row['id']
            mapped_analysis_to_sample_id[row['analysis_id']].append(sid)
            projects.add(row['project'])

            if sid not in ds:
                sample_map[sid] = SampleInternal.from_db({k: row.get(k) for k in keys})

        analysis_map: dict[int, list[SampleInternal]] = {
            analysis_id: [sample_map.get(sid) for sid in sids]
            for analysis_id, sids in mapped_analysis_to_sample_id.items()
        }

        return projects, analysis_map

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
        author=None,
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
            ('author', author or self.author),
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
        author: str = None,
        active: bool = None,
    ):
        """Update a single sample"""

        values: dict[str, Any] = {
            'author': author or self.author,
        }
        fields = [
            'author = :author',
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
        author: str = None,
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

        values: dict[str, Any] = {
            'sample': {
                'id': id_keep,
                'author': author or self.author,
                'meta': to_db_json(meta),
            },
            'ids': {'id_keep': id_keep, 'id_merge': id_merge},
        }

        _query = """
            UPDATE sample
            SET author = :author,
                meta = :meta
            WHERE id = :id
        """
        _query_seqs = f"""
            UPDATE sample_sequencing
            SET sample_id = :id_keep
            WHERE sample_id = :id_merge
        """
        _query_analyses = f"""
            UPDATE analysis_sample
            SET sample_id = :id_keep
            WHERE sample_id = :id_merge
        """
        _del_sample = f"""
            DELETE FROM sample
            WHERE id = :id_merge
        """

        async with self.connection.transaction():
            await self.connection.execute(_query, {**values['sample']})
            await self.connection.execute(_query_seqs, {**values['ids']})
            await self.connection.execute(_query_analyses, {**values['ids']})
            await self.connection.execute(_del_sample, {'id_merge': id_merge})

        project, new_sample = await self.get_sample_by_id(id_keep)
        new_sample.project = project
        new_sample.author = author or self.author

        return new_sample

    async def update_many_participant_ids(
        self, ids: list[int], participant_ids: list[int]
    ):
        """
        Update participant IDs for many samples
        Expected len(ids) == len(participant_ids)
        """
        _query = 'UPDATE sample SET participant_id=:participant_id WHERE id = :id'
        values = [
            {'id': i, 'participant_id': pid} for i, pid in zip(ids, participant_ids)
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
                'search_pattern': self.escape_like_term(query) + '%',
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
