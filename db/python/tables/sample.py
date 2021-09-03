from typing import List, Dict, Tuple, Iterable, Set

from db.python.connect import DbBase, NotFoundError, to_db_json
from db.python.tables.project import ProjectId
from models.enums import SampleType
from models.models.sample import Sample


class SampleTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sample'

    async def get_project_ids_for_sample_ids(self, sample_ids: List[int]) -> Set[int]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = 'SELECT project FROM sample WHERE id in :sample_ids GROUP BY project'
        rows = await self.connection.fetch_all(_query, {'sample_ids': sample_ids})
        return set(r['project'] for r in rows)

    async def insert_sample(
        self,
        external_id,
        sample_type: SampleType,
        active,
        meta=None,
        participant_id=None,
        author=None,
        project=None,
    ) -> int:
        """
        Create a new sample, and add it to database
        """

        kv_pairs = [
            ('external_id', external_id),
            ('participant_id', participant_id),
            ('meta', to_db_json(meta)),
            ('type', sample_type.value),
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
VALUES ({cs_id_keys}) RETURNING id;"""

        id_of_new_sample = await self.connection.fetch_val(
            _query,
            dict(kv_pairs),
        )

        return id_of_new_sample

    async def update_sample(
        self,
        id_: int,
        meta: Dict = None,
        participant_id: int = None,
        type_: SampleType = None,
        author: str = None,
        active: bool = None,
    ):
        """Update a single sample"""

        values = {
            'author': author or self.author,
        }
        fields = [
            'author = :author',
        ]
        if participant_id:
            values['participant_id'] = participant_id
            fields.append('participant_id = :participant_id')

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
        return True

    async def update_many_participant_ids(
        self, ids: List[int], participant_ids: List[int]
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

    async def get_single_by_id(self, internal_id: int) -> Tuple[ProjectId, Sample]:
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
            raise NotFoundError(f'Couldn\'t find sample with internal id {internal_id}')

        d = dict(sample_row)
        project = d.pop('project')
        sample = Sample.from_db(d)
        return project, sample

    async def get_all(
        self, check_active: bool = True
    ) -> Tuple[Iterable[ProjectId], List[Sample]]:
        """Get all samples"""
        keys = [
            'id',
            'external_id',
            'participant_id',
            'meta',
            'active',
            'type',
            'project',
        ]
        _query = f'SELECT {", ".join(keys)} FROM sample'
        if check_active:
            _query += ' WHERE active'

        sample_rows = await self.connection.fetch_all(_query)
        sample_dicts = [dict(s) for s in sample_rows]
        projects = set(s.pop('project') for s in sample_dicts)
        samples = list(map(Sample.from_db, sample_dicts))
        return projects, samples

    async def get_single_by_external_id(
        self, external_id, project: ProjectId, check_active=True
    ) -> Sample:
        """Get a Sample by its external_id"""
        keys = [
            'id',
            'external_id',
            'participant_id',
            'meta',
            'active',
            'type',
        ]
        wheres = ['external_id = :eid', 'project = :project']
        values = {'eid': external_id, 'project': project or self.project}
        if check_active:
            wheres.append('active')

        wheres_str = ' AND '.join(wheres)
        _query = f"""\
SELECT {", ".join(keys)} FROM sample
WHERE {wheres_str}
LIMIT 1;"""

        sample_row = await self.connection.fetch_one(_query, values)

        if sample_row is None:
            raise NotFoundError(
                f'Couldn\'t find active sample with external id {external_id}'
            )

        return Sample.from_db(dict(sample_row))

    async def get_sample_id_map_by_external_ids(
        self,
        external_ids: List[str],
        project: ProjectId,
    ) -> Dict[str, int]:
        """Get map of external sample id to internal id"""
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
        self, raw_internal_ids: List[int]
    ) -> Tuple[Iterable[ProjectId], Dict[int, str]]:
        """Get map of external sample id to internal id"""
        _query = 'SELECT id, external_id, project FROM sample WHERE id in :ids'
        values = {'ids': raw_internal_ids}
        rows = await self.connection.fetch_all(_query, values)

        sample_id_map = {el['id']: el['external_id'] for el in rows}
        projects = set(el['project'] for el in rows)

        return projects, sample_id_map

    async def get_all_sample_id_map_by_internal_ids(
        self, project: ProjectId
    ) -> Dict[int, str]:
        """Get sample id map for all samples"""
        _query = 'SELECT id, external_id FROM sample WHERE project = :project'
        rows = await self.connection.fetch_all(
            _query, {'project': project or self.project}
        )
        return {el[0]: el[1] for el in rows}

    async def get_samples_by(
        self,
        sample_ids: List[int] = None,
        meta: Dict[str, any] = None,
        participant_ids: List[int] = None,
        project_ids=None,
        active=True,
    ) -> Tuple[Iterable[ProjectId], List[Sample]]:
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
            replacements['sample_ids']: sample_ids

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
        projects = set(s.get('project') for s in sample_dicts)
        samples = list(map(Sample.from_db, sample_dicts))
        return projects, samples

    async def get_sample_with_missing_participants_by_internal_id(
        self, project: ProjectId
    ) -> Dict[int, str]:
        """Get samples with missing participants"""
        _query = """
SELECT id, external_id
FROM sample
WHERE participant_id IS NULL AND project = :project
"""
        rows = await self.connection.fetch_all(
            _query, {'project': project or self.project}
        )
        return {row['id']: row['external_id'] for row in rows}
