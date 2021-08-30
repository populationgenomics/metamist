from typing import List, Dict, Tuple, Optional

from models.models.sample import Sample, sample_id_format
from models.enums import SampleType

from db.python.connect import DbBase, NotFoundError, to_db_json


class SampleTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sample'

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
        if len(ids) != len(participant_ids):
            raise ValueError(
                f'Number of sampleIDs ({len(ids)}) and ParticipantIds ({len(participant_ids)}) did not match'
            )

        _query = 'UPDATE sample SET participant_id=:participant_id WHERE id = :id'
        values = [
            {'id': i, 'participant_id': pid} for i, pid in zip(ids, participant_ids)
        ]
        await self.connection.execute_many(_query, values)

    async def get_single_by_id(self, internal_id: int) -> Sample:
        """Get a Sample by its external_id"""
        keys = [
            'id',
            'external_id',
            'participant_id',
            'meta',
            'active',
            'type',
        ]
        _query = f'SELECT {", ".join(keys)} from sample where id = :id LIMIT 1;'

        sample_row = await self.connection.fetch_one(_query, {'id': internal_id})

        if sample_row is None:
            raise NotFoundError(f'Couldn\'t find sample with internal id {internal_id}')

        kwargs = {keys[i]: sample_row[i] for i in range(len(keys))}
        sample = Sample.from_db(kwargs)
        return sample

    async def get_all(self, check_active: bool = True) -> List[Sample]:
        """Get all samples"""
        keys = [
            'id',
            'external_id',
            'participant_id',
            'meta',
            'active',
            'type',
        ]
        if check_active:
            _query = f'SELECT {", ".join(keys)} FROM sample WHERE active;'
        else:
            _query = f'SELECT {", ".join(keys)} from sample;'

        sample_rows = await self.connection.fetch_all(_query)
        samples = []
        for sample_row in sample_rows:
            kwargs = {keys[i]: sample_row[i] for i in range(len(keys))}
            sample = Sample.from_db(kwargs)
            samples.append(sample)
        return samples

    async def get_single_by_external_id(
        self, external_id, check_active=True, project: Optional[int] = None
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
        if check_active:
            _query = f"""\
SELECT {", ".join(keys)} FROM sample
WHERE external_id = :eid AND active AND project = :project
LIMIT 1;"""
        else:
            _query = f"""
SELECT {", ".join(keys)} FROM sample
WHERE external_id = :eid AND project = :project
LIMIT 1;"""

        sample_row = await self.connection.fetch_one(
            _query, {'eid': external_id, 'project': project or self.project}
        )

        if sample_row is None:
            raise NotFoundError(
                f'Couldn\'t find active sample with external id {external_id}'
            )

        kwargs = {keys[i]: sample_row[i] for i in range(len(keys))}
        sample = Sample.from_db(kwargs)
        return sample

    async def get_sample_id_map_by_external_ids(
        self,
        external_ids: List[str],
        allow_missing=False,
        project: Optional[int] = None,
    ) -> Dict[str, int]:
        """Get map of external sample id to internal id"""
        _query = 'SELECT id, external_id FROM sample WHERE external_id in :external_ids AND project = :project'
        rows = await self.connection.fetch_all(
            _query, {'external_ids': external_ids, 'project': project or self.project}
        )
        sample_id_map = {el[1]: el[0] for el in rows}
        if not allow_missing and len(sample_id_map) != len(external_ids):
            provided_external_ids = set(external_ids)
            # do the check again, but use the set this time
            # (in case we're provided a list with duplicates)
            if len(sample_id_map) != len(provided_external_ids):
                # we have samples missing from the map, so we'll 404 the whole thing
                missing_sample_ids = provided_external_ids - set(sample_id_map.keys())

                raise NotFoundError(
                    f"Couldn't find samples with IDS: {', '.join(missing_sample_ids)}"
                )

        return sample_id_map

    async def get_sample_id_map_by_internal_ids(
        self, raw_internal_ids: List[int]
    ) -> Dict[int, str]:
        """Get map of external sample id to internal id"""
        _query = 'SELECT id, external_id FROM sample WHERE id in :ids'
        values = {'ids': raw_internal_ids}

        rows = await self.connection.fetch_all(_query, values)
        sample_id_map = {el[0]: el[1] for el in rows}
        if len(sample_id_map) != len(raw_internal_ids):
            provided_external_ids = set(raw_internal_ids)
            # do the check again, but use the set this time
            # (in case we're provided a list with duplicates)
            if len(sample_id_map) != len(provided_external_ids):
                # we have samples missing from the map, so we'll 404 the whole thing
                missing_sample_ids = provided_external_ids - set(sample_id_map.keys())
                raise NotFoundError(
                    f"Couldn't find samples with IDS: {', '.join(sample_id_format(list(missing_sample_ids)))}"
                )

        return sample_id_map

    async def get_all_sample_id_map_by_internal_ids(
        self, project: Optional[int] = None
    ):
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
        active=None,
    ):
        """Get samples by some criteria"""
        keys = [
            'id',
            'external_id',
            'participant_id',
            'meta',
            'active+1 as active',
            'type',
        ]
        keys_str = ', '.join(keys)

        where = []
        replacements = {}

        if project_ids:
            where.append('project in :project_ids')
            replacements['project_ids'] = project_ids

        if sample_ids:
            where.append('id in :sample_ids')
            replacements['id']: sample_ids
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

        samples = await self.connection.fetch_all(_query, replacements)
        return [Sample.from_db(dict(s)) for s in samples]

    async def samples_with_missing_participants(
        self, project: Optional[int] = None
    ) -> List[Tuple[str, int]]:
        """Get ["""
        _query = """
SELECT id, external_id
FROM sample
WHERE participant_id IS NULL AND project = :project
"""
        rows = await self.connection.fetch_all(
            _query, {'project': project or self.project}
        )
        return [(row['external_id'], row['id']) for row in rows]
