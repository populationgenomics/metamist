from typing import List, Dict

from models.models.sample import Sample, sample_id_format
from models.enums import SampleType

from db.python.connect import DbBase, NotFoundError, to_db_json
from db.python.tables.sample_map import SampleMapTable


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
        sample_id=None,
        participant_id=None,
        author=None,
    ) -> int:
        """
        Create a new sample, and add it to database
        """

        if sample_id is None:
            st = SampleMapTable(author=self.author)
            sample_id = await st.generate_sample_id(project=self.project)

        kv_pairs = [
            ('id', sample_id),
            ('external_id', external_id),
            ('participant_id', participant_id),
            ('meta', to_db_json(meta)),
            ('type', sample_type.value),
            ('active', active),
            ('author', author or self.author),
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
        sample = Sample.from_db(**kwargs)
        return sample

    async def get_single_by_external_id(self, external_id, check_active=True) -> Sample:
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
SELECT {", ".join(keys)} from sample
    where external_id = :eid AND active
    LIMIT 1;"""
        else:
            _query = f'SELECT {", ".join(keys)} from sample where external_id = :eid LIMIT 1;'

        sample_row = await self.connection.fetch_one(_query, {'eid': external_id})

        if sample_row is None:
            raise NotFoundError(
                f'Couldn\'t find active sample with external id {external_id}'
            )

        kwargs = {keys[i]: sample_row[i] for i in range(len(keys))}
        sample = Sample.from_db(**kwargs)
        return sample

    async def get_sample_id_map_by_external_ids(
        self, external_ids: List[str]
    ) -> Dict[str, int]:
        """Get map of external sample id to internal id"""
        _query = 'SELECT id, external_id FROM sample WHERE external_id in :external_ids'
        rows = await self.connection.fetch_all(_query, {'external_ids': external_ids})
        sample_id_map = {el[1]: el[0] for el in rows}
        if len(sample_id_map) != len(external_ids):
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
        rows = await self.connection.fetch_all(_query, {'ids': raw_internal_ids})
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
