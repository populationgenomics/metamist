from models.models.sample import Sample
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
        ]

        keys = [k for k, _ in kv_pairs]
        cs_keys = ', '.join(keys)
        cs_id_keys = ', '.join(f':{k}' for k in keys)
        _query = f"""\
INSERT INTO sample
    ({cs_keys})
VALUES ({cs_id_keys});"""

        await self.connection.execute(
            _query,
            dict(kv_pairs),
        )
        id_of_new_sample = (
            await self.connection.fetch_one('SELECT LAST_INSERT_ID();')
        )[0]

        return id_of_new_sample

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
