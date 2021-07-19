from typing import Optional, Dict, List

from models.models.sequence import SampleSequencing
from models.enums import SequencingType, SequencingStatus

from db.python.connect import DbBase, to_db_json


class SampleSequencingTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sample_sequencing'

    async def insert_many_sequencing(
        self,
        sequencing: List[SampleSequencing],
        author=None,
    ):
        """Insert many sequencing, returning no IDs"""

        _query = """\
INSERT INTO sample_sequencing
    (sample_id, type, meta, status, author)
VALUES (:sample_id, :type, :meta, :status, :author);"""

        values = [
            {
                'sample_id': s.sample_id,
                'type': s.type.value,
                'meta': to_db_json(s.meta),
                'status': s.status.value,
                'author': author or self.author,
            }
            for s in sequencing
        ]

        # with encode/database, can't execute many and collect the results
        await self.connection.execute_many(_query, values)

    async def insert_sequencing(
        self,
        sample_id,
        sequence_type: SequencingType,
        status: SequencingStatus,
        sequence_meta: Dict[str, any] = None,
        author=None,
    ) -> int:
        """
        Create a new sequence for a sample, and add it to database
        """

        _query = """\
INSERT INTO sample_sequencing
    (sample_id, type, meta, status, author)
VALUES (:sample_id, :type, :meta, :status, :author)
RETURNING id;"""

        await self.connection.execute(
            _query,
            {
                'sample_id': sample_id,
                'type': sequence_type.value,
                'meta': to_db_json(sequence_meta),
                'status': status.value,
                'author': author or self.author,
            },
        )
        id_of_new_sample = await self.connection.fetch_one()[0]

        return id_of_new_sample

    async def get_latest_sequence_id_by_sample_id(self, sample_id):
        """
        Get latest added sequence ID from internal sample_id
        """
        _query = """\
SELECT id from sample_sequencing
WHERE sample_id = :sample_id
ORDER by id
LIMIT 1
"""
        result = await self.connection.fetch_one(_query, {'sample_id': sample_id})
        return result[0]

    async def get_latest_sequence_id_by_external_sample_id(self, external_sample_id):
        """
        Get latest added sequence ID from external sample_id
        """
        _query = """\
SELECT sq.id from sample_sequencing sq
INNER JOIN sample s ON s.id = sq.sample_id
WHERE s.external_id = :external_id
ORDER by s.id
LIMIT 1
"""
        result = await self.connection.fetch_one(
            _query, {'external_id': external_sample_id}
        )
        return result[0]

    async def update_status(
        self,
        sequencing_id,
        status: SequencingStatus,
        author=None,
    ):
        """Update status of sequencing with sequencing_id"""
        _query = """
UPDATE sample_sequencing
    SET status = :status, author=:author
WHERE id = :sequencing_id
"""

        await self.connection.execute(
            _query,
            {
                'status': status.value,
                'author': author or self.author,
                'sequencing_id': sequencing_id,
            },
        )

    async def update_sequence(
        self,
        sequence_id,
        status: Optional[SequencingStatus] = None,
        meta: Optional[Dict] = None,
        author=None,
    ):
        """
        Can't update type
        """

        fields = {'sequencing_id': sequence_id, 'author': author or self.author}

        updaters = ['author = :author']
        if status is not None:
            updaters.append('status = :status')
            fields['status'] = status.value
        if meta is not None:

            updaters.append('meta = JSON_MERGE_PATCH(meta, :meta)')
            fields['meta'] = to_db_json(meta)

        _query = f"""
UPDATE sample_sequencing
    SET {", ".join(updaters)}
    WHERE id = :sequencing_id
"""
        await self.connection.execute(_query, fields)
        return True
