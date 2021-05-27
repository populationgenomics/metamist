from typing import Dict

# from models.models.sequence import SampleSequencing
from models.enums import SequencingType, SequencingStatus

from db.python.connect import DbBase, to_db_json


class SampleSequencingTable(DbBase):
    """
    Capture Sample table operations and queries
    """

    table_name = 'sample'

    def insert_sequencing(
        self,
        sample_id,
        sequence_type: SequencingType,
        status: SequencingStatus,
        sequence_meta: Dict[str, any] = None,
        author=None,
        commit=True,
    ) -> int:
        """
        Create a new sequence for a sample, and add it to database
        """

        _query = """\
INSERT INTO sample_sequencing
    (sample_id, type, meta, status, author)
VALUES (%s, %s, %s, %s, %s) RETURNING id;"""

        with self.get_cursor() as cursor:

            cursor.execute(
                _query,
                (
                    sample_id,
                    sequence_type.value,
                    to_db_json(sequence_meta),
                    status.value,
                    author or self.author,
                ),
            )
            id_of_new_sample = cursor.fetchone()[0]

            if commit:
                self.commit()

        return id_of_new_sample

    def get_latest_sequence_id_by_sample_id(self, sample_id):
        """
        Get latest added sequence ID from internal sample_id
        """
        _query = """\
SELECT id from sample_sequencing
WHERE sample_id = %s
ORDER by id
LIMIT 1
"""
        with self.get_cursor() as cursor:
            cursor.execute(_query, (sample_id,))
            return cursor.fetchone()[0]

    def get_latest_sequence_id_by_external_sample_id(self, external_sample_id):
        """
        Get latest added sequence ID from external sample_id
        """
        _query = """\
SELECT sq.id from sample_sequencing sq
INNER JOIN sample s ON s.id = sq.sample_id
WHERE s.external_id = %s
ORDER by s.id
LIMIT 1
"""
        with self.get_cursor() as cursor:
            cursor.execute(_query, (external_sample_id,))
            return cursor.fetchone()[0]

    def update_status(
        self, sequencing_id, status: SequencingStatus, author=None, commit=True
    ):
        """Update status of sequencing with sequencing_id"""
        _query = 'UPDATE sample_sequencing SET status = %s, author=%s WHERE id = %s'
        with self.get_cursor() as cursor:
            cursor.execute(_query, (status.value, author or self.author, sequencing_id))
            if commit:
                self.commit()
