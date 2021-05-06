# from models.models.sequence import SampleSequencingStatus
from models.enums import SequencingStatus

from db.python.connect import DbBase, NotFoundError


class SampleSequencingStatusTable(DbBase):
    """
    Capture SampleSequencingStatus table operations and queries
    """

    def insert_sequencing_status(
        self,
        sequence_id: int,
        status: SequencingStatus,
        commit=True,
    ) -> int:
        """
        Create a new sample, and add it to database
        """

        _query = """\
INSERT INTO sample_sequencing_status
    (sample_sequencing_id, status)
VALUES (%s, %s);"""

        with self.get_cursor() as cursor:

            cursor.execute(_query, (sequence_id, status.value))

            if commit:
                self.commit()

    def get_latest_sequence_id_by_sample_id(self, sample_id) -> int:
        """
        Join the sample_sequencing and sample_sequencing_status tables
        to find the latest status update, then return the sequence id.
        """

        _query = """\
SELECT sq.id FROM sample_sequencing sq
INNER JOIN sample_sequencing_status sqs ON sq.id = sqs.sample_sequencing_id
WHERE sq.sample_id = %s
ORDER BY sqs.timestamp DESC
LIMIT 1;
"""

        with self.get_cursor() as cursor:
            cursor.execute(_query, (sample_id,))
            row = cursor.fetchone()
            if not row:
                raise NotFoundError(f'sequence with sample ID {sample_id}')

            return row[0]
