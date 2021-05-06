# from models.models.sequence import SampleSequencingStatus
from models.enums import SequencingStatus

from .connect import DbBase


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
