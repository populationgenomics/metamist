from datetime import datetime

from db.python.tables.sample import SampleTable
from db.python.tables.sequencing import SampleSequencingTable

from models.enums import SequencingStatus


class SampleLayer:
    """Layer for more complex sample logic"""

    def __init__(self, connection, author):
        self.connection = connection
        self.author = author

    def update_sequencing_status_from_internal_sample_id(
        self, sample_id: int, status: SequencingStatus
    ):
        """Update the sequencing status from the internal sample id"""
        seq_table = SampleSequencingTable(
            connection=self.connection, author=self.author
        )
        seq_id = seq_table.get_latest_sequence_id_by_sample_id(sample_id)
        return seq_table.update_status(seq_id, status)

    def update_sequencing_status_from_external_sample_id(
        self, external_sample_id: str, status: SequencingStatus
    ):
        """
        Update the sequencing status from the external sample id,
        by first looking up the internal sample id.
        """
        seq_table = SampleSequencingTable(
            connection=self.connection, author=self.author
        )
        seq_id = seq_table.get_latest_sequence_id_by_external_sample_id(
            external_sample_id
        )
        return seq_table.update_status(seq_id, status)

    # def get_samples_to_analyse_since(self, date: datetime):
    #     # get all the new, renamed or swapped samples
    #     sample_table = SampleTable(connection=self.connection)
