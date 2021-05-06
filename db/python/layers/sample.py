from db.python.sample import SampleTable
from db.python.sequencingstatus import SampleSequencingStatusTable

from models.enums import SequencingStatus


class SampleLayer:
    """Layer for more complex sample logic"""

    def __init__(self, connection):
        self.connection = connection

    def update_sequencing_status_from_internal_sample_id(
        self, sample_id: int, status: SequencingStatus
    ):
        """Update the sequencing status from the internal sample id"""
        seq_status_table = SampleSequencingStatusTable(connection=self.connection)
        seq_id = seq_status_table.get_latest_sequence_id_by_sample_id(sample_id)
        seq_status_table.insert_sequencing_status(seq_id, status)

    def update_sequencing_status_from_external_sample_id(
        self, external_sample_id: str, status: SequencingStatus
    ):
        """
        Update the sequencing status from the external sample id,
        by first looking up the internal sample id.
        """
        sample_table = SampleTable(connection=self.connection)
        sample = sample_table.get_single_by_external_id(external_sample_id)
        self.update_sequencing_status_from_internal_sample_id(sample.id, status)
