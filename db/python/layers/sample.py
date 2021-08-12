from models.enums import SequenceStatus

# from db.python.tables.sample import SampleTable
from db.python.tables.sequencing import SampleSequencingTable
from db.python.layers.base import BaseLayer


class SampleLayer(BaseLayer):
    """Layer for more complex sample logic"""

    async def update_sequencing_status_from_internal_sample_id(
        self, sample_id: int, status: SequenceStatus
    ):
        """Update the sequencing status from the internal sample id"""
        seq_table = SampleSequencingTable(connection=self.connection)
        seq_id = seq_table.get_latest_sequence_id_by_sample_id(sample_id)
        return seq_table.update_status(seq_id, status)

    async def update_sequencing_status_from_external_sample_id(
        self, external_sample_id: str, status: SequenceStatus
    ):
        """
        Update the sequencing status from the external sample id,
        by first looking up the internal sample id.
        """
        seq_table = SampleSequencingTable(connection=self.connection)
        seq_id = await seq_table.get_latest_sequence_id_by_external_sample_id(
            external_sample_id
        )
        return await seq_table.update_status(seq_id, status)

    # def get_samples_to_analyse_since(self, date: datetime):
    #     # get all the new, renamed or swapped samples
    #     sample_table = SampleTable(connection=self.connection)
