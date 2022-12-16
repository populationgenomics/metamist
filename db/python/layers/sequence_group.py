from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.tables.sample import SampleTable
from db.python.tables.sequence import SampleSequencingTable


class SampleSequenceLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.seqt: SampleSequencingTable = SampleSequencingTable(connection)
        self.sampt: SampleTable = SampleTable(connection)

    def get_sequence_group_by_id(self, sequence_group_id: int, check_project_ids: bool = True):
        """
        Get sequence group by internal ID
        """
        pass

    def get_sequence_groups_by_ids(self, sequence_group_ids: list[int], check_project_ids: bool = True):
        """
        Get sequence groups by internal IDs
        """
        raise NotImplementedError

    # region CREATE / MUTATE

    def create_sequence_group(self, sequences: list[int]):
        """
        Create a sequence group from a list of sequences,
        return an exception if they're not of the same type
        """
        raise NotImplementedError

    def modify_sequences_in_group(self, sequences: list[int]):
        """
        Change the list of sequences in a sequence group, this first
        archives the existing group, and returns a new sequence group.
        """
        raise NotImplementedError

    def archive_sequence_group(self, sequence_group_id: int):
        """
        Archive sequence group, should you be able to do this?
        What are the consequences:
        - should all relevant single-sample analysis entries be archived
        - why are they being archived?
        """
        raise NotImplementedError

    # endregion

