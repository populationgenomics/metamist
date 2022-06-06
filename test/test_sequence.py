from test.testbase import DbIsolatedTest, run_test_as_sync

from unittest.mock import patch
import random
from sample_metadata.apis import SequenceApi
from sample_metadata.models import SequenceType, SequenceUpdateModel, SequenceStatus


class TestSequence(DbIsolatedTest):
    """Test sequence class"""

    # tests run in 'sorted by ascii' order

    @run_test_as_sync
    async def test_update_sequence_from_sample_and_type(self):
        """Test updating a sequence from sample and type"""
        seqapi = SequenceApi()
        # TODO: This will need to be replaced with a valid sample ID.
        sample_id = 'CPGLCL44091'
        sequence_type = 'genome'
        latest = seqapi.get_all_sequences_for_sample_id(sample_id)
        sequence_id = latest[sequence_type][0]
        sequence = seqapi.get_sequence_by_id(sequence_id)
        # Pull current status, select new test status
        current_status = sequence['status']
        print(current_status)
        statuses = [
            'received',
            'sent-to-sequencing',
            'completed-sequencing',
            'completed-qc',
            'failed-qc',
            'uploaded',
            'unknown',
        ]
        statuses.remove(current_status)
        new_status = random.choice(statuses)
        print(new_status)
        # Call new endpoint

        sequence_update_model = SequenceUpdateModel(status=SequenceStatus(new_status))

        seqapi.update_sequence_from_sample_and_type(
            sample_id=sample_id,
            sequence_type=SequenceType(sequence_type),
            sequence_update_model=sequence_update_model,
        )

        # validate new status
        sequence = seqapi.get_sequence_by_id(sequence_id)
        print(sequence['status'])
        self.assertEqual(new_status, sequence['status'])

    @patch('sample_metadata.apis.SequenceApi.get_all_sequences_for_sample_id')
    @patch('sample_metadata.apis.SequenceApi.get_sequence_by_id')
    async def test_update_sequence_from_sample_and_type_mock(
        self, get_sequence_by_id, get_all_sequences
    ):
        """Mock test updating a sequence from sample and type"""
        # TODO
