from test.testbase import DbIsolatedTest, run_test_as_sync

import random
from sample_metadata.models import (
    SequenceType,
    SequenceStatus,
    SampleType,
)
from db.python.layers.sample import SampleLayer
from db.python.layers.sequence import SampleSequenceLayer


class TestSequence(DbIsolatedTest):
    """Test sequence class"""

    @run_test_as_sync
    async def setUp(self) -> None:
        super().setUp()

        self.sl = SampleLayer(self.connection)
        self.seql = SampleSequenceLayer(self.connection)
        self.external_sample_id = 'TESTING001'
        self.sequence_type = 'genome'
        self.sequence_status = 'unknown'

        # Create new sample
        await self.sl.insert_sample(
            self.external_sample_id,
            SampleType('blood'),
            active=True,
            meta={'Testing': 'test_sequence.py'},
        )

        sample_ids = await self.sl.get_sample_id_map_by_external_ids(
            [self.external_sample_id], project=None
        )
        self.sample_id = sample_ids[self.external_sample_id]

        # Create new sequence
        _ = await self.seql.insert_sequencing(
            self.sample_id,
            SequenceType(self.sequence_type),
            SequenceStatus(self.sequence_status),
            {},
        )

    @run_test_as_sync
    async def test_update_sequence_from_sample_and_type(self):
        """Test updating a sequence from sample and type"""

        # Create a sample in this test database first, then grab
        latest = await self.seql.get_all_sequence_ids_for_sample_id(self.sample_id)
        sequence_id = latest[self.sequence_type][0]
        sequence = await self.seql.get_sequence_by_id(sequence_id)

        # Pull current status, select new test status
        current_status = sequence.status.value
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

        # Call new endpoint to update sequence status and meta
        meta = {'batch': 1}
        await self.seql.update_sequence_from_sample_and_type(
            sample_id=self.sample_id,
            sequence_type=SequenceType(self.sequence_type),
            status=SequenceStatus(new_status),
            meta=meta,
        )

        # validate new status and meta
        sequence = await self.seql.get_sequence_by_id(sequence_id)
        self.assertEqual(new_status, sequence.status.value)
        self.assertEqual(meta, sequence.meta)

    @run_test_as_sync
    async def test_update_sequence_from_external_id_and_type(self):
        """Test updating a sequence from external id and type"""

        # Create a sample in this test database first, then grab
        latest = await self.seql.get_all_sequence_ids_for_sample_id(self.sample_id)
        sequence_id = latest[self.sequence_type][0]
        sequence = await self.seql.get_sequence_by_id(sequence_id)

        # Pull current status, select new test status
        current_status = sequence.status.value
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

        # Call new endpoint to update sequence status and meta
        meta = {'batch': 1}
        await self.seql.update_sequence_from_external_id_and_type(
            external_id=self.external_sample_id,
            sequence_type=SequenceType(self.sequence_type),
            status=SequenceStatus(new_status),
            meta=meta,
        )

        # validate new status and meta
        sequence = await self.seql.get_sequence_by_id(sequence_id)
        self.assertEqual(new_status, sequence.status.value)
        self.assertEqual(meta, sequence.meta)
