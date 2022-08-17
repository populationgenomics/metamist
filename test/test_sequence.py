import random
from test.testbase import DbIsolatedTest, run_test_as_sync

from models.models.sample import sample_id_transform_to_raw

from db.python.connect import NotFoundError
from db.python.layers.sample import SampleLayer
from db.python.layers.sequence import SampleSequenceLayer

from sample_metadata.models import (
    SequenceType,
    SequenceStatus,
    SampleType,
)


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

        self.external_sample_no_seq = 'NOSEQ001'

        # Create new sample
        await self.sl.insert_sample(
            self.external_sample_id,
            SampleType('blood'),
            active=True,
            meta={'Testing': 'test_sequence'},
        )

        sample = await self.sl.get_single_by_external_id(
            external_id=self.external_sample_id, project=None
        )
        self.raw_cpg_id = sample_id_transform_to_raw(sample.id)

        # Create new sequence
        await self.seql.insert_sequencing(
            self.raw_cpg_id,
            SequenceType(self.sequence_type),
            SequenceStatus(self.sequence_status),
            {},
        )

        # Create new sample with no sequence
        await self.sl.insert_sample(
            self.external_sample_no_seq,
            SampleType('blood'),
            active=True,
            meta={'Testing': 'test_sequence'},
        )

    @run_test_as_sync
    async def test_update_sequence_from_sample_and_type(self):
        """Test updating a sequence from sample and type"""

        # Create a sample in this test database first, then grab
        latest = await self.seql.get_all_sequence_ids_for_sample_id(self.raw_cpg_id)
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
            sample_id=self.raw_cpg_id,
            sequence_type=SequenceType(self.sequence_type),
            status=SequenceStatus(new_status),
            meta=meta,
        )

        # validate new status and meta
        sequence = await self.seql.get_sequence_by_id(sequence_id)
        self.assertEqual(new_status, sequence.status.value)
        self.assertEqual(meta, sequence.meta)

    @run_test_as_sync
    async def test_invalid_samples(self):
        """Testing an invalid sample update"""
        # define invalid data
        invalid_sample_id = 'INVALID123'
        new_status = 'received'
        meta = {'batch': 1}

        with self.assertRaises(NotFoundError):
            await self.seql.update_sequence_from_sample_and_type(
                sample_id=invalid_sample_id,
                sequence_type=SequenceType(self.sequence_type),
                status=SequenceStatus(new_status),
                meta=meta,
            )

        with self.assertRaises(NotFoundError):
            await self.seql.update_sequence_from_sample_and_type(
                sample_id=self.external_sample_no_seq,
                sequence_type=SequenceType(self.sequence_type),
                status=SequenceStatus(new_status),
                meta=meta,
            )

    @run_test_as_sync
    async def test_upsert_sequence_from_external_id_and_type(self):
        """Test updating a sequence from external id and type"""

        # Create a sample in this test database first, then grab
        latest = await self.seql.get_all_sequence_ids_for_sample_id(self.raw_cpg_id)
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
        await self.seql.upsert_sequence_from_external_id_and_type(
            external_sample_id=self.external_sample_id,
            sequence_type=SequenceType(self.sequence_type),
            status=SequenceStatus(new_status),
            meta=meta,
            sample_type=SampleType('blood'),
        )

        # validate new status and meta
        sequence = await self.seql.get_sequence_by_id(sequence_id)
        self.assertEqual(new_status, sequence.status.value)
        self.assertEqual(meta, sequence.meta)

    @run_test_as_sync
    async def test_new_sample_upsert_sequence_from_external_id_and_type(self):
        """Test updating a sequence from external id and type, where
        the sample does not already exist"""

        # Set up new sample
        external_sample_id = 'NEW_TEST123'

        status = 'uploaded'

        # Call new endpoint to update sequence status and meta
        meta = {'batch': 2}
        sequence_id = await self.seql.upsert_sequence_from_external_id_and_type(
            external_sample_id=external_sample_id,
            sequence_type=SequenceType(self.sequence_type),
            status=SequenceStatus(status),
            meta=meta,
            sample_type=SampleType('blood'),
        )

        # validate new status and meta
        sequence = await self.seql.get_sequence_by_id(sequence_id)
        self.assertEqual(status, sequence.status.value)
        self.assertEqual(meta, sequence.meta)
