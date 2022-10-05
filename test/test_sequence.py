import logging
import random
from operator import attrgetter

from models.enums import SampleType
from test.testbase import DbIsolatedTest, run_as_sync

from db.python.connect import NotFoundError
from db.python.layers.sample import SampleLayer
from db.python.layers.sequence import SampleSequenceLayer, SequenceType, SequenceStatus

class TestSequence(DbIsolatedTest):
    """Test sequence class"""

    # pylint: disable=too-many-instance-attributes

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()

        self.slayer = SampleLayer(self.connection)
        self.seqlayer = SampleSequenceLayer(self.connection)
        self.external_sample_id = 'TESTING001'

        # Create new sample
        self.sample_id_raw = await self.slayer.insert_sample(
            self.external_sample_id,
            SampleType.BLOOD,
            active=True,
            meta={'Testing': 'test_sequence'},
        )


        # self.sequence_one = SampleSequencing(
        #     sample_id=self.external_sample_id,
        #     status='unknown',
        #     type='genome',
        #     meta={'batch': '1'},
        # )
        # self.sequence_two = SampleSequencing(
        #     sample_id=self.external_sample_id,
        #     status='received',
        #     type='exome',
        #     meta={'batch': '2'},
        # )
        #
        # self.external_sample_no_seq = 'NOSEQ001'
        #
        #
        #
        # self.sample = await sl.get_single_by_external_id(
        #     external_id=self.external_sample_id, project=self.project_id
        # )
        #
        # # Create new sequence
        # self.seq1 = await self.seql.insert_sequencing(
        #     sample_id=self.raw_cpg_id,
        #     sequence_type=self.sequence_one.type,
        #     status=self.sequence_one.status,
        #     sequence_meta=self.sequence_one.meta,
        #     external_ids={},
        # )
        # # Create second sequence
        # self.seq2 = await self.seql.insert_sequencing(
        #     sample_id=self.raw_cpg_id,
        #     sequence_type=self.sequence_two.type,
        #     status=self.sequence_two.status,
        #     sequence_meta=self.sequence_two.meta,
        #     external_ids={},
        # )
        #
        # # Update attributes in SampleSequencing objects for validation
        # self.sequence_one.id = self.seq1
        # self.sequence_two.id = self.seq2
        # self.sequence_one.sample_id = self.raw_cpg_id
        # self.sequence_two.sample_id = self.raw_cpg_id
        # self.all_sequences = [self.sequence_one, self.sequence_two]
        #
        # # Create new sample with no sequence
        # await sl.insert_sample(
        #     self.external_sample_no_seq,
        #     SampleType('blood'),
        #     active=True,
        #     meta={'Testing': 'test_sequence'},
        # )

    @run_as_sync
    async def test_insert_sequence(self):
        external_ids = {'default': 'SEQ01', 'collaborator2': 'CBSEQ_1'}
        meta = {
            '1': 1,
            'nested': {
                'nested': 'dict'
            },
            "alpha": ["b", "e", "t"]
        }
        seq_id = await self.seqlayer.insert_sequencing(
            sample_id=self.sample_id_raw,
            sequence_type=SequenceType.GENOME,
            status=SequenceStatus.UPLOADED,
            sequence_meta=meta,
            external_ids={'default': 'SEQ01', 'collaborator2': 'CBSEQ_1'},
        )

        sequence = await self.seqlayer.get_sequence_by_id(sequence_id=seq_id, check_project_id=False)

        self.assertEqual(seq_id, sequence.id)
        self.assertEqual(self.sample_id_raw, int(sequence.sample_id))
        self.assertEqual('genome', sequence.type.value)
        self.assertEqual('uploaded', sequence.status.value)
        self.assertDictEqual(external_ids, sequence.external_ids)
        self.assertDictEqual(meta, sequence.meta)

    @run_as_sync
    async def test_update(self):
        seq_id = await self.seqlayer.insert_sequencing(
            sample_id=self.sample_id_raw,
            sequence_type=SequenceType.GENOME,
            status=SequenceStatus.RECEIVED,
            sequence_meta={'a': 1, 'b': 2},
            external_ids={'default': 'SEQ01', 'untouched': 'UTC+1'},
        )

        await self.seqlayer.update_sequence(seq_id, external_ids={'default': 'NSQ_01', 'ext': 'EXTSEQ01'}, status=SequenceStatus.UPLOADED, meta={'a': 2, 'c': True})

        update_sequence = await self.seqlayer.get_sequence_by_id(sequence_id=seq_id, check_project_id=False)

        self.assertEqual(seq_id, update_sequence.id)
        self.assertEqual(self.sample_id_raw, int(update_sequence.sample_id))
        self.assertEqual('genome', update_sequence.type.value)
        self.assertEqual('uploaded', update_sequence.status.value)
        self.assertDictEqual({'default': 'NSQ_01', 'ext': 'EXTSEQ01', 'untouched': 'UTC+1'}, update_sequence.external_ids)
        self.assertDictEqual({'a': 2, 'b': 2, 'c': True}, update_sequence.meta)


    @run_as_sync
    async def test_update_status(self):
        """
        Test update all sequence statuses
        """
        seq_id = await self.seqlayer.insert_sequencing(
            sample_id=self.sample_id_raw,
            sequence_type=SequenceType.GENOME,
            status=SequenceStatus.UPLOADED,
            sequence_meta={},
            external_ids={},
        )

        # cycle through all statuses, and check that works
        for status in SequenceStatus:
            await self.seqlayer.update_status(seq_id, status, check_project_id=False)
            status_to_check = await self.connection.connection.fetch_one('SELECT status FROM sample_sequencing WHERE id = :id', {'id': seq_id})
            self.assertEqual(status.value, status_to_check['status'])









    # @run_as_sync
    # async def test_update_sequence_from_sample_and_type(self):
    #     """Test updating a sequence from sample and type"""
    #
    #     # Create a sample in this test database first, then grab
    #     latest = await self.seql.get_sequence_ids_for_sample_id(self.raw_cpg_id)
    #     sequence_id = latest[self.sequence_one.type.value][-1]
    #     sequence = await self.seql.get_sequence_by_id(
    #         sequence_id, check_project_id=False
    #     )
    #
    #     # Pull current status, select new test status
    #     current_status = sequence.status.value
    #     statuses = [
    #         'received',
    #         'sent-to-sequencing',
    #         'completed-sequencing',
    #         'completed-qc',
    #         'failed-qc',
    #         'uploaded',
    #         'unknown',
    #     ]
    #     statuses.remove(current_status)
    #     new_status = random.choice(statuses)
    #
    #     # Call new endpoint to update sequence status and meta
    #     meta = {'batch': 1}
    #     await self.seql.update_latest_sequence_from_sample_and_type(
    #         sample_id=self.raw_cpg_id,
    #         sequence_type=self.sequence_one.type,
    #         status=SequenceStatus(new_status),
    #         meta=meta,
    #     )
    #
    #     # validate new status and meta
    #     sequence = await self.seql.get_sequence_by_id(
    #         sequence_id, check_project_id=False
    #     )
    #     self.assertEqual(new_status, sequence.status.value)
    #     self.assertEqual(meta, sequence.meta)
    #
    # @run_as_sync
    # async def test_invalid_samples(self):
    #     """Testing an invalid sample update"""
    #     # define invalid data
    #     invalid_sample_id = 'INVALID123'
    #     new_status = 'received'
    #     meta = {'batch': 1}
    #
    #     with self.assertRaises(NotFoundError):
    #         await self.seql.update_latest_sequence_from_sample_and_type(
    #             sample_id=invalid_sample_id,
    #             sequence_type=self.sequence_one.type,
    #             status=SequenceStatus(new_status),
    #             meta=meta,
    #         )
    #
    #     with self.assertRaises(NotFoundError):
    #         await self.seql.update_latest_sequence_from_sample_and_type(
    #             sample_id=self.external_sample_no_seq,
    #             sequence_type=self.sequence_one.type,
    #             status=SequenceStatus(new_status),
    #             meta=meta,
    #         )
    #
    # @run_as_sync
    # async def test_upsert_sequence_from_external_id_and_type(self):
    #     """Test updating a sequence from external id and type"""
    #
    #     # Create a sample in this test database first, then grab
    #     latest = await self.seql.get_sequence_ids_for_sample_id(self.raw_cpg_id)
    #     sequence_id = latest[self.sequence_one.type.value][0]
    #     sequence = await self.seql.get_sequence_by_id(
    #         sequence_id, check_project_id=False
    #     )
    #
    #     # Pull current status, select new test status
    #     current_status = sequence.status.value
    #     statuses = [
    #         'received',
    #         'sent-to-sequencing',
    #         'completed-sequencing',
    #         'completed-qc',
    #         'failed-qc',
    #         'uploaded',
    #         'unknown',
    #     ]
    #     statuses.remove(current_status)
    #     new_status = random.choice(statuses)
    #
    #     # Call new endpoint to update sequence status and meta
    #     meta = {'batch': 1}
    #     await self.seql.upsert_sequence_from_external_id_and_type(
    #         external_sample_id=self.external_sample_id,
    #         sequence_type=self.sequence_one.type,
    #         status=SequenceStatus(new_status),
    #         meta=meta,
    #         sample_type=SampleType('blood'),
    #     )
    #
    #     # validate new status and meta
    #     sequence = await self.seql.get_sequence_by_id(
    #         sequence_id, check_project_id=False
    #     )
    #     self.assertEqual(new_status, sequence.status.value)
    #     self.assertEqual(meta, sequence.meta)
    #
    # @run_as_sync
    # async def test_new_sample_upsert_sequence_from_external_id_and_type(self):
    #     """Test updating a sequence from external id and type, where
    #     the sample does not already exist"""
    #
    #     # Set up new sample
    #     external_sample_id = 'NEW_TEST123'
    #
    #     status = 'uploaded'
    #
    #     # Call new endpoint to update sequence status and meta
    #     meta = {'batch': 2}
    #     sequence_id = await self.seql.upsert_sequence_from_external_id_and_type(
    #         external_sample_id=external_sample_id,
    #         sequence_type=self.sequence_one.type,
    #         status=SequenceStatus(status),
    #         meta=meta,
    #         sample_type=SampleType('blood'),
    #     )
    #
    #     # validate new status and meta
    #     sequence = await self.seql.get_sequence_by_id(
    #         sequence_id, check_project_id=False
    #     )
    #     self.assertEqual(status, sequence.status.value)
    #     self.assertEqual(meta, sequence.meta)
    #
    # @run_as_sync
    # async def test_get_sequences_with_filters(self):
    #     """Testing selecting sequence with filters"""
    #
    #     # Filter by direct sequence ID
    #     results = await self.seqlayer.get_sequences_by(sequence_ids=[self.seq1, self.seq2])
    #     results.sort(key=attrgetter('id'))
    #     self.assertEqual(results, self.all_sequences)
    #
    #     # Filter by samples and sequence meta
    #     results_batch_one = await self.seql.get_sequences_by(
    #         sample_ids=[self.raw_cpg_id], seq_meta=self.sequence_one.meta
    #     )
    #     results_batch_two = await self.seql.get_sequences_by(
    #         sample_ids=[self.raw_cpg_id], seq_meta=self.sequence_two.meta
    #     )
    #     self.assertEqual(results_batch_one, [self.sequence_one])
    #     self.assertEqual(results_batch_two, [self.sequence_two])
    #
    #     # Filter by project & type
    #     results_exomes = await self.seql.get_sequences_by(
    #         project_ids=[self.project_id], types=[self.sequence_two.type.value]
    #     )
    #     self.assertEqual(results_exomes, [self.sequence_two])
    #
    #     results_sample_filter = await self.seql.get_sequences_by(
    #         project_ids=[self.project_id], sample_meta={'Testing': 'test_sequence'}
    #     )
    #     results_sample_filter.sort(key=attrgetter('id'))
    #     self.assertEqual(results_sample_filter, self.all_sequences)
    #
    # @run_as_sync
    # async def test_get_sequences_with_filters_empty(self):
    #     """Testing select sequence with criteria that will not return
    #     any sequences"""
    #
    #     # Testing statuses
    #     results_statuses = await self.seql.get_sequences_by(
    #         project_ids=[self.project_id], statuses=['failed-qc', 'sent-to-sequencing']
    #     )
    #     self.assertFalse(results_statuses)
    #
    #     # Testing samples that don't exist
    #     invalid_samples = ['INVALID_SAMPLE', 'INVALID_SAMPLE_2']
    #     results_invalid_samples = await self.seql.get_sequences_by(
    #         project_ids=[self.project_id], sample_ids=invalid_samples
    #     )
    #     self.assertFalse(results_invalid_samples)
