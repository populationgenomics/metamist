from test.testbase import DbIsolatedTest, run_as_sync
from pymysql.err import IntegrityError


from db.python.connect import NotFoundError
from db.python.layers.sample import SampleLayer
from db.python.layers.sequence import SampleSequenceLayer, SequenceType, SequenceStatus
from models.models.sequence import SampleSequencing
from models.enums import SampleType


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

    @run_as_sync
    async def test_not_found_sequence(self):
        """
        Test the NotFoundError when getting an invalid sequence ID
        """

        @run_as_sync
        async def get():
            return await self.seqlayer.get_sequence_by_id(999, check_project_id=False)

        self.assertRaises(NotFoundError, get)

    @run_as_sync
    async def test_insert_sequence(self):
        """
        Test inserting a sequence, and check all values are inserted correctly
        """
        external_ids = {'default': 'SEQ01', 'collaborator2': 'CBSEQ_1'}
        meta = {'1': 1, 'nested': {'nested': 'dict'}, 'alpha': ['b', 'e', 't']}
        seq_id = await self.seqlayer.insert_sequencing(
            sample_id=self.sample_id_raw,
            sequence_type=SequenceType.GENOME,
            status=SequenceStatus.UPLOADED,
            sequence_meta=meta,
            external_ids=external_ids,
        )

        sequence = await self.seqlayer.get_sequence_by_id(
            sequence_id=seq_id, check_project_id=False
        )

        self.assertEqual(seq_id, sequence.id)
        self.assertEqual(self.sample_id_raw, int(sequence.sample_id))
        self.assertEqual('genome', sequence.type.value)
        self.assertEqual('uploaded', sequence.status.value)
        self.assertDictEqual(external_ids, sequence.external_ids)
        self.assertDictEqual(meta, sequence.meta)

    @run_as_sync
    async def test_insert_sequences_for_each_type(self):
        """
        Test inserting a sequence, and check all values are inserted correctly
        """
        meta = {'1': 1, 'nested': {'nested': 'dict'}, 'alpha': ['b', 'e', 't']}
        seq_ids = await self.seqlayer.insert_many_sequencing(
            [
                SampleSequencing(
                    external_ids={},
                    sample_id=self.sample_id_raw,
                    type=stype,
                    meta=meta,
                    status=SequenceStatus.COMPLETED_SEQUENCING,
                )
                for stype in SequenceType
            ]
        )

        inserted_types_rows = await self.connection.connection.fetch_all(
            'SELECT type FROM sample_sequencing WHERE id in :ids', {'ids': seq_ids}
        )
        inserted_types = set(r['type'] for r in inserted_types_rows)

        self.assertEqual(len(SequenceType), len(seq_ids))
        self.assertSetEqual(set(t.value for t in SequenceType), inserted_types)

    @run_as_sync
    async def test_clashing_external_ids(self):
        """Test that should fail when 2nd sequence is inserted with same external_id"""
        external_ids = {'default': 'clashing'}
        await self.seqlayer.insert_sequencing(
            sample_id=self.sample_id_raw,
            sequence_type=SequenceType.GENOME,
            status=SequenceStatus.UPLOADED,
            sequence_meta={},
            external_ids=external_ids,
        )

        @run_as_sync
        async def _insert_failing_sequence():
            return await self.seqlayer.insert_sequencing(
                sample_id=self.sample_id_raw,
                sequence_type=SequenceType.GENOME,
                status=SequenceStatus.UPLOADED,
                sequence_meta={},
                external_ids=external_ids,
            )

        _n_sequences_query = 'SELECT COUNT(*) from sample_sequencing'
        self.assertEqual(
            1, await self.connection.connection.fetch_val(_n_sequences_query)
        )
        self.assertRaises(IntegrityError, _insert_failing_sequence)
        # make sure the transaction unwinds the insert second sequence if the external_id clashes
        self.assertEqual(
            1, await self.connection.connection.fetch_val(_n_sequences_query)
        )

    @run_as_sync
    async def test_insert_clashing_external_ids_multiple(self):
        """
        Test inserting a sequence, and check all values are inserted correctly
        """
        external_ids = {'default': 'clashing'}

        @run_as_sync
        async def _insert_clashing():
            return await self.seqlayer.insert_many_sequencing(
                [
                    SampleSequencing(
                        # both get the same external_ids
                        external_ids=external_ids,
                        sample_id=self.sample_id_raw,
                        type=SequenceType.EXOME,
                        meta={},
                        status=SequenceStatus.COMPLETED_SEQUENCING,
                    )
                    for _ in range(2)
                ]
            )

        _n_sequences_query = 'SELECT COUNT(*) from sample_sequencing'

        self.assertEqual(
            0, await self.connection.connection.fetch_val(_n_sequences_query)
        )
        self.assertRaises(IntegrityError, _insert_clashing)
        self.assertEqual(
            0, await self.connection.connection.fetch_val(_n_sequences_query)
        )

    @run_as_sync
    async def test_getting_sequence_by_external_id(self):
        """
        Test get differences sequences by multiple IDs
        """
        seq1_id = await self.seqlayer.insert_sequencing(
            sample_id=self.sample_id_raw,
            sequence_type=SequenceType.GENOME,
            status=SequenceStatus.UPLOADED,
            sequence_meta={},
            external_ids={'default': 'SEQ01', 'other': 'EXT_SEQ1'},
        )
        seq2_id = await self.seqlayer.insert_sequencing(
            sample_id=self.sample_id_raw,
            sequence_type=SequenceType.EXOME,
            status=SequenceStatus.UPLOADED,
            sequence_meta={},
            external_ids={'default': 'SEQ02'},
        )

        self.assertEqual(
            seq1_id, (await self.seqlayer.get_sequence_by_external_id('SEQ01')).id
        )
        self.assertEqual(
            seq1_id, (await self.seqlayer.get_sequence_by_external_id('EXT_SEQ1')).id
        )
        seq2 = await self.seqlayer.get_sequence_by_external_id('SEQ02')
        self.assertEqual(seq2_id, seq2.id)

    @run_as_sync
    async def test_get_sequences_by_sample_id(self):
        """Get many sequences by sample ID"""
        seq1_id = await self.seqlayer.insert_sequencing(
            sample_id=self.sample_id_raw,
            sequence_type=SequenceType.GENOME,
            status=SequenceStatus.UPLOADED,
            sequence_meta={},
            external_ids={},
        )
        seq2_id = await self.seqlayer.insert_sequencing(
            sample_id=self.sample_id_raw,
            sequence_type=SequenceType.EXOME,
            status=SequenceStatus.UPLOADED,
            sequence_meta={},
            external_ids={},
        )

        by_type = await self.seqlayer.get_sequence_ids_for_sample_ids_by_type(
            [self.sample_id_raw], check_project_ids=False
        )

        self.assertIn(self.sample_id_raw, by_type)
        self.assertEqual(1, len(by_type))

        self.assertListEqual(
            [seq1_id], by_type[self.sample_id_raw][SequenceType.GENOME]
        )
        self.assertListEqual([seq2_id], by_type[self.sample_id_raw][SequenceType.EXOME])

    @run_as_sync
    async def test_query(self):
        """Test query_sequences in different combinations"""
        sample_id_for_test = await self.slayer.insert_sample(
            'SAM_TEST_QUERY',
            SampleType.BLOOD,
            active=True,
            meta={'collection-year': '2022'},
        )

        seq1_id = await self.seqlayer.insert_sequencing(
            sample_id=sample_id_for_test,
            sequence_type=SequenceType.GENOME,
            status=SequenceStatus.UPLOADED,
            sequence_meta={'unique': 'a', 'common': 'common'},
            external_ids={'default': 'SEQ01'},
        )
        seq2_id = await self.seqlayer.insert_sequencing(
            sample_id=sample_id_for_test,
            sequence_type=SequenceType.EXOME,
            status=SequenceStatus.RECEIVED,
            sequence_meta={'unique': 'b', 'common': 'common'},
            external_ids={'default': 'SEQ02'},
        )

        async def search_result_to_ids(**query):
            seqs = await self.seqlayer.get_sequences_by(
                **query, project_ids=[self.project_id]
            )
            return {s.id for s in seqs}

        # sample_ids
        self.assertSetEqual(
            {seq1_id, seq2_id},
            await search_result_to_ids(sample_ids=[sample_id_for_test]),
        )
        self.assertSetEqual(set(), await search_result_to_ids(sample_ids=[9_999_999]))

        # external sequence IDs
        self.assertSetEqual(
            {seq1_id}, await search_result_to_ids(external_sequence_ids=['SEQ01'])
        )
        self.assertSetEqual(
            {seq1_id, seq2_id},
            await search_result_to_ids(external_sequence_ids=['SEQ01', 'SEQ02']),
        )

        # seq_meta
        self.assertSetEqual(
            {seq2_id}, await search_result_to_ids(seq_meta={'unique': 'b'})
        )
        self.assertSetEqual(
            {seq1_id, seq2_id},
            await search_result_to_ids(seq_meta={'common': 'common'}),
        )

        # sample meta
        self.assertSetEqual(
            {seq1_id, seq2_id},
            await search_result_to_ids(sample_meta={'collection-year': '2022'}),
        )
        self.assertSetEqual(
            set(), await search_result_to_ids(sample_meta={'unknown_key': '2022'})
        )

        # sequence types
        self.assertSetEqual(
            {seq1_id}, await search_result_to_ids(types=[SequenceType.GENOME])
        )
        self.assertSetEqual(
            {seq2_id}, await search_result_to_ids(types=[SequenceType.EXOME])
        )
        self.assertSetEqual(
            {seq1_id, seq2_id},
            await search_result_to_ids(types=[SequenceType.GENOME, SequenceType.EXOME]),
        )

        # combination
        self.assertSetEqual(
            {seq2_id},
            await search_result_to_ids(
                sample_meta={'collection-year': '2022'}, external_sequence_ids=['SEQ02']
            ),
        )
        self.assertSetEqual(
            {seq1_id},
            await search_result_to_ids(
                external_sequence_ids=['SEQ01'], types=[SequenceType.GENOME]
            ),
        )
        self.assertSetEqual(
            set(),
            await search_result_to_ids(
                external_sequence_ids=['SEQ01'], types=[SequenceType.EXOME]
            ),
        )

    @run_as_sync
    async def test_update(self):
        """Test updating a sequence, and all fields are updated correctly"""
        seq_id = await self.seqlayer.insert_sequencing(
            sample_id=self.sample_id_raw,
            sequence_type=SequenceType.GENOME,
            status=SequenceStatus.RECEIVED,
            sequence_meta={'a': 1, 'b': 2},
            external_ids={
                'default': 'SEQ01',
                'untouched': 'UTC+1',
                'to_delete': 'VALUE',
            },
        )

        await self.seqlayer.update_sequence(
            seq_id,
            external_ids={'default': 'NSQ_01', 'ext': 'EXTSEQ01', 'to_delete': None},
            status=SequenceStatus.UPLOADED,
            meta={'a': 2, 'c': True},
        )

        update_sequence = await self.seqlayer.get_sequence_by_id(
            sequence_id=seq_id, check_project_id=False
        )

        self.assertEqual(seq_id, update_sequence.id)
        self.assertEqual(self.sample_id_raw, int(update_sequence.sample_id))
        self.assertEqual('genome', update_sequence.type.value)
        self.assertEqual('uploaded', update_sequence.status.value)
        self.assertDictEqual(
            {'default': 'NSQ_01', 'ext': 'EXTSEQ01', 'untouched': 'UTC+1'},
            update_sequence.external_ids,
        )
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
            status_to_check = await self.connection.connection.fetch_one(
                'SELECT status FROM sample_sequencing WHERE id = :id', {'id': seq_id}
            )
            self.assertEqual(status.value, status_to_check['status'])
