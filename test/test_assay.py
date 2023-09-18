from test.testbase import DbIsolatedTest, run_as_sync

from pymysql.err import IntegrityError

from db.python.connect import NotFoundError
from db.python.enum_tables import AssayTypeTable
from db.python.layers.assay import AssayLayer
from db.python.layers.sample import SampleLayer
from db.python.tables.assay import AssayFilter
from db.python.utils import GenericFilter
from models.models.assay import AssayUpsertInternal
from models.models.sample import SampleUpsertInternal

default_sequencing_meta = {
    'sequencing_type': 'genome',
    'sequencing_platform': 'short-read',
    'sequencing_technology': 'illumina',
}


class TestAssay(DbIsolatedTest):
    """Test assay class"""

    # pylint: disable=too-many-instance-attributes

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()

        self.slayer = SampleLayer(self.connection)
        self.assaylayer = AssayLayer(self.connection)
        self.external_sample_id = 'TESTING001'

        # Create new sample
        self.sample_id_raw = (
            await self.slayer.upsert_sample(
                SampleUpsertInternal(
                    external_id=self.external_sample_id,
                    type='blood',
                    active=True,
                    meta={'Testing': 'test_assay'},
                )
            )
        ).id

        at = AssayTypeTable(self.connection)
        await at.insert('metabolomics')

    @run_as_sync
    async def test_not_found_assay(self):
        """
        Test the NotFoundError when getting an invalid assay ID
        """

        @run_as_sync
        async def get():
            return await self.assaylayer.get_assay_by_id(-1, check_project_id=False)

        self.assertRaises(NotFoundError, get)

    @run_as_sync
    async def test_upsert_assay(self):
        """
        Test inserting a assay, and check all values are inserted correctly
        """
        external_ids = {'default': 'SEQ01', 'collaborator2': 'CBSEQ_1'}
        meta = {
            '1': 1,
            'nested': {'nested': 'dict'},
            'alpha': ['b', 'e', 't'],
            **default_sequencing_meta,
        }
        upserted_assay = await self.assaylayer.upsert_assay(
            AssayUpsertInternal(
                sample_id=self.sample_id_raw,
                type='sequencing',
                meta=meta,
                external_ids=external_ids,
            )
        )

        assay = await self.assaylayer.get_assay_by_id(
            assay_id=upserted_assay.id, check_project_id=False
        )

        self.assertEqual(upserted_assay.id, assay.id)
        self.assertEqual(self.sample_id_raw, int(assay.sample_id))
        self.assertEqual('sequencing', assay.type)
        self.assertDictEqual(external_ids, assay.external_ids)
        self.assertDictEqual(meta, assay.meta)

    @run_as_sync
    async def test_insert_assays_for_each_type(self):
        """
        Test inserting a assay, and check all values are inserted correctly
        """
        meta = {
            '1': 1,
            'nested': {'nested': 'dict'},
            'alpha': ['b', 'e', 't'],
            **default_sequencing_meta,
        }
        sequencing_types = ['sequencing', 'metabolomics']
        assays = await self.assaylayer.upsert_assays(
            [
                AssayUpsertInternal(
                    external_ids={'eid': f'external_id_{_type}'},
                    sample_id=self.sample_id_raw,
                    type='sequencing',
                    meta=meta,
                )
                for _type in sequencing_types
            ]
        )
        assay_ids = [a.id for a in assays]
        inserted_types_rows = await self.connection.connection.fetch_all(
            'SELECT type FROM assay WHERE id in :ids', {'ids': assay_ids}
        )
        inserted_types = set(r['type'] for r in inserted_types_rows)

        self.assertEqual(len(sequencing_types), len(assay_ids))
        self.assertEqual(1, len(inserted_types))

    @run_as_sync
    async def test_clashing_external_ids(self):
        """Test that should fail when 2nd assay is inserted with same external_id"""
        external_ids = {'default': 'clashing'}
        await self.assaylayer.upsert_assay(
            AssayUpsertInternal(
                sample_id=self.sample_id_raw,
                type='sequencing',
                meta={**default_sequencing_meta},
                external_ids=external_ids,
            )
        )

        @run_as_sync
        async def _insert_failing_assay():
            return await self.assaylayer.upsert_assay(
                AssayUpsertInternal(
                    sample_id=self.sample_id_raw,
                    type='sequencing',
                    meta={**default_sequencing_meta},
                    external_ids=external_ids,
                )
            )

        _n_assays_query = 'SELECT COUNT(*) from assay'
        self.assertEqual(1, await self.connection.connection.fetch_val(_n_assays_query))
        self.assertRaises(IntegrityError, _insert_failing_assay)
        # make sure the transaction unwinds the insert second assay if the external_id clashes
        self.assertEqual(1, await self.connection.connection.fetch_val(_n_assays_query))

    @run_as_sync
    async def test_insert_clashing_external_ids_multiple(self):
        """
        Test inserting a assay, and check all values are inserted correctly
        """
        external_ids = {'default': 'clashing'}

        @run_as_sync
        async def _insert_clashing():
            return await self.assaylayer.upsert_assays(
                [
                    AssayUpsertInternal(
                        # both get the same external_ids
                        external_ids=external_ids,
                        sample_id=self.sample_id_raw,
                        type='sequencing',
                        meta={**default_sequencing_meta},
                    )
                    for _ in range(2)
                ]
            )

        _n_assays_query = 'SELECT COUNT(*) from assay'

        self.assertEqual(0, await self.connection.connection.fetch_val(_n_assays_query))
        self.assertRaises(IntegrityError, _insert_clashing)
        self.assertEqual(0, await self.connection.connection.fetch_val(_n_assays_query))

    @run_as_sync
    async def test_getting_assay_by_external_id(self):
        """
        Test get differences assays by multiple IDs
        """
        seq1 = await self.assaylayer.upsert_assay(
            AssayUpsertInternal(
                sample_id=self.sample_id_raw,
                type='sequencing',
                meta={**default_sequencing_meta},
                external_ids={'default': 'SEQ01', 'other': 'EXT_SEQ1'},
            )
        )
        seq2 = await self.assaylayer.upsert_assay(
            AssayUpsertInternal(
                sample_id=self.sample_id_raw,
                type='sequencing',
                meta={**default_sequencing_meta},
                external_ids={'default': 'SEQ02'},
            )
        )

        fquery_1 = AssayFilter(
            external_id=GenericFilter(eq='SEQ01'),
            project=GenericFilter(eq=self.project_id),
        )
        self.assertEqual(seq1.id, (await self.assaylayer.query(fquery_1))[0].id)
        fquery_2 = AssayFilter(
            external_id=GenericFilter(eq='EXT_SEQ1'),
            project=GenericFilter(eq=self.project_id),
        )
        self.assertEqual(seq1.id, (await self.assaylayer.query(fquery_2))[0].id)
        fquery_3 = AssayFilter(
            external_id=GenericFilter(eq='SEQ02'),
            project=GenericFilter(eq=self.project_id),
        )
        self.assertEqual(seq2.id, (await self.assaylayer.query(fquery_3))[0].id)

    @run_as_sync
    async def test_query(self):
        """Test query_assays in different combinations"""
        sample = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_id='SAM_TEST_QUERY',
                type='blood',
                active=True,
                meta={'collection-year': '2022'},
            )
        )

        sample_id_for_test = sample.id

        seqs = await self.assaylayer.upsert_assays(
            [
                AssayUpsertInternal(
                    sample_id=sample_id_for_test,
                    type='sequencing',
                    meta={'unique': 'a', 'common': 'common', **default_sequencing_meta},
                    external_ids={'default': 'SEQ01'},
                ),
                AssayUpsertInternal(
                    sample_id=sample_id_for_test,
                    type='sequencing',
                    meta={'unique': 'b', 'common': 'common', **default_sequencing_meta},
                    external_ids={'default': 'SEQ02'},
                ),
            ]
        )

        async def search_result_to_ids(filter_: AssayFilter):
            filter_.project = GenericFilter(eq=self.project_id)
            seqs = await self.assaylayer.query(filter_)
            return {s.id for s in seqs}

        seq1_id = seqs[0].id
        seq2_id = seqs[1].id

        # sample_ids
        self.assertSetEqual(
            {seq1_id, seq2_id},
            await search_result_to_ids(
                AssayFilter(sample_id=GenericFilter(in_=[sample_id_for_test]))
            ),
        )
        self.assertSetEqual(
            set(),
            await search_result_to_ids(
                AssayFilter(sample_id=GenericFilter(in_=[9_999_999]))
            ),
        )

        # external assay IDs
        self.assertSetEqual(
            {seq1_id},
            await search_result_to_ids(
                AssayFilter(external_id=GenericFilter(eq='SEQ01'))
            ),
        )
        self.assertSetEqual(
            {seq1_id, seq2_id},
            await search_result_to_ids(
                AssayFilter(
                    external_id=GenericFilter(in_=['SEQ01', 'SEQ02']),
                )
            ),
        )

        # seq_meta
        self.assertSetEqual(
            {seq2_id},
            await search_result_to_ids(
                AssayFilter(meta={'unique': GenericFilter(eq='b')})
            ),
        )
        self.assertSetEqual(
            {seq1_id, seq2_id},
            await search_result_to_ids(
                AssayFilter(meta={'common': GenericFilter(eq='common')})
            ),
        )

        # sample meta
        self.assertSetEqual(
            {seq1_id, seq2_id},
            await search_result_to_ids(
                AssayFilter(sample_meta={'collection-year': GenericFilter(eq='2022')})
            ),
        )
        self.assertSetEqual(
            set(),
            await search_result_to_ids(
                AssayFilter(sample_meta={'unknown_key': GenericFilter(eq='2022')})
            ),
        )

        # assay types
        self.assertSetEqual(
            {seq1_id, seq2_id},
            await search_result_to_ids(
                AssayFilter(type=GenericFilter(in_=['sequencing']))
            ),
        )

        # combination
        self.assertSetEqual(
            {seq2_id},
            await search_result_to_ids(
                AssayFilter(
                    sample_meta={'collection-year': GenericFilter(eq='2022')},
                    external_id=GenericFilter(in_=['SEQ02']),
                )
            ),
        )
        self.assertSetEqual(
            {seq1_id},
            await search_result_to_ids(
                AssayFilter(
                    external_id=GenericFilter(in_=['SEQ01']),
                    type=GenericFilter(eq='sequencing'),
                )
            ),
        )
        # self.assertSetEqual(
        #     set(),
        #     await search_result_to_ids(
        #         external_assay_ids=['SEQ01'], types=[SequenceType.EXOME]
        #     ),
        # )

    @run_as_sync
    async def test_update(self):
        """Test updating an assay, and all fields are updated correctly"""
        # insert
        assay = await self.assaylayer.upsert_assay(
            AssayUpsertInternal(
                sample_id=self.sample_id_raw,
                type='sequencing',
                meta={'a': 1, 'b': 2, **default_sequencing_meta},
                external_ids={
                    'default': 'SEQ01',
                    'untouched': 'UTC+1',
                    'to_delete': 'VALUE',
                },
            )
        )

        await self.assaylayer.upsert_assay(
            AssayUpsertInternal(
                id=assay.id,
                sample_id=self.sample_id_raw,
                external_ids={
                    'default': 'NSQ_01',
                    'ext': 'EXTSEQ01',
                    'to_delete': None,
                },
                meta={'a': 2, 'c': True},
            )
        )

        update_assay = await self.assaylayer.get_assay_by_id(
            assay_id=assay.id, check_project_id=False
        )

        self.assertEqual(assay.id, update_assay.id)
        self.assertEqual(self.sample_id_raw, int(update_assay.sample_id))
        self.assertEqual('sequencing', update_assay.type)
        self.assertDictEqual(
            {'default': 'NSQ_01', 'ext': 'EXTSEQ01', 'untouched': 'UTC+1'},
            update_assay.external_ids,
        )
        self.assertDictEqual(
            {'a': 2, 'b': 2, 'c': True, **default_sequencing_meta}, update_assay.meta
        )

    @run_as_sync
    async def test_update_type(self):
        """
        Test update all assay statuses
        """
        assay = await self.assaylayer.upsert_assay(
            AssayUpsertInternal(
                sample_id=self.sample_id_raw,
                type='sequencing',
                meta={**default_sequencing_meta},
                external_ids={},
            )
        )

        # cycle through all statuses, and check that works
        await self.assaylayer.upsert_assay(
            AssayUpsertInternal(id=assay.id, type='metabolomics'),
            check_project_id=False,
        )
        row_to_check = await self.connection.connection.fetch_one(
            'SELECT type FROM assay WHERE id = :id',
            {'id': assay.id},
        )
        self.assertEqual('metabolomics', row_to_check['type'])
