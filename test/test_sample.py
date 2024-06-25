import unittest
from test.testbase import DbIsolatedTest, run_as_sync

from db.python.filters.generic import GenericFilter
from db.python.filters.sample import SampleFilter
from db.python.layers.sample import SampleLayer
from models.models import PRIMARY_EXTERNAL_ORG, SampleUpsertInternal


class TestSample(DbIsolatedTest):
    """Test sample class"""

    # tests run in 'sorted by ascii' order
    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()

        self.slayer = SampleLayer(self.connection)

    @run_as_sync
    async def test_add_sample(self):
        """Test inserting a sample"""
        sample = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta={'meta': 'meta ;)'},
            )
        )

        samples = await self.connection.connection.fetch_all(
            'SELECT id, type, meta, project FROM sample'
        )
        self.assertEqual(1, len(samples))
        self.assertEqual(sample.id, samples[0]['id'])

        mapping = await self.slayer.get_sample_id_map_by_external_ids(['Test01'])
        self.assertDictEqual({'Test01': sample.id}, mapping)

    @run_as_sync
    async def test_get_sample(self):
        """Test getting formed sample"""
        meta_dict = {'meta': 'meta ;)'}
        s = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta=meta_dict,
            )
        )

        sample = await self.slayer.get_by_id(s.id, check_project_id=False)

        self.assertEqual('blood', sample.type)
        self.assertDictEqual(meta_dict, sample.meta)

    @run_as_sync
    async def test_query_sample_by_eid(self):
        """Test querying samples by an external ID, and check it's returned"""
        meta_dict = {'meta': 'meta ;)'}
        ex_ids = {PRIMARY_EXTERNAL_ORG: 'Test01', 'external_org': 'ex01'}
        s = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids=ex_ids,
                type='blood',
                active=True,
                meta=meta_dict,
            )
        )

        samples = await self.slayer.query(
            SampleFilter(external_id=GenericFilter(eq='Test01'))
        )
        self.assertEqual(1, len(samples))
        self.assertEqual(s.id, samples[0].id)
        self.assertDictEqual(ex_ids, samples[0].external_ids)

        samples = await self.slayer.query(
            SampleFilter(external_id=GenericFilter(eq='ex01'))
        )
        self.assertEqual(1, len(samples))
        self.assertEqual(s.id, samples[0].id)
        self.assertDictEqual(ex_ids, samples[0].external_ids)

        samples = await self.slayer.query(
            SampleFilter(external_id=GenericFilter(eq='ex02'))
        )
        self.assertEqual(0, len(samples))

    @run_as_sync
    async def test_update_sample(self):
        """Test updating a sample"""
        meta_dict = {'meta': 'meta ;)'}
        s = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta=meta_dict,
            )
        )

        new_external_id_dict = {PRIMARY_EXTERNAL_ORG: 'Test02'}
        await self.slayer.upsert_sample(
            SampleUpsertInternal(id=s.id, external_ids=new_external_id_dict)
        )

        sample = await self.slayer.get_by_id(s.id, check_project_id=False)

        self.assertDictEqual(new_external_id_dict, sample.external_ids)

    @run_as_sync
    async def test_nested_samples_and_query(self):
        """
        Test inserting a sample with nested samples and querying them
        """
        nested_sample = SampleUpsertInternal(
            external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
            type='blood',
            nested_samples=[
                SampleUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'Test02'},
                    type='blood',
                    nested_samples=[
                        SampleUpsertInternal(
                            external_ids={PRIMARY_EXTERNAL_ORG: 'Test03'},
                            type='blood',
                        )
                    ],
                )
            ],
        )

        inserted = (await self.slayer.upsert_samples([nested_sample]))[0]
        first_child = inserted.nested_samples[0]
        children_id = {first_child.id, first_child.nested_samples[0].id}

        # get all
        all_samples = await self.slayer.query(
            SampleFilter(project=GenericFilter(eq=self.project_id))
        )
        self.assertEqual(3, len(all_samples))

        # get only the root
        root_samples = await self.slayer.query(
            SampleFilter(
                project=GenericFilter(eq=self.project_id),
                sample_root_id=GenericFilter(isnull=True),
            )
        )
        self.assertEqual(1, len(root_samples))
        self.assertEqual(root_samples[0].id, inserted.id)
        parentless_samples = await self.slayer.query(
            SampleFilter(
                project=GenericFilter(eq=self.project_id),
                sample_parent_id=GenericFilter(isnull=True),
            )
        )
        self.assertEqual(1, len(parentless_samples))
        self.assertEqual(parentless_samples[0].id, inserted.id)

        # get all children
        children = await self.slayer.query(
            SampleFilter(
                project=GenericFilter(eq=self.project_id),
                sample_root_id=GenericFilter(eq=inserted.id),
            )
        )
        self.assertEqual(2, len(children))
        self.assertSetEqual(children_id, {c.id for c in children})

        # get only first child
        first_child_res = await self.slayer.query(
            SampleFilter(
                project=GenericFilter(eq=self.project_id),
                sample_parent_id=GenericFilter(eq=inserted.id),
            )
        )
        self.assertEqual(1, len(first_child_res))
        self.assertEqual(first_child.id, first_child_res[0].id)


class TestSampleUnwrapping(unittest.TestCase):
    """Test unwrapping nested samples into an ordered list of rows"""

    def test_nested_sample_unwrapping_basic(self):
        """Basic case, one level, a few sub-samples"""
        sample = SampleUpsertInternal(
            id=1,
            nested_samples=[
                SampleUpsertInternal(id=2),
                SampleUpsertInternal(id=3),
                SampleUpsertInternal(id=4),
            ],
        )

        unwrapped = SampleLayer.unwrap_nested_samples([sample])

        self.assertEqual(4, len(unwrapped))

        first_row = unwrapped[0]
        self.assertTupleEqual(
            (None, None, 1),
            (first_row.root, first_row.parent, first_row.sample.id),
        )

        last_row = unwrapped[-1]
        self.assertTupleEqual(
            (1, 1, 4),
            (last_row.root.id, last_row.parent.id, last_row.sample.id),
        )

    def test_nested_sample_unwrapping_many_layers(self):
        """
        Multiple levels, but less than the max depth
        I wrote this explicitly so there couldn't be an issue in a codegen
        """
        sample = SampleUpsertInternal(
            id=1,
            nested_samples=[
                SampleUpsertInternal(
                    id=2,
                    nested_samples=[
                        SampleUpsertInternal(
                            id=3,
                            nested_samples=[
                                SampleUpsertInternal(
                                    id=4,
                                    nested_samples=[
                                        SampleUpsertInternal(
                                            id=5,
                                            nested_samples=[
                                                SampleUpsertInternal(
                                                    id=6,
                                                    nested_samples=[
                                                        SampleUpsertInternal(id=7)
                                                    ],
                                                )
                                            ],
                                        )
                                    ],
                                )
                            ],
                        ),
                    ],
                ),
            ],
        )

        unwrapped = SampleLayer.unwrap_nested_samples([sample])

        self.assertEqual(7, len(unwrapped))

        first_row = unwrapped[0]
        self.assertTupleEqual(
            (None, None, 1),
            (first_row.root, first_row.parent, first_row.sample.id),
        )

        last_row = unwrapped[-1]
        self.assertTupleEqual(
            (1, 6, 7),
            (last_row.root.id, last_row.parent.id, last_row.sample.id),
        )

    def test_nested_sample_unwrapping_overflow(self):
        """
        Test something way too deep, so that the depth protection is triggered
        """
        # at least 20 layers
        root = SampleUpsertInternal(id=1)

        prev = root
        for i in range(2, 21):
            new_sample = SampleUpsertInternal(id=i)
            prev.nested_samples = [new_sample]
            prev = new_sample

        with self.assertRaises(SampleLayer.SampleUnwrapMaxDepthError):
            SampleLayer.unwrap_nested_samples([root])
