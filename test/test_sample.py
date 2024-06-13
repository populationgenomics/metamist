from test.testbase import DbIsolatedTest, run_as_sync

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

    # @run_as_sync
    # async def test_update_sample(self):
    #     """Test to see what's in the database"""
    #     print('Running test 2')
    #     print(await self.connection.connection.fetch_all('SELECT * FROM sample'))
