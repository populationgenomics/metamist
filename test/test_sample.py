from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.sample import SampleLayer
from models.models.sample import SampleUpsertInternal


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
                external_id='Test01',
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

    @run_as_sync
    async def test_get_sample(self):
        """Test getting formed sample"""
        meta_dict = {'meta': 'meta ;)'}
        s = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_id='Test01',
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
                external_id='Test01',
                type='blood',
                active=True,
                meta=meta_dict,
            )
        )

        new_external_id = 'Test02'
        await self.slayer.upsert_sample(
            SampleUpsertInternal(id=s.id, external_id=new_external_id)
        )

        sample = await self.slayer.get_by_id(s.id, check_project_id=False)

        self.assertEqual(new_external_id, sample.external_id)

    # @run_as_sync
    # async def test_update_sample(self):
    #     """Test to see what's in the database"""
    #     print('Running test 2')
    #     print(await self.connection.connection.fetch_all('SELECT * FROM sample'))
