from test.testbase import DbIsolatedTest, run_as_sync

from models.models.sample import sample_id_format
from db.python.layers.sample import SampleLayer, SampleType


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
        s = await self.slayer.insert_sample(
            'Test01',
            SampleType.BLOOD,
            active=True,
            meta={'meta': 'meta ;)'},
        )

        samples = await self.connection.connection.fetch_all(
            'SELECT id, type, meta, project FROM sample'
        )
        self.assertEqual(1, len(samples))
        s = samples[0]
        self.assertEqual(1, s['id'])

    @run_as_sync
    async def test_get_sample(self):
        """Test getting formed sample"""
        meta_dict = {'meta': 'meta ;)'}
        s = await self.slayer.insert_sample(
            'Test01',
            SampleType.BLOOD,
            active=True,
            meta=meta_dict,
        )

        sample = await self.slayer.get_by_id(s, check_project_id=False)

        self.assertEqual(sample_id_format(s), sample.id)
        self.assertEqual(SampleType.BLOOD, sample.type)
        self.assertDictEqual(meta_dict, sample.meta)

    # @run_as_sync
    # async def test_update_sample(self):
    #     """Test to see what's in the database"""
    #     print('Running test 2')
    #     print(await self.connection.connection.fetch_all('SELECT * FROM sample'))
