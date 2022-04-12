from test.testbase import DbIsolatedTest, run_test_as_sync

from db.python.layers.sample import SampleLayer, SampleType


class TestSample(DbIsolatedTest):
    """Test sample class"""

    # tests run in 'sorted by ascii' order

    @run_test_as_sync
    async def test_add_sample(self):
        """Test inserting a sample"""
        sl = SampleLayer(self.connection)
        s = await sl.insert_sample(
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

    @run_test_as_sync
    async def test_update_sample(self):
        """Test to see what's in the database"""
        print('Running test 2')
        print(await self.connection.connection.fetch_all('SELECT * FROM sample'))
