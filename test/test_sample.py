from test.testbase import DbTest, run_as_sync

from db.python.layers.sample import SampleLayer, SampleType


class TestSample(DbTest):
    """Test sample class"""

    # tests run in 'sorted by ascii' order

    @run_as_sync
    async def test_number_1(self):
        """Test inserting a sample"""
        sl = SampleLayer(self.connection)
        s = await sl.insert_sample(
            'Test01',
            SampleType.BLOOD,
            active=True,
            meta={'meta': 'meta ;)'},
            check_project_id=False,
            project=1,
        )
        print(f'Inserted a sample with {s}')

        print(await self.connection.connection.fetch_all('SELECT * FROM sample'))

    @run_as_sync
    async def test_number_2(self):
        """Test to see what's in the database"""
        print('Running test 2')
        print(await self.connection.connection.fetch_all('SELECT * FROM sample'))
