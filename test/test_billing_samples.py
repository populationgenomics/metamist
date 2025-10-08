from unittest import mock
from test.testbase import DbIsolatedTest, run_as_sync

from db.python.tables.sample import SampleTable

def mock_fetch_all_sample_count(*_args, **_kwargs):
    """
    This is a mockup function for fetch_all function on the Database object.
    This returns one mockup of a query result that contains the the number of samples
    in a project for each month.
    """
    # mockup BQ query result topic cost by invoice month, return row iterator
    mock_rows = mock.AsyncMock(args={}, spec=['__iter__', '__next__'])
    mock_rows.total_rows = 3
    mock_rows.__iter__.return_value = [
        {'project': 'ourdna', 'year': '2025', 'month': 3, 'count': 100},
        {'project': 'ourdna', 'year': '2025', 'month': 5, 'count': 150},
        {'project': 'ourdna', 'year': '2025', 'month': 8, 'count': 170},
    ]
    
    return mock_rows


class TestSample(DbIsolatedTest):
    """Test sample class"""

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()

        self.sample_table = SampleTable(self.connection)


    @run_as_sync
    async def test_monthly_samples_count_per_project(self):
        self.sample_table.connection.fetch_all = mock.AsyncMock(side_effect=mock_fetch_all_sample_count)
        ret = await self.sample_table.get_monthly_samples_count_per_project([1, 2, 3])

        print(ret)
        assert ret