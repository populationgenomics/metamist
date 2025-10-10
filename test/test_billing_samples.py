from unittest import mock
from test.testbase import DbIsolatedTest, run_as_sync
from datetime import date

from db.python.tables.sample import SampleTable

def mock_fetch_all_sample_count(fetch_all_return: list[dict[str, int]]):
    """
    This is a mockup function for fetch_all function on the Database object.
    This returns one mockup of a query result that contains the the number of samples
    in a project for each month.
    """
    def sample_count_mock(*_args, **_kwargs):
        mock_rows = mock.AsyncMock(args={}, spec=['__iter__', '__next__'])
        mock_rows.total_rows = len(fetch_all_return)
        mock_rows.__iter__.return_value = fetch_all_return

        return mock_rows
    
    return sample_count_mock


class TestMonthlySamplesPerProject(DbIsolatedTest):
    """Test sample class"""

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()

        self.sample_table = SampleTable(self.connection)

        # Mock calls to date.today() for all tests.
        self.patcher = mock.patch('db.python.tables.sample.date')
        mock_date = self.patcher.start()
        mock_date.today.return_value = date(year=2025, month=9, day=1)
        mock_date.fromisoformat.side_effect = lambda *args, **kw: date.fromisoformat(*args, **kw) # https://docs.python.org/3/library/unittest.mock-examples.html#partial-mocking

    @run_as_sync
    async def tearDown(self):
        super().tearDown()
        self.patcher.stop()

    @run_as_sync
    async def test_basic(self):
        # Set up sample count mocking.
        fetch_all_mock = mock_fetch_all_sample_count([
            {'project': 0, 'year': 2025, 'month': 2, 'count': 100},
            {'project': 0, 'year': 2025, 'month': 4, 'count': 150},
            {'project': 0, 'year': 2025, 'month': 7, 'count': 170},
        ])
        self.sample_table.connection.fetch_all = mock.AsyncMock(side_effect=fetch_all_mock)
        result = await self.sample_table.get_monthly_samples_count_per_project()

        month_costs = result[0] # Retrieve the cost/month for the given test project ID.
        self.assertDictEqual(month_costs,
            {
                date(year=2025, month=2, day=1): 100,
                date(year=2025, month=3, day=1): 100,
                date(year=2025, month=4, day=1): 250,
                date(year=2025, month=5, day=1): 250,
                date(year=2025, month=6, day=1): 250,
                date(year=2025, month=7, day=1): 420,
                date(year=2025, month=8, day=1): 420,
                date(year=2025, month=9, day=1): 420,
            })
        
    @run_as_sync
    async def test_only_this_month(self):
        # Set up sample count mocking.
        fetch_all_mock = mock_fetch_all_sample_count([
            {'project': 0, 'year': 2025, 'month': 9, 'count': 170},
        ])
        self.sample_table.connection.fetch_all = mock.AsyncMock(side_effect=fetch_all_mock)
        result = await self.sample_table.get_monthly_samples_count_per_project()

        month_costs = result[0] # Retrieve the cost/month for the given test project ID.
        self.assertDictEqual(month_costs, {date(year=2025, month=9, day=1): 170})