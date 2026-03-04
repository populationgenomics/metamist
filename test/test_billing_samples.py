import datetime

import pytest

from db.python.connect import Connection
from db.python.tables.sample import SampleTable


def get_mock_date(date_to_mock: datetime.date):
    """
    Patches the return value of date.today()
    Additionally, the fromisoformat of the date class is configured to function as it would without mocking.
    """

    class MockDate(datetime.date):
        @classmethod
        def today(cls):
            return date_to_mock

    return MockDate


@pytest.fixture
def mock_fetchall(monkeypatch):
    """
    This is a mockup function for fetch_all function on the Database object.
    This returns one mockup of a query result that contains the the number of samples
    in a project for each month.
    """

    def _mock_fetchall(connection, fetch_all_return: list[dict[str, int]]):
        class MockCursor:
            async def fetchall(self):
                return fetch_all_return

        async def mock_execute(*args):
            return MockCursor()

        monkeypatch.setattr(connection.pg_connection, 'execute', mock_execute)

    return _mock_fetchall


class TestMonthlySamplesPerProject:
    """Tests for retrieving the accumulated samples per month."""

    @pytest.fixture(autouse=True)
    def set_up(self, connection_with_project: Connection):
        self.connection = connection_with_project
        self.sample_table = SampleTable(self.connection)

    @pytest.mark.asyncio
    async def test_standard_date_gaps(self, mock_fetchall, monkeypatch):
        """Tests the case wherein there is multiple dates in the database, separated by a few months."""

        monkeypatch.setattr(
            'db.python.tables.sample.date', get_mock_date(datetime.date(2025, 9, 1))
        )

        # Set up sample count mocking.
        mock_fetchall(
            self.connection,
            [
                {'project': 0, 'year': 2025, 'month': 2, 'count': 100},
                {'project': 0, 'year': 2025, 'month': 4, 'count': 150},
                {'project': 0, 'year': 2025, 'month': 7, 'count': 170},
            ],
        )

        result = await self.sample_table.get_monthly_samples_count_per_project()
        month_costs = result[0]

        assert month_costs == {
            datetime.date(year=2025, month=2, day=1): 100,
            datetime.date(year=2025, month=3, day=1): 100,
            datetime.date(year=2025, month=4, day=1): 250,
            datetime.date(year=2025, month=5, day=1): 250,
            datetime.date(year=2025, month=6, day=1): 250,
            datetime.date(year=2025, month=7, day=1): 420,
            datetime.date(year=2025, month=8, day=1): 420,
            datetime.date(year=2025, month=9, day=1): 420,
        }

    @pytest.mark.asyncio
    async def test_only_this_month(self, monkeypatch, mock_fetchall):

        monkeypatch.setattr(
            'db.python.tables.sample.date', get_mock_date(datetime.date(2025, 9, 1))
        )
        mock_fetchall(
            self.connection,
            [
                {'project': 0, 'year': 2025, 'month': 9, 'count': 170},
            ],
        )

        result = await self.sample_table.get_monthly_samples_count_per_project()
        month_costs = result[0]
        assert month_costs == {datetime.date(year=2025, month=9, day=1): 170}
