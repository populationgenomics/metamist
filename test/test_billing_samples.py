import datetime

import pytest

from db.python.connect import Connection
from db.python.tables.sample import SampleTable


@pytest.fixture
def mock_fetchall(monkeypatch):
    """
    This is a mockup function for fetchall function on the Database object.
    This returns one mockup of a query result that contains the number of samples
    in a project for each month.
    """

    def _mock_fetchall(connection, fetch_all_return: list[dict[str, int]]):
        class MockCursor:
            async def fetchall(self):
                return fetch_all_return

        async def mock_execute(*_args):
            return MockCursor()

        monkeypatch.setattr(connection.pg_connection, 'execute', mock_execute)

    return _mock_fetchall


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
        month_costs = result[
            0
        ]  # Retrieve the cost/month for the given test project ID.

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
        """Tests the case wherein the only record in the database is the current month."""
        # Set up sample count mocking.

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
        month_costs = result[
            0
        ]  # Retrieve the cost/month for the given test project ID.
        assert month_costs == {datetime.date(year=2025, month=9, day=1): 170}


class TestMonthlySamplesCountQuery:
    """
    Tests sample history query, which counts samples over time, including samples that
    have since been deleted.
    """

    @pytest.fixture(autouse=True)
    def set_up(self, connection_with_project: Connection):
        self.connection = connection_with_project
        self.sample_table = SampleTable(self.connection)

    async def _insert_sample(self, project: int) -> int:
        """Manually insert a sample row and return its id."""
        async with self.connection.pg_connection.cursor() as cur:
            await cur.execute(
                t"""
                INSERT INTO sample (project, type, active)
                VALUES ({project}, 'blood', true)
                RETURNING id
                """
            )
            row = await cur.fetchone()
            assert row is not None
            return row['id']

    async def _counts_this_month(self) -> dict[int, int]:
        """count samples by project id for current month"""
        this_month = datetime.date.today().replace(day=1)
        result = await self.sample_table.get_monthly_samples_count_per_project()
        return {proj: months[this_month] for proj, months in result.items()}

    @pytest.mark.asyncio
    async def test_deleted_sample_still_counted(self):
        """A deleted sample remains in sample_history and is still counted."""
        project = self.connection.project_id
        assert project is not None
        keep = await self._insert_sample(project)
        merged_away = await self._insert_sample(project)

        async with self.connection.pg_connection.cursor() as cur:
            await cur.execute(t'DELETE FROM sample WHERE id = {merged_away}')
            # the versioning trigger should have moved it to history, not dropped it
            await cur.execute(
                t'SELECT count(*) AS n FROM sample_history WHERE id = {merged_away}'
            )
            row = await cur.fetchone()
            assert row is not None and row['n'] == 1

        async with self.connection.pg_connection.cursor() as cur:
            await cur.execute(t'SELECT count(*) AS n FROM sample WHERE id = {keep}')
            row = await cur.fetchone()
            assert row is not None and row['n'] == 1

        assert (await self._counts_this_month())[project] == 2

    @pytest.mark.asyncio
    async def test_sample_that_moved_project_counts_in_both(self):
        """
        If a sample changes project, it is counted in both. Not sure whether this
        is correct or not but it mirrors the current functionality in mariadb."""
        project_1 = self.connection.project_id
        assert project_1 is not None
        sample = await self._insert_sample(project_1)

        async with self.connection.pg_connection.cursor() as cur:
            await cur.execute("""
                INSERT INTO project (name, dataset, meta)
                VALUES ('test-project-2', 'test-dataset-2', '{}')
                RETURNING id
            """)
            row = await cur.fetchone()
            assert row is not None
            project_2: int = row['id']

            await cur.execute(
                t'UPDATE sample SET project = {project_2} WHERE id = {sample}'
            )

        counts = await self._counts_this_month()
        assert counts[project_1] == 1
        assert counts[project_2] == 1
