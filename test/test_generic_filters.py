"""Integration tests for GenericFilter against a real PostgreSQL database"""

import dataclasses
from datetime import date

import pytest
from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool

from db.python.filters import GenericFilter, GenericFilterModel
from models.enums.analysis import AnalysisStatus
from models.models.project import ProjectMemberRole


@dataclasses.dataclass(kw_only=True)
class GenericFilterTest(GenericFilterModel):
    """Test model for GenericFilter"""

    test_string: GenericFilter[str] | None = None
    test_int: GenericFilter[int] | None = None
    test_bool: GenericFilter[bool] | None = None
    test_date: GenericFilter[date] | None = None
    test_enum: GenericFilter[AnalysisStatus] | None = None
    test_str_enum: GenericFilter[ProjectMemberRole] | None = None


@pytest.fixture
async def test_table(db_pool: AsyncConnectionPool[AsyncConnection[DictRow]]):
    """Create a test table for generic filter testing"""
    async with db_pool.connection() as conn:
        # Create test table
        await conn.execute("""
            CREATE TABLE test_generic_filters (
                id SERIAL PRIMARY KEY,
                test_string TEXT,
                test_int INTEGER,
                test_bool BOOLEAN,
                test_date DATE,
                test_enum main.analysis_status,
                test_str_enum main.project_member_role
            )
        """)

        yield

        # Cleanup after tests
        await conn.execute('DROP TABLE IF EXISTS test_generic_filters')


@pytest.fixture
async def test_data(
    test_table: None,  # noqa: ARG001
    db_pool: AsyncConnectionPool[AsyncConnection[DictRow]],
):
    """Insert test data into the test table"""
    async with db_pool.connection() as conn:
        # Insert test data
        await conn.execute("""
            INSERT INTO test_generic_filters (test_string, test_int, test_bool, test_date, test_enum, test_str_enum)
            VALUES
                ('test', 100, true, '2024-01-01', 'queued', 'reader'),
                ('Test', 200, false, '2024-02-01', 'in-progress', 'contributor'),
                ('another', 150, true, '2024-01-15', 'queued', 'writer'),
                ('per%ce_nt', 175, false, '2024-03-01', 'completed', 'reader'),
                ('contains_test', 125, true, '2024-01-10', 'in-progress', 'contributor'),
                ('testprefix', 300, false, '2024-04-01', 'queued', 'writer'),
                ('TestPrefix', 350, true, '2024-05-01', 'completed', 'reader'),
                (NULL, 999, NULL, NULL, NULL, NULL)
        """)

    return db_pool


async def execute_filter(
    conn: AsyncConnection[DictRow],
    filter_: GenericFilterTest,
    field_mapping: dict[str, str] | None = None,
) -> list[DictRow]:
    """Helper to execute a filter and return results"""
    where_clause = filter_.to_sql(field_mapping)

    query = t'select * from test_generic_filters where {where_clause:q} order by id'

    cur = await conn.execute(query)
    return await cur.fetchall()


@pytest.mark.asyncio
class TestGenericFilters:
    """Test generic filters against real database"""

    async def test_basic_no_override(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that the basic filter converts to SQL as expected and executes correctly"""
        filter_ = GenericFilterTest(test_string=GenericFilter(eq='test'))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        assert len(results) == 1
        assert results[0]['test_string'] == 'test'
        assert results[0]['test_int'] == 100

    async def test_contains_case_sensitive(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that the contains filter works and is case-sensitive"""
        filter_ = GenericFilterTest(test_string=GenericFilter(contains='test'))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match 'test', 'contains_test', 'testprefix'
        assert len(results) == 3
        assert {r['test_string'] for r in results} == {
            'test',
            'contains_test',
            'testprefix',
        }

    async def test_malicious_contains(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that a contains-% filter works with special characters properly escaped"""
        filter_ = GenericFilterTest(test_string=GenericFilter(contains='per%ce_nt'))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        assert len(results) == 1
        assert results[0]['test_string'] == 'per%ce_nt'

    async def test_icontains_is_not_case_sensitive(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that the icontains filter is case-insensitive"""
        filter_ = GenericFilterTest(test_string=GenericFilter(icontains='test'))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match 'test', 'Test', 'contains_test', 'testprefix', 'TestPrefix'
        assert len(results) == 5

    async def test_malicious_icontains(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that an icontains-% filter works with special characters properly escaped"""
        filter_ = GenericFilterTest(test_string=GenericFilter(icontains='PER%CE_NT'))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        assert len(results) == 1
        assert results[0]['test_string'] == 'per%ce_nt'

    async def test_in_single(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that a single value filtered using the 'in' operator gets converted to an eq operation"""
        filter_ = GenericFilterTest(test_string=GenericFilter(in_=['test']))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        assert len(results) == 1
        assert results[0]['test_string'] == 'test'

    async def test_in_multiple(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that values filtered using the 'in' operator work correctly"""
        filter_ = GenericFilterTest(test_int=GenericFilter(in_=[100, 150]))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        assert len(results) == 2
        assert {r['test_int'] for r in results} == {100, 150}

    async def test_gt_single(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that a single value filtered using the 'gt' operator works correctly"""
        filter_ = GenericFilterTest(test_int=GenericFilter(gt=175))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match 200, 300, 350, 999
        assert len(results) == 4
        assert all(r['test_int'] > 175 for r in results)

    async def test_gte_single(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that a single value filtered using the 'gte' operator works correctly"""
        filter_ = GenericFilterTest(test_int=GenericFilter(gte=175))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match 175, 200, 300, 350, 999
        assert len(results) == 5
        assert all(r['test_int'] >= 175 for r in results)

    async def test_lt_single(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that a single value filtered using the 'lt' operator works correctly"""
        filter_ = GenericFilterTest(test_int=GenericFilter(lt=150))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match 100, 125
        assert len(results) == 2
        assert all(r['test_int'] < 150 for r in results)

    async def test_lte_single(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that a single value filtered using the 'lte' operator works correctly"""
        filter_ = GenericFilterTest(test_int=GenericFilter(lte=150))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match 100, 125, 150
        assert len(results) == 3
        assert all(r['test_int'] <= 150 for r in results)

    async def test_not_in_multiple(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that values filtered using the 'nin' operator work correctly"""
        filter_ = GenericFilterTest(test_int=GenericFilter(nin=[100, 150, 200]))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match 125, 175, 300, 350, 999
        assert len(results) == 5
        assert all(r['test_int'] not in [100, 150, 200] for r in results)

    async def test_not_in_includes_nulls(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that nulls are included in the result of not in query"""
        filter_ = GenericFilterTest(
            test_enum=GenericFilter(
                nin=[AnalysisStatus.COMPLETED, AnalysisStatus.QUEUED]
            )
        )

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match in progress and null
        assert len(results) == 3
        assert (
            len([r for r in results if r['test_enum'] == AnalysisStatus.IN_PROGRESS])
            == 2
        )
        assert len([r for r in results if r['test_enum'] is None]) == 1
        assert all(
            r['test_enum'] not in [AnalysisStatus.COMPLETED, AnalysisStatus.QUEUED]
            for r in results
        )

    async def test_neq(self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]):
        """Test that the 'neq' (not equal) operator works correctly"""
        filter_ = GenericFilterTest(test_string=GenericFilter(neq='test'))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should exclude only 'test'
        assert len(results) == 6
        assert all(r['test_string'] != 'test' for r in results)

    async def test_startswith(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that the 'startswith' operator works correctly"""
        filter_ = GenericFilterTest(test_string=GenericFilter(startswith='test'))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match 'test', 'testprefix'
        assert len(results) == 2
        assert {r['test_string'] for r in results} == {'test', 'testprefix'}

    async def test_isnull_true(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that the 'isnull=True' operator works correctly"""
        # Insert a row with NULL test_string
        async with test_data.connection() as conn:
            filter_ = GenericFilterTest(test_string=GenericFilter(isnull=True))
            results = await execute_filter(conn, filter_)

        assert len(results) == 1
        assert results[0]['test_string'] is None
        assert results[0]['test_int'] == 999

    async def test_isnull_false(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that the 'isnull=False' operator works correctly"""
        # Insert a row with NULL test_string
        async with test_data.connection() as conn:
            filter_ = GenericFilterTest(test_string=GenericFilter(isnull=False))
            results = await execute_filter(conn, filter_)

        # Should match all rows except the NULL one
        assert len(results) == 7
        assert all(r['test_string'] is not None for r in results)

    async def test_multiple_conditions(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test combining multiple filter conditions"""
        filter_ = GenericFilterTest(
            test_string=GenericFilter(icontains='test'), test_int=GenericFilter(gte=200)
        )

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match 'Test' (200), 'testprefix' (300), 'TestPrefix' (350)
        assert len(results) == 3
        assert all('test' in r['test_string'].lower() for r in results)
        assert all(r['test_int'] >= 200 for r in results)

    # Boolean filter tests
    async def test_bool_eq_true(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test filtering boolean field for True values"""
        filter_ = GenericFilterTest(test_bool=GenericFilter(eq=True))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match rows 1, 3, 5, 7
        assert len(results) == 4
        assert all(r['test_bool'] is True for r in results)

    async def test_bool_eq_false(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test filtering boolean field for False values"""
        filter_ = GenericFilterTest(test_bool=GenericFilter(eq=False))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match rows 2, 4, 6
        assert len(results) == 3
        assert all(r['test_bool'] is False for r in results)

    async def test_bool_neq(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test not equal filter on boolean field"""
        filter_ = GenericFilterTest(test_bool=GenericFilter(neq=True))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        assert len(results) == 3
        assert all(r['test_bool'] is False for r in results)

    # Date filter tests
    async def test_date_eq(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test equality filter on date field"""
        filter_ = GenericFilterTest(test_date=GenericFilter(eq=date(2024, 1, 1)))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        assert len(results) == 1
        assert results[0]['test_date'] == date(2024, 1, 1)

    async def test_date_gt(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test greater than filter on date field"""
        filter_ = GenericFilterTest(test_date=GenericFilter(gt=date(2024, 2, 1)))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match dates after 2024-02-01: 2024-03-01, 2024-04-01, 2024-05-01
        assert len(results) == 3
        assert all(r['test_date'] > date(2024, 2, 1) for r in results)

    async def test_date_gte(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test greater than or equal filter on date field"""
        filter_ = GenericFilterTest(test_date=GenericFilter(gte=date(2024, 2, 1)))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match dates >= 2024-02-01
        assert len(results) == 4
        assert all(r['test_date'] >= date(2024, 2, 1) for r in results)

    async def test_date_lt(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test less than filter on date field"""
        filter_ = GenericFilterTest(test_date=GenericFilter(lt=date(2024, 2, 1)))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match dates before 2024-02-01: 2024-01-01, 2024-01-10, 2024-01-15
        assert len(results) == 3
        assert all(r['test_date'] < date(2024, 2, 1) for r in results)

    async def test_date_lte(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test less than or equal filter on date field"""
        filter_ = GenericFilterTest(test_date=GenericFilter(lte=date(2024, 2, 1)))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match dates <= 2024-02-01
        assert len(results) == 4
        assert all(r['test_date'] <= date(2024, 2, 1) for r in results)

    async def test_date_in(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test 'in' filter on date field"""
        filter_ = GenericFilterTest(
            test_date=GenericFilter(in_=[date(2024, 1, 1), date(2024, 3, 1)])
        )

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        assert len(results) == 2
        assert {r['test_date'] for r in results} == {date(2024, 1, 1), date(2024, 3, 1)}

    # Enum filter tests
    async def test_enum_eq(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test equality filter on enum field"""
        filter_ = GenericFilterTest(test_enum=GenericFilter(eq=AnalysisStatus.QUEUED))

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match rows 1, 3, 6 (queued)
        assert len(results) == 3
        assert all(r['test_enum'] == AnalysisStatus.QUEUED for r in results)

    async def test_enum_in(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test 'in' filter on enum field"""
        filter_ = GenericFilterTest(
            test_enum=GenericFilter(
                in_=[AnalysisStatus.QUEUED, AnalysisStatus.IN_PROGRESS]
            )
        )

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match rows with queued or in-progress
        assert len(results) == 5
        assert all(
            r['test_enum'] in [AnalysisStatus.QUEUED, AnalysisStatus.IN_PROGRESS]
            for r in results
        )

    async def test_enum_neq(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test not equal filter on enum field"""
        filter_ = GenericFilterTest(
            test_enum=GenericFilter(neq=AnalysisStatus.COMPLETED)
        )

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should exclude completed
        assert len(results) == 5
        assert all(r['test_enum'] != 'completed' for r in results)

    async def test_enum_nin(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test 'not in' filter on enum field"""
        filter_ = GenericFilterTest(
            test_enum=GenericFilter(nin=[AnalysisStatus.QUEUED])
        )

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should exclude queued
        assert len(results) == 5
        assert all(r['test_enum'] != 'queued' for r in results)

    # String enum filter tests
    async def test_str_enum_eq(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test equality filter on string enum field"""
        filter_ = GenericFilterTest(
            test_str_enum=GenericFilter(eq=ProjectMemberRole.contributor)
        )

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match rows 2, 5 (contributor)
        assert len(results) == 2
        assert all(r['test_str_enum'] == 'contributor' for r in results)

    async def test_str_enum_in(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test 'in' filter on string enum field"""
        filter_ = GenericFilterTest(
            test_str_enum=GenericFilter(
                in_=[ProjectMemberRole.reader, ProjectMemberRole.writer]
            )
        )

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match rows with reader or writer
        assert len(results) == 5
        assert all(r['test_str_enum'] in ['reader', 'writer'] for r in results)

    # Combined filters with multiple types
    async def test_combined_multiple_types(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test combining filters across different data types"""
        filter_ = GenericFilterTest(
            test_bool=GenericFilter(eq=True),
            test_int=GenericFilter(gte=150),
            test_enum=GenericFilter(eq=AnalysisStatus.QUEUED),
        )

        async with test_data.connection() as conn:
            results = await execute_filter(conn, filter_)

        # Should match row 6: bool=True, int=300, enum=queued (only matching row with bool=True, int>=150, enum=queued)
        assert len(results) == 1
        assert results[0]['test_bool'] is True
        assert results[0]['test_int'] >= 150
        assert results[0]['test_enum'] == AnalysisStatus.QUEUED

    async def test_filter_with_override(
        self, test_data: AsyncConnectionPool[AsyncConnection[DictRow]]
    ):
        """Test that the basic filter converts to SQL as expected with column name overrides and executes correctly"""
        filter_ = GenericFilterTest(test_string=GenericFilter(eq='test'))
        field_mapping = {
            'test_string': 'tgf.test_string',
        }
        where_clause = filter_.to_sql(field_mapping)

        async with test_data.connection() as conn:
            query = t'select * from test_generic_filters tgf where {where_clause:q} order by id'
            results = await (await conn.execute(query)).fetchall()

        assert len(results) == 1
        assert results[0]['test_string'] == 'test'
        assert results[0]['test_int'] == 100
