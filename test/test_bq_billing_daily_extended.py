# pylint: disable=protected-access
import datetime
from test.testbase import run_as_sync
from test.testbqbase import BqTest
from typing import Any

from db.python.tables.bq.billing_daily_extended import BillingDailyExtendedTable
from db.python.tables.bq.billing_filter import BillingFilter
from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from db.python.utils import InternalError
from models.models import BillingColumn, BillingTotalCostQueryModel


class TestBillingGcpDailyTable(BqTest):
    """Test BillingRawTable and its methods"""

    def setUp(self):
        super().setUp()

        # setup table object
        self.table_obj = BillingDailyExtendedTable(self.connection)

    def test_query_to_partitioned_filter(self):
        """Test query to partitioned filter conversion"""

        # given
        start_date = '2023-01-01'
        end_date = '2024-01-01'
        filters: dict[BillingColumn, str | list[Any] | dict[Any, Any]] = {
            BillingColumn.TOPIC: 'TEST_TOPIC'
        }

        # expected
        expected_filter = BillingFilter(
            day=GenericBQFilter(
                gte=datetime.datetime(2023, 1, 1, 0, 0),
                lte=datetime.datetime(2024, 1, 1, 0, 0),
            ),
            topic=GenericBQFilter(eq='TEST_TOPIC'),
        )

        query = BillingTotalCostQueryModel(
            fields=[],  # not relevant for this test, but can't be null generally
            start_date=start_date,
            end_date=end_date,
            filters=filters,
        )
        filter_ = BillingDailyExtendedTable._query_to_partitioned_filter(query)

        # BillingFilter has __eq__ method, so we can compare them directly
        self.assertEqual(expected_filter, filter_)

    def test_error_no_connection(self):
        """Test No connection exception"""

        with self.assertRaises(InternalError) as context:
            BillingDailyExtendedTable(None)

        self.assertTrue(
            'No connection was provided to the table \'BillingDailyExtendedTable\''
            in str(context.exception)
        )

    def test_get_table_name(self):
        """Test get_table_name"""

        # table name is set in the class
        given_table_name = 'TEST_TABLE_NAME'

        # set table name
        self.table_obj.table_name = given_table_name

        # test get table name function
        table_name = self.table_obj.get_table_name()

        self.assertEqual(given_table_name, table_name)

    @run_as_sync
    async def test_get_extended_values_return_empty_list(self):
        """Test get_extended_values as empty list"""

        # mock BigQuery result as empty list
        self.bq_result.result.return_value = []

        # test get table name function
        records = await self.table_obj.get_extended_values('dataset')

        self.assertEqual([], records)

    @run_as_sync
    async def test_get_extended_values_return_valid_list(self):
        """Test get_extended_values as list of 2 records"""

        # mock BigQuery result as list of 2 records
        self.bq_result.result.return_value = [
            {'dataset': 'DATA1'},
            {'dataset': 'DATA2'},
        ]

        # test get table name function
        records = await self.table_obj.get_extended_values('dataset')

        self.assertEqual(['DATA1', 'DATA2'], records)

    @run_as_sync
    async def test_get_extended_values_error(self):
        """Test get_extended_values return exception as invalid ext column"""

        # mock BigQuery result as empty list
        self.bq_result.result.return_value = []

        # test get table name function
        with self.assertRaises(ValueError) as context:
            await self.table_obj.get_extended_values('rubish')

        self.assertTrue('Invalid field value' in str(context.exception))
