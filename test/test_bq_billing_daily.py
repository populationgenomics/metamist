# pylint: disable=protected-access
import datetime
from test.testbase import run_as_sync
from test.testbqbase import BqTest
from typing import Any
from unittest import mock

import google.cloud.bigquery as bq

from db.python.tables.bq.billing_daily import BillingDailyTable
from db.python.tables.bq.billing_filter import BillingFilter
from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from db.python.utils import InternalError
from models.models import BillingColumn, BillingTotalCostQueryModel


class TestBillingDailyTable(BqTest):
    """Test BillingRawTable and its methods"""

    def setUp(self):
        super().setUp()

        # setup table object
        self.table_obj = BillingDailyTable(self.connection)

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
        filter_ = BillingDailyTable._query_to_partitioned_filter(query)

        # BillingFilter has __eq__ method, so we can compare them directly
        self.assertEqual(expected_filter, filter_)

    def test_error_no_connection(self):
        """Test No connection exception"""

        with self.assertRaises(InternalError) as context:
            BillingDailyTable(None)

        self.assertTrue(
            'No connection was provided to the table \'BillingDailyTable\''
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
    async def test_last_loaded_day_return_valid_day(self):
        """Test _last_loaded_day"""

        given_last_day = '2021-01-01 00:00:00'

        # mock BigQuery result

        self.bq_result.result.return_value = [
            mock.MagicMock(
                spec=bq.Row,
                last_loaded_day=datetime.datetime.strptime(
                    given_last_day, '%Y-%m-%d %H:%M:%S'
                ),
            )
        ]  # 2021-01-01

        # test get table name function
        last_loaded_day = await self.table_obj._last_loaded_day()

        self.assertEqual(given_last_day, last_loaded_day)

    @run_as_sync
    async def test_last_loaded_day_return_none(self):
        """Test _last_loaded_day as None"""

        # mock BigQuery result as empty list
        self.bq_result.result.return_value = []

        # test get table name function
        last_loaded_day = await self.table_obj._last_loaded_day()

        self.assertEqual(None, last_loaded_day)

    def test_prepare_daily_cost_subquery(self):
        """Test _prepare_daily_cost_subquery"""

        self.table_obj.table_name = 'TEST_TABLE_NAME'

        # given
        given_field = BillingColumn.COST
        given_query_params: list[Any] = []
        given_last_loaded_day = '2021-01-01 00:00:00'

        (
            query_params,
            daily_cost_field,
            daily_cost_join,
        ) = self.table_obj._prepare_daily_cost_subquery(
            given_field, given_query_params, given_last_loaded_day
        )

        # expected
        expected_daily_cost_join = """LEFT JOIN (
            SELECT
                cost as field,
                cost_category,
                SUM(cost) as cost
            FROM
            `TEST_TABLE_NAME`
            WHERE day = TIMESTAMP(@last_loaded_day)
            GROUP BY
                field,
                cost_category
        ) day
        ON month.field = day.field
        AND month.cost_category = day.cost_category
        """

        self.assertEqual(
            [
                bq.ScalarQueryParameter(
                    'last_loaded_day', 'STRING', '2021-01-01 00:00:00'
                )
            ],
            query_params,
        )

        self.assertEqual(', day.cost as daily_cost', daily_cost_field)
        self.assertEqual(expected_daily_cost_join, daily_cost_join)

    @run_as_sync
    async def test_get_entities_as_empty_list(self):
        """
        Test get_topics, get_invoice_months,
        get_cost_categories and get_skus as empty list
        """

        # mock BigQuery result as empty list
        self.bq_result.result.return_value = []

        # test get_topics function
        records = await self.table_obj.get_topics()
        self.assertEqual([], records)

        # test get_invoice_months function
        records = await self.table_obj.get_invoice_months()
        self.assertEqual([], records)

        # test get_cost_categories function
        records = await self.table_obj.get_cost_categories()
        self.assertEqual([], records)

        # test get_skus function
        records = await self.table_obj.get_skus()
        self.assertEqual([], records)

    @run_as_sync
    async def test_get_topics_return_valid_list(self):
        """Test get_topics as empty list"""

        # mock BigQuery result as list of 2 records
        self.bq_result.result.return_value = [
            {'topic': 'TOPIC1'},
            {'topic': 'TOPIC2'},
        ]

        # test get_topics function
        records = await self.table_obj.get_topics()

        self.assertEqual(['TOPIC1', 'TOPIC2'], records)

    @run_as_sync
    async def test_get_invoice_months_return_valid_list(self):
        """Test get_invoice_months as empty list"""

        # mock BigQuery result as list of 2 records
        self.bq_result.result.return_value = [
            {'invoice_month': '202401'},
            {'invoice_month': '202402'},
        ]

        # test get_invoice_months function
        records = await self.table_obj.get_invoice_months()

        self.assertEqual(['202401', '202402'], records)

    @run_as_sync
    async def test_get_cost_categories_return_valid_list(self):
        """Test get_cost_categories as empty list"""

        # mock BigQuery result as list of 2 records
        self.bq_result.result.return_value = [
            {'cost_category': 'CAT1'},
        ]

        # test get_cost_categories function
        records = await self.table_obj.get_cost_categories()

        self.assertEqual(['CAT1'], records)

    @run_as_sync
    async def test_get_skus_return_valid_list(self):
        """Test get_skus as empty list"""

        # mock BigQuery result as list of 3 records
        self.bq_result.result.return_value = [
            {'sku': 'SKU1'},
            {'sku': 'SKU2'},
            {'sku': 'SKU3'},
        ]

        # test get_skus function
        records = await self.table_obj.get_skus()
        self.assertEqual(['SKU1', 'SKU2', 'SKU3'], records)

        # test get_skus function with limit,
        # limit is ignored in the test as we already have mockup data
        records = await self.table_obj.get_skus(limit=3)
        self.assertEqual(['SKU1', 'SKU2', 'SKU3'], records)

        # test get_skus function with limit & offset
        # limit & offset are ignored in the test as we already have mockup data
        records = await self.table_obj.get_skus(limit=3, offset=1)
        self.assertEqual(['SKU1', 'SKU2', 'SKU3'], records)
