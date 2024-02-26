# pylint: disable=protected-access
import datetime
from test.testbase import run_as_sync
from test.testbqbase import BqTest
from textwrap import dedent
from typing import Any
from unittest import mock

import google.cloud.bigquery as bq

from db.python.tables.bq.billing_filter import BillingFilter
from db.python.tables.bq.billing_gcp_daily import BillingGcpDailyTable
from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from db.python.utils import InternalError
from models.models import BillingColumn, BillingTotalCostQueryModel


class TestBillingGcpDailyTable(BqTest):
    """Test BillingRawTable and its methods"""

    def setUp(self):
        super().setUp()

        # setup table object
        self.table_obj = BillingGcpDailyTable(self.connection)

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
            part_time=GenericBQFilter(
                gte=datetime.datetime(2023, 1, 1, 0, 0),
                lte=datetime.datetime(2024, 1, 8, 0, 0),  # 7 days added
            ),
            topic=GenericBQFilter(eq='TEST_TOPIC'),
        )

        query = BillingTotalCostQueryModel(
            fields=[],  # not relevant for this test, but can't be null generally
            start_date=start_date,
            end_date=end_date,
            filters=filters,
        )
        filter_ = BillingGcpDailyTable._query_to_partitioned_filter(query)

        # BillingFilter has __eq__ method, so we can compare them directly
        self.assertEqual(expected_filter, filter_)

    def test_error_no_connection(self):
        """Test No connection exception"""

        with self.assertRaises(InternalError) as context:
            BillingGcpDailyTable(None)

        self.assertTrue(
            'No connection was provided to the table \'BillingGcpDailyTable\''
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

        AND part_time >= TIMESTAMP(@last_loaded_day)
        AND part_time <= TIMESTAMP_ADD(
            TIMESTAMP(@last_loaded_day), INTERVAL 7 DAY
        )

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
        self.assertEqual(dedent(expected_daily_cost_join), dedent(daily_cost_join))

    @run_as_sync
    async def test_get_gcp_projects_return_empty_list(self):
        """Test get_gcp_projects as empty list"""

        # mock BigQuery result as empty list
        self.bq_result.result.return_value = []

        # test get table name function
        gcp_projects = await self.table_obj.get_gcp_projects()

        self.assertEqual([], gcp_projects)

    @run_as_sync
    async def test_get_gcp_projects_return_valid_list(self):
        """Test get_gcp_projects as empty list"""

        # mock BigQuery result as list of 2 records
        self.bq_result.result.return_value = [
            {'gcp_project': 'PROJECT1'},
            {'gcp_project': 'PROJECT2'},
        ]

        # test get table name function
        gcp_projects = await self.table_obj.get_gcp_projects()

        self.assertEqual(['PROJECT1', 'PROJECT2'], gcp_projects)
