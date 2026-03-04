import datetime
from typing import Any

import google.cloud.bigquery as bq
import pytest

from db.python.tables.bq.billing_daily import BillingDailyTable
from db.python.tables.bq.billing_filter import BillingFilter
from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from db.python.utils import InternalError
from models.models import BillingColumn, BillingTotalCostQueryModel
from test.testbqbase import BqTest


class TestBillingDailyTable(BqTest):
    """Test BillingRawTable and its methods"""

    @pytest.fixture(autouse=True)
    def set_up(self):
        super().set_up()

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
        assert expected_filter == filter_

    def test_error_no_connection(self):
        """Test No connection exception"""

        with pytest.raises(InternalError) as context:
            BillingDailyTable(None)

        assert "No connection was provided to the table 'BillingDailyTable'" in str(
            context.value
        )

    def test_get_table_name(self):
        """Test get_table_name"""

        # table name is set in the class
        given_table_name = 'TEST_TABLE_NAME'

        # set table name
        self.table_obj.table_name = given_table_name

        # test get table name function
        table_name = self.table_obj.get_table_name()

        assert given_table_name == table_name

    @pytest.mark.asyncio
    async def test_last_loaded_day_return_valid_day(self):
        """Test _last_loaded_day"""

        given_last_day = '2021-01-01 00:00:00'

        # mock BigQuery result
        row_values = (datetime.datetime.strptime(given_last_day, '%Y-%m-%d %H:%M:%S'),)

        self.bq_result._rows = [
            bq.Row(row_values, {'last_loaded_day': 0})
        ]  # 2021-01-01

        # test get table name function
        last_loaded_day = await self.table_obj._last_loaded_day()

        assert given_last_day == last_loaded_day

    @pytest.mark.asyncio
    async def test_last_loaded_day_return_none(self):
        """Test _last_loaded_day as None"""

        # mock BigQuery result as empty list
        self.bq_result._rows = []

        # test get table name function
        last_loaded_day = await self.table_obj._last_loaded_day()

        assert last_loaded_day is None

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

        assert [
            bq.ScalarQueryParameter('last_loaded_day', 'STRING', '2021-01-01 00:00:00')
        ] == query_params

        assert daily_cost_field == ', day.cost as daily_cost'
        assert expected_daily_cost_join == daily_cost_join

    @pytest.mark.asyncio
    async def test_get_entities_as_empty_list(self):
        """
        Test get_topics, get_invoice_months,
        get_cost_categories and get_skus as empty list
        """

        # mock BigQuery result as empty list
        self.bq_result._rows = []

        # test get_topics function
        records = await self.table_obj.get_topics()
        assert records == []

        # test get_invoice_months function
        records = await self.table_obj.get_invoice_months()
        assert records == []

        # test get_cost_categories function
        records = await self.table_obj.get_cost_categories()
        assert records == []

        # test get_skus function
        records = await self.table_obj.get_skus()
        assert records == []

    @pytest.mark.asyncio
    async def test_get_topics_return_valid_list(self):
        """Test get_topics as empty list"""

        # mock BigQuery result as list of 2 records
        self.bq_result._rows = [
            {'topic': 'TOPIC1'},
            {'topic': 'TOPIC2'},
        ]

        # test get_topics function
        records = await self.table_obj.get_topics()

        assert records == ['TOPIC1', 'TOPIC2']

    @pytest.mark.asyncio
    async def test_get_invoice_months_return_valid_list(self):
        """Test get_invoice_months as empty list"""

        # mock BigQuery result as list of 2 records
        self.bq_result._rows = [
            {'invoice_month': '202401'},
            {'invoice_month': '202402'},
        ]

        # test get_invoice_months function
        records = await self.table_obj.get_invoice_months()

        assert records == ['202401', '202402']

    @pytest.mark.asyncio
    async def test_get_cost_categories_return_valid_list(self):
        """Test get_cost_categories as empty list"""

        # mock BigQuery result as list of 2 records
        self.bq_result._rows = [
            {'cost_category': 'CAT1'},
        ]

        # test get_cost_categories function
        records = await self.table_obj.get_cost_categories()

        assert records == ['CAT1']

    @pytest.mark.asyncio
    async def test_get_skus_return_valid_list(self):
        """Test get_skus as empty list"""

        # mock BigQuery result as list of 3 records
        self.bq_result._rows = [
            {'sku': 'SKU1'},
            {'sku': 'SKU2'},
            {'sku': 'SKU3'},
        ]

        # test get_skus function
        records = await self.table_obj.get_skus()
        assert records == ['SKU1', 'SKU2', 'SKU3']

        # test get_skus function with limit,
        # limit is ignored in the test as we already have mockup data
        records = await self.table_obj.get_skus(limit=3)
        assert records == ['SKU1', 'SKU2', 'SKU3']

        # test get_skus function with limit & offset
        # limit & offset are ignored in the test as we already have mockup data
        records = await self.table_obj.get_skus(limit=3, offset=1)
        assert records == ['SKU1', 'SKU2', 'SKU3']
