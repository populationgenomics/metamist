import datetime
from typing import Any

import pytest

from db.python.tables.bq.billing_filter import BillingFilter
from db.python.tables.bq.billing_raw import BillingRawTable
from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from db.python.utils import InternalError
from models.models import BillingColumn, BillingTotalCostQueryModel
from test.testbqbase import BqTest


class TestBillingRawTable(BqTest):
    """Test BillingRawTable and its methods"""

    @pytest.fixture(autouse=True)
    def set_up(self):
        self.base_set_up()

        # setup table object
        self.table_obj = BillingRawTable(self.connection)

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
            usage_end_time=GenericBQFilter(
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
        filter_ = BillingRawTable._query_to_partitioned_filter(query)

        # BillingFilter has __eq__ method, so we can compare them directly
        assert expected_filter == filter_

    def test_error_no_connection(self):
        """Test No connection exception"""

        with pytest.raises(InternalError) as context:
            BillingRawTable(None)

        assert "No connection was provided to the table 'BillingRawTable'" in str(
            context.value
        )

    def test_get_table_name(self):
        """Test get_table_name"""

        # table name is set in the class
        given_table_name = 'TEST_BQ_AGGREG_RAW'

        # set table name
        self.table_obj.table_name = given_table_name

        # test get table name function
        table_name = self.table_obj.get_table_name()

        assert given_table_name == table_name
