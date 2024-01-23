# pylint: disable=protected-access
import datetime
from test.testbase import run_as_sync
from test.testbqbase import BqTest
from typing import Any
from unittest import mock

import google.cloud.bigquery as bq

from db.python.tables.bq.billing_base import (
    BillingBaseTable,
    abbrev_cost_category,
    prepare_time_periods,
)
from db.python.tables.bq.billing_daily_extended import BillingDailyExtendedTable
from db.python.tables.bq.billing_filter import BillingFilter
from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from models.models import (
    BillingColumn,
    BillingCostBudgetRecord,
    BillingCostDetailsRecord,
    BillingTimePeriods,
    BillingTotalCostQueryModel,
)


class TestBillingBaseTable(BqTest):
    """Test BillingBaseTable and its methods"""

    def setUp(self):
        super().setUp()

        # setup table object
        # base is abstract, so we need to use a child class
        # DailyExtended is the simplest one, almost no overrides
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
        filter_ = BillingBaseTable._query_to_partitioned_filter(query)

        # BillingFilter has __eq__ method, so we can compare them directly
        self.assertEqual(expected_filter, filter_)

    def test_abbrev_cost_category(self):
        """Test abbrev_cost_category"""

        # table name is set in the class
        categories_to_expected = {
            'Cloud Storage': 'S',
            'Compute Engine': 'C',
            'Other': 'C',
        }

        # test category to abreveation
        for cat, expected_abrev in categories_to_expected.items():
            self.assertEqual(expected_abrev, abbrev_cost_category(cat))

    def test_prepare_time_periods_by_day(self):
        """Test prepare_time_periods"""

        query = BillingTotalCostQueryModel(
            fields=[],
            start_date='2024-01-01',
            end_date='2024-01-01',
            time_periods=BillingTimePeriods.DAY,
        )

        time_group = prepare_time_periods(query)

        self.assertEqual('FORMAT_DATE("%Y-%m-%d", day) as day', time_group.field)
        self.assertEqual('PARSE_DATE("%Y-%m-%d", day) as day', time_group.formula)
        self.assertEqual(',', time_group.separator)

    def test_prepare_time_periods_by_week(self):
        """Test prepare_time_periods"""

        query = BillingTotalCostQueryModel(
            fields=[],
            start_date='2024-01-01',
            end_date='2024-01-01',
            time_periods=BillingTimePeriods.WEEK,
        )

        time_group = prepare_time_periods(query)

        self.assertEqual('FORMAT_DATE("%Y%W", day) as day', time_group.field)
        self.assertEqual('PARSE_DATE("%Y%W", day) as day', time_group.formula)
        self.assertEqual(',', time_group.separator)

    def test_prepare_time_periods_by_month(self):
        """Test prepare_time_periods"""

        query = BillingTotalCostQueryModel(
            fields=[],
            start_date='2024-01-01',
            end_date='2024-01-01',
            time_periods=BillingTimePeriods.MONTH,
        )

        time_group = prepare_time_periods(query)

        self.assertEqual('FORMAT_DATE("%Y%m", day) as day', time_group.field)
        self.assertEqual('PARSE_DATE("%Y%m", day) as day', time_group.formula)
        self.assertEqual(',', time_group.separator)

    def test_prepare_time_periods_by_invoice_month(self):
        """Test prepare_time_periods"""

        query = BillingTotalCostQueryModel(
            fields=[],
            start_date='2024-01-01',
            end_date='2024-01-01',
            time_periods=BillingTimePeriods.INVOICE_MONTH,
        )

        time_group = prepare_time_periods(query)

        self.assertEqual('invoice_month as day', time_group.field)
        self.assertEqual('PARSE_DATE("%Y%m", day) as day', time_group.formula)
        self.assertEqual(',', time_group.separator)

    def test_filter_to_optimise_query(self):
        """Test _filter_to_optimise_query"""

        result = BillingBaseTable._filter_to_optimise_query()
        self.assertEqual(
            'day >= TIMESTAMP(@start_day) AND day <= TIMESTAMP(@last_day)', result
        )

    def test_last_loaded_day_filter(self):
        """Test _last_loaded_day_filter"""

        result = BillingBaseTable._last_loaded_day_filter()
        self.assertEqual('day = TIMESTAMP(@last_loaded_day)', result)

    def test_convert_output(self):
        """Test _convert_output"""

        empty_results = BillingBaseTable._convert_output(None)
        self.assertEqual([], empty_results)

        query_job_result = mock.MagicMock(spec=bq.job.QueryJob)
        query_job_result.result.total_rows = 0

        empty_list = BillingBaseTable._convert_output(query_job_result)
        self.assertEqual([], empty_list)

        query_job_result = mock.MagicMock(spec=bq.job.QueryJob)
        query_job_result.result.return_value = mock.MagicMock(spec=bq.table.RowIterator)

        empty_row_iterator = BillingBaseTable._convert_output(query_job_result)
        self.assertEqual([], empty_row_iterator)

        # test with one empty
        mock_rows = mock.MagicMock(spec=bq.table.RowIterator)
        mock_rows.total_rows = 1
        mock_rows.__iter__.return_value = [{}]

        query_job_result = mock.MagicMock(spec=bq.job.QueryJob)
        query_job_result.result.return_value = mock_rows

        single_row = BillingBaseTable._convert_output(query_job_result)
        self.assertEqual([{}], single_row)

        # test with label item
        mock_rows = mock.MagicMock(spec=bq.table.RowIterator)
        mock_rows.total_rows = 1
        mock_rows.__iter__.return_value = [
            {'labels': [{'key': 'test_key', 'value': 'test_value'}]}
        ]

        query_job_result = mock.MagicMock(spec=bq.job.QueryJob)
        query_job_result.result.return_value = mock_rows

        row_iterator = BillingBaseTable._convert_output(query_job_result)
        self.assertEqual(
            [
                {
                    # keep the original labels
                    'labels': [{'key': 'test_key', 'value': 'test_value'}],
                    # append the labels as key-value pairs
                    'test_key': 'test_value',
                }
            ],
            row_iterator,
        )

    def test_prepare_order_by_string(self):
        """Test _prepare_order_by_string"""

        # empty_results
        self.assertEqual('', BillingBaseTable._prepare_order_by_string(None))

        # DESC order by column
        self.assertEqual(
            'ORDER BY cost DESC',
            BillingBaseTable._prepare_order_by_string({BillingColumn.COST: True}),
        )

        # ASC order by column
        self.assertEqual(
            'ORDER BY cost ASC',
            BillingBaseTable._prepare_order_by_string({BillingColumn.COST: False}),
        )

        # order by 2 columns
        self.assertEqual(
            'ORDER BY cost ASC,day DESC',
            BillingBaseTable._prepare_order_by_string(
                {BillingColumn.COST: False, BillingColumn.DAY: True}
            ),
        )

    def test_prepare_aggregation(self):
        """Test _prepare_aggregation"""

        query = BillingTotalCostQueryModel(
            fields=[], start_date='2024-01-01', end_date='2024-01-01'
        )

        fields_selected, group_by = BillingBaseTable._prepare_aggregation(query)
        # no fields selected so it is empty
        self.assertEqual('', fields_selected)
        # by default results are grouped by day
        self.assertEqual('GROUP BY day', group_by)

        # test when query is not grouped by
        query = BillingTotalCostQueryModel(
            fields=[BillingColumn.TOPIC],
            start_date='2024-01-01',
            end_date='2024-01-01',
            group_by=False,
        )

        fields_selected, group_by = BillingBaseTable._prepare_aggregation(query)
        # topic field is selected
        self.assertEqual('topic', fields_selected)
        # group by is switched off
        self.assertEqual('', group_by)

        # test when query is grouped by, but column can not be grouped by
        # cost can not be grouped by, so it is not present in the result
        query = BillingTotalCostQueryModel(
            fields=[BillingColumn.TOPIC, BillingColumn.COST],
            start_date='2024-01-01',
            end_date='2024-01-01',
            group_by=True,
        )

        fields_selected, group_by = BillingBaseTable._prepare_aggregation(query)
        self.assertEqual('topic', fields_selected)
        # always group by day and any field that can be grouped by
        self.assertEqual('GROUP BY day,topic', group_by)

    def test_prepare_labels_function(self):
        """Test _prepare_labels_function"""

        # test when there are no filters
        query = BillingTotalCostQueryModel(
            fields=[], start_date='2024-01-01', end_date='2024-01-01'
        )

        self.assertEqual(None, BillingBaseTable._prepare_labels_function(query))

        # test when there are no labels in filters
        query = BillingTotalCostQueryModel(
            fields=[],
            start_date='2024-01-01',
            end_date='2024-01-01',
            filters={BillingColumn.TOPIC: 'test_topic'},
        )

        self.assertEqual(None, BillingBaseTable._prepare_labels_function(query))

        # test when there are labels in filters
        query = BillingTotalCostQueryModel(
            fields=[],
            start_date='2024-01-01',
            end_date='2024-01-01',
            filters={BillingColumn.LABELS: {'test_key': 'test_value'}},
        )

        func_filter = BillingBaseTable._prepare_labels_function(query)

        self.assertEqual('getLabelValue', func_filter.func_name)
        self.assertEqual(
            """
                    CREATE TEMP FUNCTION getLabelValue(
                        labels ARRAY<STRUCT<key STRING, value STRING>>, label STRING
                    ) AS (
                        (SELECT value FROM UNNEST(labels) WHERE key = label LIMIT 1)
                    );
                """,
            func_filter.func_implementation,
        )
        self.assertEqual(
            '(getLabelValue(labels,@param1) = @value1)',
            func_filter.func_where,
        )

        self.assertEqual(
            [
                bq.ScalarQueryParameter('param1', 'STRING', 'test_key'),
                bq.ScalarQueryParameter('value1', 'STRING', 'test_value'),
            ],
            func_filter.func_sql_parameters,
        )

    def test_execute_query(self):
        """Test _execute_query"""

        # we are not running SQL against real BQ, just a mocking, so we can use any query
        sql_query = 'SELECT 1;'
        sql_params: list[Any] = []

        # test results_as_list=True
        given_bq_results = [[], [123], ['a', 'b', 'c']]
        for bq_result in given_bq_results:
            self.bq_result.result.return_value = bq_result
            results = self.table_obj._execute_query(
                sql_query, sql_params, results_as_list=True
            )
            self.assertEqual(bq_result, results)

        # now test results_as_list=False
        for bq_result in given_bq_results:
            # mock BigQuery result
            self.bq_client.query.return_value = bq_result
            results = self.table_obj._execute_query(
                sql_query, sql_params, results_as_list=False
            )
            self.assertEqual(bq_result, results)

        # now test results_as_list=False and with some dummy params
        sql_params = [
            bq.ScalarQueryParameter('dummy_not_used', 'STRING', '2021-01-01 00:00:00')
        ]

        for bq_result in given_bq_results:
            # mock BigQuery result
            self.bq_client.query.return_value = bq_result
            results = self.table_obj._execute_query(
                sql_query, sql_params, results_as_list=False
            )
            self.assertEqual(bq_result, results)

    @run_as_sync
    async def test_append_total_running_cost(self):
        """Test _append_total_running_cost"""

        # test _append_total_running_cost function, no topic present
        total_record = await BillingBaseTable._append_total_running_cost(
            field=BillingColumn.TOPIC,
            is_current_month=True,
            last_loaded_day=None,
            total_monthly={'C': {'ALL': 1000}, 'S': {'ALL': 2000}},
            total_daily={'C': {'ALL': 100}, 'S': {'ALL': 200}},
            total_monthly_category={},
            total_daily_category={},
            results=[],
        )

        self.assertEqual(
            [
                BillingCostBudgetRecord(
                    field='All Topics',
                    total_monthly=3000.0,
                    total_daily=300.0,
                    compute_monthly=1000.0,
                    compute_daily=100.0,
                    storage_monthly=2000.0,
                    storage_daily=200.0,
                    details=[],
                    budget_spent=None,
                    budget=None,
                    last_loaded_day=None,
                )
            ],
            total_record,
        )

        # test _append_total_running_cost function, not current month
        total_record = await BillingBaseTable._append_total_running_cost(
            field=BillingColumn.TOPIC,
            is_current_month=False,
            last_loaded_day=None,
            total_monthly={'C': {'ALL': 1000}, 'S': {'ALL': 2000}},
            total_daily=None,
            total_monthly_category={},
            total_daily_category={},
            results=[],
        )

        self.assertEqual(
            [
                BillingCostBudgetRecord(
                    field='All Topics',
                    total_monthly=3000.0,
                    total_daily=None,
                    compute_monthly=1000.0,
                    compute_daily=None,
                    storage_monthly=2000.0,
                    storage_daily=None,
                    details=[],
                    budget_spent=None,
                    budget=None,
                    last_loaded_day=None,
                )
            ],
            total_record,
        )

        total_record = await BillingBaseTable._append_total_running_cost(
            field=BillingColumn.TOPIC,
            is_current_month=True,
            last_loaded_day=None,
            total_monthly={'C': {'ALL': 1000}, 'S': {'ALL': 2000}},
            total_daily={'C': {'ALL': 100}, 'S': {'ALL': 200}},
            total_monthly_category={
                'Compute Engine': 900,
                'Cloud Storage': 2000,
                'Other': 100,
            },
            total_daily_category={
                'Compute Engine': 90,
                'Cloud Storage': 200,
                'Other': 10,
            },
            results=[],
        )

        self.assertEqual(
            [
                BillingCostBudgetRecord(
                    field='All Topics',
                    total_monthly=3000.0,
                    total_daily=300.0,
                    compute_monthly=1000.0,
                    compute_daily=100.0,
                    storage_monthly=2000.0,
                    storage_daily=200.0,
                    details=[
                        BillingCostDetailsRecord(
                            cost_group='C',
                            cost_category='Compute Engine',
                            daily_cost=90.0,
                            monthly_cost=900.0,
                        ),
                        BillingCostDetailsRecord(
                            cost_group='S',
                            cost_category='Cloud Storage',
                            daily_cost=200.0,
                            monthly_cost=2000.0,
                        ),
                        BillingCostDetailsRecord(
                            cost_group='C',
                            cost_category='Other',
                            daily_cost=10.0,
                            monthly_cost=100.0,
                        ),
                    ],
                    budget_spent=None,
                    budget=None,
                    last_loaded_day=None,
                )
            ],
            total_record,
        )
