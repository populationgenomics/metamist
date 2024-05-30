# pylint: disable=protected-access too-many-public-methods
from datetime import datetime
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
from models.enums import BillingTimePeriods
from models.models import (
    BillingColumn,
    BillingCostBudgetRecord,
    BillingCostDetailsRecord,
    BillingTotalCostQueryModel,
)


def mock_execute_query_running_cost(query, *_args, **_kwargs):
    """
    This is a mockup function for _execute_query function
    This returns one mockup BQ query result
    for 2 different SQL queries used by get_running_cost API point
    Those 2 queries are:
    1. query to get last loaded day
    2. query to get aggregated monthly/daily cost
    """
    if ' as last_loaded_day' in query:
        # This is the 1st query to get last loaded day
        # mockup BQ query result for last_loaded_day as list of rows
        return [
            mock.MagicMock(spec=bq.Row, last_loaded_day='2024-01-01 00:00:00+00:00')
        ]

    # This is the 2nd query to get aggregated monthly cost
    # return mockup BQ query result as list of rows
    return [
        mock.MagicMock(
            spec=bq.Row,
            field='TOPIC1',
            cost_category='Compute Engine',
            daily_cost=123.45,
            monthly_cost=2345.67,
        )
    ]


def mock_execute_query_get_total_cost(_query, *_args, **_kwargs):
    """
    This is a mockup function for _execute_query function
    This returns one mockup BQ query result
    """
    # mockup BQ query result topic cost by invoice month, return row iterator
    mock_rows = mock.MagicMock(spec=bq.table.RowIterator)
    mock_rows.total_rows = 3
    mock_rows.__iter__.return_value = [
        {'day': '202301', 'topic': 'TOPIC1', 'cost': 123.10},
        {'day': '202302', 'topic': 'TOPIC1', 'cost': 223.20},
        {'day': '202303', 'topic': 'TOPIC1', 'cost': 323.30},
    ]
    mock_result = mock.MagicMock(spec=bq.job.QueryJob)
    mock_result.result.return_value = mock_rows
    return mock_result


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
                gte=datetime(2023, 1, 1, 0, 0),
                lte=datetime(2024, 1, 1, 0, 0),
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

    def test_convert_output_empty_results(self):
        """Test _convert_output - various empty results"""

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

    def test_convert_output_one_record(self):
        """Test _convert_output - one record result"""

        mock_rows = mock.MagicMock(spec=bq.table.RowIterator)
        mock_rows.total_rows = 1
        mock_rows.__iter__.return_value = [{}]

        query_job_result = mock.MagicMock(spec=bq.job.QueryJob)
        query_job_result.result.return_value = mock_rows

        single_row = BillingBaseTable._convert_output(query_job_result)
        self.assertEqual([{}], single_row)

    def test_convert_output_label_record(self):
        """Test _convert_output - test with label item"""
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

    def test_prepare_order_by_string_empty(self):
        """Test _prepare_order_by_string - empty results"""

        self.assertEqual('', BillingBaseTable._prepare_order_by_string(None))

    def test_prepare_order_by_string_order_by_one_column(self):
        """Test _prepare_order_by_string"""

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

    def test_prepare_order_by_string_order_by_two_columns(self):
        """Test _prepare_order_by_string - order by 2 columns"""
        self.assertEqual(
            'ORDER BY cost ASC,day DESC',
            BillingBaseTable._prepare_order_by_string(
                {BillingColumn.COST: False, BillingColumn.DAY: True}
            ),
        )

    def test_prepare_aggregation_default_group_by(self):
        """Test _prepare_aggregation"""

        query = BillingTotalCostQueryModel(
            fields=[], start_date='2024-01-01', end_date='2024-01-01'
        )

        fields_selected, group_by = BillingBaseTable._prepare_aggregation(query)
        # no fields selected so it is empty
        self.assertEqual('', fields_selected)
        # by default results are grouped by day
        self.assertEqual('GROUP BY day', group_by)

    def test_prepare_aggregation_default_no_grouping_by(self):
        """Test _prepare_aggregation"""

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

    def test_prepare_aggregation_default_group_by_more_columns(self):
        """Test _prepare_aggregation"""

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

    def test_execute_query_results_as_list(self):
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

    def test_execute_query_results_not_as_list(self):
        """Test _execute_query"""

        # we are not running SQL against real BQ, just a mocking, so we can use any query
        sql_query = 'SELECT 1;'
        sql_params: list[Any] = []

        # now test results_as_list=False
        given_bq_results = [[], [123], ['a', 'b', 'c']]
        for bq_result in given_bq_results:
            # mock BigQuery result
            self.bq_result.result.return_value = bq_result
            self.bq_result.total_bytes_processed = 0
            results = self.table_obj._execute_query(
                sql_query, sql_params, results_as_list=True
            )
            self.assertEqual(bq_result, results)

    def test_execute_query_with_sql_params(self):
        """Test _execute_query"""

        # now test results_as_list=False and with some dummy params
        sql_query = 'SELECT 1;'
        sql_params = [
            bq.ScalarQueryParameter('dummy_not_used', 'STRING', '2021-01-01 00:00:00')
        ]

        given_bq_results = [[], [123], ['a', 'b', 'c']]
        for bq_result in given_bq_results:
            # mock BigQuery result
            self.bq_result.result.return_value = bq_result
            self.bq_result.total_bytes_processed = 0
            results = self.table_obj._execute_query(
                sql_query, sql_params, results_as_list=True
            )
            self.assertEqual(bq_result, results)

    @run_as_sync
    async def test_append_total_running_cost_no_topic(self):
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

    @run_as_sync
    async def test_append_total_running_cost_not_current_month(self):
        """Test _append_total_running_cost"""

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

    @run_as_sync
    async def test_append_total_running_cost_current_month(self):
        """Test _append_total_running_cost"""

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

    @run_as_sync
    async def test_budgets_by_gcp_project_empty_results(self):
        """Test _budgets_by_gcp_project"""

        # Only GCP_PROJECT and current month has budget
        empty_result = await self.table_obj._budgets_by_gcp_project(
            BillingColumn.TOPIC, False
        )
        self.assertEqual({}, empty_result)

        # GCP_PROJECT and current month, but BQ mockup setup as empty values
        empty_result = await self.table_obj._budgets_by_gcp_project(
            BillingColumn.GCP_PROJECT, True
        )
        self.assertEqual({}, empty_result)

    @run_as_sync
    async def test_budgets_by_gcp_project_with_results(self):
        """Test _budgets_by_gcp_project"""

        # GCP_PROJECT and current month and Mockup set as 2 records
        self.bq_result.result.return_value = [
            mock.MagicMock(spec=bq.Row, gcp_project='Project1', budget=1000.0),
            mock.MagicMock(spec=bq.Row, gcp_project='Project2', budget=2000.0),
        ]

        non_empty_result = await self.table_obj._budgets_by_gcp_project(
            BillingColumn.GCP_PROJECT, True
        )
        self.assertDictEqual({'Project1': 1000.0, 'Project2': 2000.0}, non_empty_result)

    @run_as_sync
    async def test_execute_running_cost_query_invalid_months(self):
        """Test _execute_running_cost_query"""

        # test invalid inputs
        with self.assertRaises(ValueError) as context:
            await self.table_obj._execute_running_cost_query(BillingColumn.TOPIC, None)

        self.assertTrue('Invalid invoice month' in str(context.exception))

        with self.assertRaises(ValueError) as context:
            await self.table_obj._execute_running_cost_query(
                BillingColumn.TOPIC, '12345678'
            )

        self.assertTrue('Invalid invoice month' in str(context.exception))

        with self.assertRaises(ValueError) as context:
            await self.table_obj._execute_running_cost_query(
                BillingColumn.TOPIC, '1024AA'
            )
        self.assertTrue('Invalid invoice month' in str(context.exception))

    @run_as_sync
    async def test_execute_running_cost_query_empty_results_old_month(self):
        """Test _execute_running_cost_query"""

        # no mocked BQ results, should return as empty
        (
            is_current_month,
            last_loaded_day,
            query_job_result,
        ) = await self.table_obj._execute_running_cost_query(
            BillingColumn.TOPIC, '202101'
        )

        self.assertEqual(False, is_current_month)
        self.assertEqual(None, last_loaded_day)
        self.assertEqual([], query_job_result)

    @run_as_sync
    async def test_execute_running_cost_query_empty_results_current_month(self):
        """Test _execute_running_cost_query"""

        # no mocked BQ results, should return as empty
        # use current month to test the current month branch
        current_month_as_string = datetime.now().strftime('%Y%m')
        (
            is_current_month,
            last_loaded_day,
            query_job_result,
        ) = await self.table_obj._execute_running_cost_query(
            BillingColumn.TOPIC, current_month_as_string
        )

        self.assertEqual(True, is_current_month)
        self.assertEqual(None, last_loaded_day)
        self.assertEqual([], query_job_result)

    @run_as_sync
    async def test_append_running_cost_records_empty_results(self):
        """Test _append_running_cost_records"""

        # test empty results
        empty_results = await self.table_obj._append_running_cost_records(
            field=BillingColumn.TOPIC,
            is_current_month=False,
            last_loaded_day=None,
            total_monthly={},
            total_daily={},
            field_details={},
            results=[],
        )

        self.assertEqual([], empty_results)

    @run_as_sync
    async def test_append_running_cost_records_simple_data(self):
        """Test _append_running_cost_records"""

        # prepare simple input data
        field_details: dict[str, Any] = {
            'Project1': [],
        }

        simple_result = await self.table_obj._append_running_cost_records(
            field=BillingColumn.GCP_PROJECT,
            is_current_month=False,
            last_loaded_day=None,
            total_monthly={'C': {}, 'S': {}},
            total_daily={'C': {}, 'S': {}},
            field_details=field_details,
            results=[],
        )

        self.assertEqual(
            [
                BillingCostBudgetRecord(
                    field='Project1',
                    total_monthly=0.0,
                    compute_monthly=0.0,
                    compute_daily=0.0,
                    storage_monthly=0.0,
                    storage_daily=0.0,
                    details=[],
                    last_loaded_day=None,
                    total_daily=None,
                    budget_spent=None,
                    budget=None,
                )
            ],
            simple_result,
        )

    @run_as_sync
    async def test_append_running_cost_records_with_details(self):
        """Test _append_running_cost_records"""

        # prepare input data with more details
        field_details = {
            'Project2': [
                {
                    'cost_group': 'C',
                    'cost_category': 'Compute Engine',
                    'daily_cost': 90.0,
                    'monthly_cost': 900.0,
                }
            ],
        }

        detailed_result = await self.table_obj._append_running_cost_records(
            field=BillingColumn.GCP_PROJECT,
            is_current_month=False,
            last_loaded_day=None,
            total_monthly={'C': {}, 'S': {}},
            total_daily={'C': {}, 'S': {}},
            field_details=field_details,
            results=[],
        )

        self.assertEqual(
            [
                BillingCostBudgetRecord(
                    field='Project2',
                    total_monthly=0.0,
                    compute_monthly=0.0,
                    compute_daily=0.0,
                    storage_monthly=0.0,
                    storage_daily=0.0,
                    details=[
                        BillingCostDetailsRecord(
                            cost_group='C',
                            cost_category='Compute Engine',
                            daily_cost=90.0,
                            monthly_cost=900.0,
                        )
                    ],
                    last_loaded_day=None,
                    total_daily=None,
                    budget_spent=None,
                    budget=None,
                )
            ],
            detailed_result,
        )

    @run_as_sync
    async def test_get_running_cost_invalid_input(self):
        """Test get_running_cost"""

        # test invalid outputs
        with self.assertRaises(ValueError) as context:
            await self.table_obj.get_running_cost(
                # not allowed field
                field=BillingColumn.SKU,
                invoice_month=None,
            )

        self.assertTrue(
            (
                'Invalid field only topic, dataset, gcp-project, compute_category, '
                'wdl_task_name, cromwell_sub_workflow_name & namespace are allowed'
            )
            in str(context.exception)
        )

    @run_as_sync
    async def test_get_running_cost_empty_results(self):
        """Test get_running_cost"""

        # test empty cost (no BQ mockup data provided)
        empty_results = await self.table_obj.get_running_cost(
            field=BillingColumn.TOPIC,
            invoice_month='202301',
        )

        self.assertEqual([], empty_results)

    @run_as_sync
    async def test_get_running_cost_older_month(self):
        """Test get_running_cost"""

        # mockup BQ sql query result for _execute_running_cost_query function
        self.table_obj._execute_query = mock.MagicMock(
            side_effect=mock_execute_query_running_cost
        )

        one_record_result = await self.table_obj.get_running_cost(
            field=BillingColumn.TOPIC,
            invoice_month='202301',
        )

        self.assertEqual(
            [
                BillingCostBudgetRecord(
                    field='All Topics',
                    total_monthly=2345.67,
                    total_daily=None,
                    compute_monthly=2345.67,
                    compute_daily=None,
                    storage_monthly=0.0,
                    storage_daily=None,
                    details=[
                        BillingCostDetailsRecord(
                            cost_group='C',
                            cost_category='Compute Engine',
                            daily_cost=None,
                            monthly_cost=2345.67,
                        )
                    ],
                    budget_spent=None,
                    budget=None,
                    last_loaded_day=None,
                ),
                BillingCostBudgetRecord(
                    field='TOPIC1',
                    total_monthly=2345.67,
                    total_daily=None,
                    compute_monthly=2345.67,
                    compute_daily=0.0,
                    storage_monthly=0.0,
                    storage_daily=0.0,
                    details=[
                        BillingCostDetailsRecord(
                            cost_group='C',
                            cost_category='Compute Engine',
                            daily_cost=None,
                            monthly_cost=2345.67,
                        )
                    ],
                    budget_spent=None,
                    budget=None,
                    last_loaded_day=None,
                ),
            ],
            one_record_result,
        )

    @run_as_sync
    async def test_get_running_cost_current_month(self):
        """Test get_running_cost"""

        # mockup BQ sql query result for _execute_running_cost_query function
        self.table_obj._execute_query = mock.MagicMock(
            side_effect=mock_execute_query_running_cost
        )

        # use the current month to test the current month branch
        current_month_as_string = datetime.now().strftime('%Y%m')

        current_month_result = await self.table_obj.get_running_cost(
            field=BillingColumn.TOPIC,
            invoice_month=current_month_as_string,
        )

        self.assertEqual(
            [
                BillingCostBudgetRecord(
                    field='All Topics',
                    total_monthly=2345.67,
                    total_daily=123.45,
                    compute_monthly=2345.67,
                    compute_daily=123.45,
                    storage_monthly=0.0,
                    storage_daily=0.0,
                    details=[
                        BillingCostDetailsRecord(
                            cost_group='C',
                            cost_category='Compute Engine',
                            daily_cost=123.45,
                            monthly_cost=2345.67,
                        )
                    ],
                    budget_spent=None,
                    budget=None,
                    last_loaded_day='Jan 01',
                ),
                BillingCostBudgetRecord(
                    field='TOPIC1',
                    total_monthly=2345.67,
                    total_daily=123.45,
                    compute_monthly=2345.67,
                    compute_daily=123.45,
                    storage_monthly=0.0,
                    storage_daily=0.0,
                    details=[
                        BillingCostDetailsRecord(
                            cost_group='C',
                            cost_category='Compute Engine',
                            daily_cost=123.45,
                            monthly_cost=2345.67,
                        )
                    ],
                    budget_spent=None,
                    budget=None,
                    last_loaded_day='Jan 01',
                ),
            ],
            current_month_result,
        )

    @run_as_sync
    async def test_get_total_cost(self):
        """Test get_total_cost"""

        # test invalid input
        query = BillingTotalCostQueryModel(
            fields=[], start_date='2023-01-01', end_date='2024-01-01'
        )

        with self.assertRaises(ValueError) as context:
            await self.table_obj.get_total_cost(query)

        self.assertTrue('Date and Fields are required' in str(context.exception))

        # test empty results
        query = BillingTotalCostQueryModel(
            fields=[BillingColumn.TOPIC],
            start_date='2023-01-01',
            end_date='2024-01-01',
            time_periods=BillingTimePeriods.INVOICE_MONTH,
        )

        # no BQ mockup data setup, returns empty list
        empty_results = await self.table_obj.get_total_cost(query)
        self.assertEqual([], empty_results)

        # mockup BQ sql query result for _execute_query to return 3 records.
        # implementation is inside mock_execute_query function
        self.table_obj._execute_query = mock.MagicMock(
            side_effect=mock_execute_query_get_total_cost
        )
        results = await self.table_obj.get_total_cost(query)
        self.assertEqual(
            [
                {'day': '202301', 'topic': 'TOPIC1', 'cost': 123.1},
                {'day': '202302', 'topic': 'TOPIC1', 'cost': 223.2},
                {'day': '202303', 'topic': 'TOPIC1', 'cost': 323.3},
            ],
            results,
        )
