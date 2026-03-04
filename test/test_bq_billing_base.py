from datetime import datetime
from typing import Any

import google.cloud.bigquery as bq
import pytest

from db.python.tables.bq.billing_base import BillingBaseTable
from db.python.tables.bq.billing_daily_extended import BillingDailyExtendedTable
from db.python.tables.bq.billing_filter import BillingFilter
from db.python.tables.bq.billing_utils import (
    abbrev_cost_category,
    append_detailed_cost_records,
    append_total_running_cost,
    convert_output,
    filter_to_optimise_query,
    last_loaded_day_filter,
    prepare_aggregation,
    prepare_order_by_string,
    prepare_time_periods,
)
from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from models.enums import BillingTimePeriods
from models.models import (
    BillingColumn,
    BillingCostBudgetRecord,
    BillingCostDetailsRecord,
    BillingRunningCostQueryModel,
    BillingTotalCostQueryModel,
)
from test.testbqbase import BqTest, MockQueryJob, MockResult


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
        row_values = ('2024-01-01 00:00:00+00:00',)
        return [bq.Row(row_values, {'last_loaded_day': 0})]

    # This is the 2nd query to get aggregated monthly cost
    # return mockup BQ query result as list of rows
    return [
        bq.Row(
            ('TOPIC1', 'Compute Engine', 123.45, 2345.67),
            {'field': 0, 'cost_category': 1, 'daily_cost': 2, 'monthly_cost': 3},
        ),
    ]


def mock_execute_query_empty(_query, *_args, **_kwargs):
    """
    Mock function returning an empty MockQueryJob.
    """
    return MockQueryJob(MockResult(rows=[], total_rows=0))


def mock_execute_query_get_total_cost(_query, *_args, **_kwargs):
    """
    This is a mockup function for _execute_query function
    This returns one mockup BQ query result
    """
    # mockup BQ query result topic cost by invoice month, return row iterator
    rows = [
        {'day': '202301', 'topic': 'TOPIC1', 'cost': 123.10},
        {'day': '202302', 'topic': 'TOPIC1', 'cost': 223.20},
        {'day': '202303', 'topic': 'TOPIC1', 'cost': 323.30},
    ]
    return MockQueryJob(MockResult(rows=rows))


class TestBillingBaseTable(BqTest):
    """Test BillingBaseTable and its methods"""

    @pytest.fixture(autouse=True)
    def set_up(self):
        super().set_up()

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

        assert expected_filter == filter_

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
            assert expected_abrev == abbrev_cost_category(cat)

    def test_prepare_time_periods_by_day(self):
        """Test prepare_time_periods"""

        query = BillingTotalCostQueryModel(
            fields=[],
            start_date='2024-01-01',
            end_date='2024-01-01',
            time_periods=BillingTimePeriods.DAY,
        )

        time_group = prepare_time_periods(query)

        assert time_group.field == 'FORMAT_DATE("%Y-%m-%d", day) as day'
        assert time_group.formula == 'PARSE_DATE("%Y-%m-%d", day) as day'
        assert time_group.separator == ','

    def test_prepare_time_periods_by_week(self):
        """Test prepare_time_periods"""

        query = BillingTotalCostQueryModel(
            fields=[],
            start_date='2024-01-01',
            end_date='2024-01-01',
            time_periods=BillingTimePeriods.WEEK,
        )

        time_group = prepare_time_periods(query)

        assert time_group.field == 'FORMAT_DATE("%Y%W", day) as day'
        assert time_group.formula == 'PARSE_DATE("%Y%W", day) as day'
        assert time_group.separator == ','

    def test_prepare_time_periods_by_month(self):
        """Test prepare_time_periods"""

        query = BillingTotalCostQueryModel(
            fields=[],
            start_date='2024-01-01',
            end_date='2024-01-01',
            time_periods=BillingTimePeriods.MONTH,
        )

        time_group = prepare_time_periods(query)

        assert time_group.field == 'FORMAT_DATE("%Y%m", day) as day'
        assert time_group.formula == 'PARSE_DATE("%Y%m", day) as day'
        assert time_group.separator == ','

    def test_prepare_time_periods_by_invoice_month(self):
        """Test prepare_time_periods"""

        query = BillingTotalCostQueryModel(
            fields=[],
            start_date='2024-01-01',
            end_date='2024-01-01',
            time_periods=BillingTimePeriods.INVOICE_MONTH,
        )

        time_group = prepare_time_periods(query)

        assert time_group.field == 'invoice_month as day'
        assert time_group.formula == 'PARSE_DATE("%Y%m", day) as day'
        assert time_group.separator == ','

    def test_filter_to_optimise_query(self):
        """Test _filter_to_optimise_query"""

        result = filter_to_optimise_query()
        assert result == 'day >= TIMESTAMP(@start_day) AND day <= TIMESTAMP(@last_day)'

    def test_last_loaded_day_filter(self):
        """Test _last_loaded_day_filter"""

        result = last_loaded_day_filter()
        assert result == 'day = TIMESTAMP(@last_loaded_day)'

    def test_convert_output_empty_results(self):
        """Test _convert_output - various empty results"""
        assert convert_output(None) == []
        assert convert_output(MockQueryJob(MockResult(total_rows=0))) == []
        assert convert_output(MockQueryJob(MockResult(rows=[]))) == []

    def test_convert_output_one_record(self):
        """Test _convert_output - one record result"""
        single_row = convert_output(MockQueryJob(MockResult(rows=[{}])))
        assert single_row == [{}]

    def test_convert_output_label_record(self):
        """Test _convert_output - test with label item"""
        row_iterator = convert_output(
            MockQueryJob(
                MockResult(
                    rows=[{'labels': [{'key': 'test_key', 'value': 'test_value'}]}]
                )
            )
        )
        assert row_iterator == [
            {
                # keep the original tables
                'labels': [{'key': 'test_key', 'value': 'test_value'}],
                # append the labels as key-value pairs
                'test_key': 'test_value',
            }
        ]

    def test_prepare_order_by_string_empty(self):
        """Test _prepare_order_by_string - empty results"""
        assert prepare_order_by_string(None) == ''

    def test_prepare_order_by_string_order_by_one_column(self):
        """Test _prepare_order_by_string"""

        # DESC order by column
        assert (
            prepare_order_by_string({BillingColumn.COST: True}) == 'ORDER BY cost DESC'
        )
        # ASC order by column
        assert (
            prepare_order_by_string({BillingColumn.COST: False}) == 'ORDER BY cost ASC'
        )

    def test_prepare_order_by_string_order_by_two_columns(self):
        """Test _prepare_order_by_string - order by 2 columns"""
        assert (
            prepare_order_by_string(
                {BillingColumn.COST: False, BillingColumn.DAY: True}
            )
            == 'ORDER BY cost ASC,day DESC'
        )

    def test_prepare_aggregation_default_group_by(self):
        """Test _prepare_aggregation"""

        query = BillingTotalCostQueryModel(
            fields=[], start_date='2024-01-01', end_date='2024-01-01'
        )

        fields_selected, group_by = prepare_aggregation(query)

        # no fields selected so it is empty
        assert fields_selected == ''
        # by default results are grouped by day
        assert group_by == 'GROUP BY day'

    def test_prepare_aggregation_default_no_grouping_by(self):
        """Test _prepare_aggregation"""

        # test when query is not grouped by
        query = BillingTotalCostQueryModel(
            fields=[BillingColumn.TOPIC],
            start_date='2024-01-01',
            end_date='2024-01-01',
            group_by=False,
        )

        fields_selected, group_by = prepare_aggregation(query)

        # topic field is selected
        assert fields_selected == 'topic'
        # group by is switched off
        assert group_by == ''

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

        fields_selected, group_by = prepare_aggregation(query)
        assert fields_selected == 'topic'
        # always group by day and any field that can be grouped by
        assert group_by == 'GROUP BY day,topic'

    def test_execute_query_results_as_list(self):
        """Test _execute_query"""

        # we are not running SQL against real BQ, just a mocking, so we can use any query
        sql_query = 'SELECT 1;'
        sql_params: list[Any] = []

        # test results_as_list=True
        given_bq_results = [[], [123], ['a', 'b', 'c']]
        for bq_result in given_bq_results:
            self.bq_result._rows = bq_result
            results = self.table_obj._execute_query(
                sql_query, sql_params, results_as_list=True
            )
            assert bq_result == results

    def test_execute_query_results_not_as_list(self):
        """Test _execute_query"""

        # we are not running SQL against real BQ, just a mocking, so we can use any query
        sql_query = 'SELECT 1;'
        sql_params: list[Any] = []

        # now test results_as_list=False
        given_bq_results = [[], [123], ['a', 'b', 'c']]
        for bq_result in given_bq_results:
            # mock BigQuery result
            self.bq_result._rows = bq_result
            self.bq_result.total_bytes_processed = 0
            results = self.table_obj._execute_query(
                sql_query, sql_params, results_as_list=True
            )
            assert bq_result == results

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
            self.bq_result._rows = bq_result
            self.bq_result.total_bytes_processed = 0
            results = self.table_obj._execute_query(
                sql_query, sql_params, results_as_list=True
            )
            assert bq_result == results

    @pytest.mark.asyncio
    async def test_append_total_running_cost_no_topic(self):
        """Test _append_total_running_cost"""

        # test _append_total_running_cost function, no topic present
        total_record = await append_total_running_cost(
            field=BillingColumn.TOPIC,
            is_current_month=True,
            last_loaded_day=None,
            total_monthly={'C': {'ALL': 1000}, 'S': {'ALL': 2000}},
            total_daily={'C': {'ALL': 100}, 'S': {'ALL': 200}},
            total_monthly_category={},
            total_daily_category={},
            results=[],
        )

        assert [
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
        ] == total_record

    @pytest.mark.asyncio
    async def test_append_total_running_cost_not_current_month(self):
        """Test _append_total_running_cost"""

        # test _append_total_running_cost function, not current month
        total_record = await append_total_running_cost(
            field=BillingColumn.TOPIC,
            is_current_month=False,
            last_loaded_day=None,
            total_monthly={'C': {'ALL': 1000}, 'S': {'ALL': 2000}},
            total_daily=None,
            total_monthly_category={},
            total_daily_category={},
            results=[],
        )

        assert [
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
        ] == total_record

    @pytest.mark.asyncio
    async def test_append_total_running_cost_current_month(self):
        """Test _append_total_running_cost"""

        total_record = await append_total_running_cost(
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

        assert [
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
        ] == total_record

    @pytest.mark.asyncio
    async def test_budgets_by_gcp_project_empty_results(self):
        """Test _budgets_by_gcp_project"""

        # Only GCP_PROJECT and current month has budget
        empty_result = await self.table_obj._budgets_by_gcp_project(
            BillingColumn.TOPIC, False
        )
        assert empty_result == {}

        # GCP_PROJECT and current month, but BQ mockup setup as empty values
        empty_result = await self.table_obj._budgets_by_gcp_project(
            BillingColumn.GCP_PROJECT, True
        )
        assert empty_result == {}

    @pytest.mark.asyncio
    async def test_budgets_by_gcp_project_with_results(self):
        """Test _budgets_by_gcp_project"""

        # GCP_PROJECT and current month and Mockup set as 2 records
        self.bq_result._rows = [
            bq.Row(('Project1', 1000.0), {'gcp_project': 0, 'budget': 1}),
            bq.Row(('Project2', 2000.0), {'gcp_project': 0, 'budget': 1}),
        ]

        non_empty_result = await self.table_obj._budgets_by_gcp_project(
            BillingColumn.GCP_PROJECT, True
        )
        assert non_empty_result == {'Project1': 1000.0, 'Project2': 2000.0}

    @pytest.mark.asyncio
    async def test_execute_running_cost_query_invalid_months(self):
        """Test _execute_running_cost_query"""

        # test invalid inputs
        with pytest.raises(ValueError) as context:
            await self.table_obj._execute_running_cost_query_with_filters(
                BillingRunningCostQueryModel(
                    field=BillingColumn.TOPIC, invoice_month=None
                )
            )
        assert 'Invalid invoice month' in str(context.value)

        with pytest.raises(ValueError) as context:
            await self.table_obj._execute_running_cost_query_with_filters(
                BillingRunningCostQueryModel(
                    field=BillingColumn.TOPIC, invoice_month='12345678'
                )
            )
        assert 'Invalid invoice month' in str(context.value)

        with pytest.raises(ValueError) as context:
            await self.table_obj._execute_running_cost_query_with_filters(
                BillingRunningCostQueryModel(
                    field=BillingColumn.TOPIC, invoice_month='1024AA'
                )
            )
        assert 'Invalid invoice month' in str(context.value)

    @pytest.mark.asyncio
    async def test_execute_running_cost_query_empty_results_old_month(self):
        """Test _execute_running_cost_query"""

        # no mocked BQ results, should return as empty
        (
            is_current_month,
            last_loaded_day,
            query_job_result,
        ) = await self.table_obj._execute_running_cost_query_with_filters(
            BillingRunningCostQueryModel(
                field=BillingColumn.TOPIC, invoice_month='202101'
            )
        )

        assert False is is_current_month
        assert None is last_loaded_day
        assert query_job_result == []

    @pytest.mark.asyncio
    async def test_execute_running_cost_query_empty_results_current_month(self):
        """Test _execute_running_cost_query"""

        # no mocked BQ results, should return as empty
        # use current month to test the current month branch
        current_month_as_string = datetime.now().strftime('%Y%m')
        (
            is_current_month,
            last_loaded_day,
            query_job_result,
        ) = await self.table_obj._execute_running_cost_query_with_filters(
            BillingRunningCostQueryModel(
                field=BillingColumn.TOPIC, invoice_month=current_month_as_string
            )
        )

        assert True is is_current_month
        assert None is last_loaded_day
        assert query_job_result == []

    @pytest.mark.asyncio
    async def test_append_total_running_cost_empty_results(self):
        """Test append_total_running_cost"""

        # test empty results
        empty_results = await append_detailed_cost_records(
            budgets_per_gcp_project={},
            is_current_month=False,
            last_loaded_day=None,
            total_monthly={},
            total_daily={},
            field_details={},
            results=[],
        )

        assert empty_results == []

    @pytest.mark.asyncio
    async def test_append_running_cost_records_simple_data(self):
        """Test append_running_cost_records"""

        # prepare simple input data
        field_details: dict[str, Any] = {
            'Project1': [],
        }

        simple_result = await append_detailed_cost_records(
            budgets_per_gcp_project={},
            is_current_month=False,
            last_loaded_day=None,
            total_monthly={'C': {}, 'S': {}},
            total_daily={'C': {}, 'S': {}},
            field_details=field_details,
            results=[],
        )

        assert [
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
        ] == simple_result

    @pytest.mark.asyncio
    async def test_append_running_cost_records_with_details(self):
        """Test append_running_cost_records"""

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

        detailed_result = await append_detailed_cost_records(
            budgets_per_gcp_project={},
            is_current_month=False,
            last_loaded_day=None,
            total_monthly={'C': {}, 'S': {}},
            total_daily={'C': {}, 'S': {}},
            field_details=field_details,
            results=[],
        )

        assert [
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
        ] == detailed_result

    @pytest.mark.asyncio
    async def test_get_running_cost_invalid_input(self):
        """Test get_running_cost"""

        # test invalid outputs
        with pytest.raises(ValueError) as context:
            await self.table_obj.get_running_cost_with_filters(
                # not allowed field
                BillingRunningCostQueryModel(
                    field=BillingColumn.SKU,
                    invoice_month=None,
                )
            )

        assert (
            'Invalid field only topic, dataset, gcp-project, compute_category, '
            'wdl_task_name, cromwell_sub_workflow_name & namespace are allowed'
        ) in str(context.value)

    @pytest.mark.asyncio
    async def test_get_running_cost_empty_results(self):
        """Test get_running_cost"""

        # test empty cost (no BQ mockup data provided)
        empty_results = await self.table_obj.get_running_cost_with_filters(
            BillingRunningCostQueryModel(
                field=BillingColumn.TOPIC,
                invoice_month='202301',
            )
        )

        assert empty_results == []

    @pytest.mark.asyncio
    async def test_get_running_cost_older_month(self):
        """Test get_running_cost"""

        # mockup BQ sql query result for _execute_running_cost_query function
        self.table_obj._execute_query = mock_execute_query_running_cost

        one_record_result = await self.table_obj.get_running_cost_with_filters(
            BillingRunningCostQueryModel(
                field=BillingColumn.TOPIC,
                invoice_month='202301',
            )
        )

        assert [
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
        ] == one_record_result

    @pytest.mark.asyncio
    async def test_get_running_cost_current_month(self):
        """Test get_running_cost"""

        # mockup BQ sql query result for _execute_running_cost_query function
        self.table_obj._execute_query = mock_execute_query_running_cost
        # use the current month to test the current month branch
        current_month_as_string = datetime.now().strftime('%Y%m')

        current_month_result = await self.table_obj.get_running_cost_with_filters(
            BillingRunningCostQueryModel(
                field=BillingColumn.TOPIC,
                invoice_month=current_month_as_string,
            )
        )

        assert [
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
        ] == current_month_result

    @pytest.mark.asyncio
    async def test_get_total_cost(self):
        """Test get_total_cost"""

        # test invalid input
        query = BillingTotalCostQueryModel(
            fields=[], start_date='2023-01-01', end_date='2024-01-01'
        )

        with pytest.raises(ValueError) as context:
            await self.table_obj.get_total_cost(query)
        assert 'Date and Fields are required' in str(context.value)

        # test empty results
        query = BillingTotalCostQueryModel(
            fields=[BillingColumn.TOPIC],
            start_date='2023-01-01',
            end_date='2024-01-01',
            time_periods=BillingTimePeriods.INVOICE_MONTH,
        )

        # no BQ mockup data setup, returns empty list
        empty_results = await self.table_obj.get_total_cost(query)
        assert empty_results == []

        # mockup BQ sql query result for _execute_query to return 3 records.
        # implementation is inside mock_execute_query function
        self.table_obj._execute_query = mock_execute_query_get_total_cost

        results = await self.table_obj.get_total_cost(query)
        assert results == [
            {'day': '202301', 'topic': 'TOPIC1', 'cost': 123.1},
            {'day': '202302', 'topic': 'TOPIC1', 'cost': 223.2},
            {'day': '202303', 'topic': 'TOPIC1', 'cost': 323.3},
        ]
