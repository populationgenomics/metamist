import re
from abc import ABCMeta, abstractmethod
from collections import Counter, defaultdict, namedtuple
from datetime import datetime
from typing import Any

from google.cloud import bigquery

from api.settings import BQ_BUDGET_VIEW, BQ_DAYS_BACK_OPTIMAL
from api.utils.dates import get_invoice_month_range, reformat_datetime
from db.python.gcp_connect import BqDbBase
from models.models import (
    BillingColumn,
    BillingCostBudgetRecord,
    BillingTimePeriods,
    BillingTotalCostQueryModel,
)

# Label added to each Billing Big Query request,
# so we can track the cost of metamist-api BQ usage
BQ_LABELS = {'source': 'metamist-api'}


# Day Time details used in grouping and parsing formulas
TimeGroupingDetails = namedtuple('TimeGroupingDetails', ['field', 'formula'])


def abbrev_cost_category(cost_category: str) -> str:
    """abbreviate cost category"""
    return 'S' if cost_category == 'Cloud Storage' else 'C'


def prepare_time_periods(
    query: BillingTotalCostQueryModel,
) -> TimeGroupingDetails:
    """Prepare Time periods grouping and parsing formulas"""
    time_column = query.time_column or 'day'
    result = TimeGroupingDetails('', '')

    # Based on specified time period, add the corresponding column
    if query.time_periods == BillingTimePeriods.DAY:
        result = TimeGroupingDetails(
            field=f'FORMAT_DATE("%Y-%m-%d", {time_column}) as day, ',
            formula='PARSE_DATE("%Y-%m-%d", day) as day, ',
        )
    elif query.time_periods == BillingTimePeriods.WEEK:
        result = TimeGroupingDetails(
            field=f'FORMAT_DATE("%Y%W", {time_column}) as day, ',
            formula='PARSE_DATE("%Y%W", day) as day, ',
        )
    elif query.time_periods == BillingTimePeriods.MONTH:
        result = TimeGroupingDetails(
            field=f'FORMAT_DATE("%Y%m", {time_column}) as day, ',
            formula='PARSE_DATE("%Y%m", day) as day, ',
        )
    elif query.time_periods == BillingTimePeriods.INVOICE_MONTH:
        result = TimeGroupingDetails(
            field='invoice_month as day, ', formula='PARSE_DATE("%Y%m", day) as day, '
        )

    return result


def construct_filter(
    name: str, value: Any, is_label: bool = False
) -> tuple[str, bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter]:
    """Based on Filter value, construct filter string and query parameter

    Args:
        name (str): Filter name
        value (Any): Filter value
        is_label (bool, optional): Is filter a label?. Defaults to False.

    Returns:
        tuple[str, bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter]
    """
    compare = '='
    b1, b2 = '', ''
    param_type = bigquery.ScalarQueryParameter
    key = name.replace('-', '_')

    if isinstance(value, list):
        compare = 'IN'
        b1, b2 = 'UNNEST(', ')'
        param_type = bigquery.ArrayQueryParameter

    if is_label:
        name = f'getLabelValue(labels, "{name}")'

    return (
        f'{name} {compare} {b1}@{key}{b2}',
        param_type(key, 'STRING', value),
    )


class BillingBaseTable(BqDbBase):
    """Billing Base Table
    This is abstract class, it should not be instantiated
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_table_name(self):
        """Get table name"""
        pass

    def _execute_query(
        self, query: str, params: list[Any] = [], results_as_list: bool = True
    ) -> list[Any]:
        """Execute query, add BQ labels"""
        job_config = bigquery.QueryJobConfig(query_parameters=params, labels=BQ_LABELS)
        if results_as_list:
            return list(
                self._connection.connection.query(query, job_config=job_config).result()
            )

        # otherwise return as BQ iterator
        return self._connection.connection.query(query, job_config=job_config)

    def _filter_to_optimise_query(self) -> str:
        """Filter string to optimise BQ query"""
        return 'day >= TIMESTAMP(@start_day) AND day <= TIMESTAMP(@last_day)'

    def _last_loaded_day_filter(self) -> str:
        """Last Loaded day filter string"""
        return 'day = TIMESTAMP(@last_loaded_day)'

    def _prepare_time_filters(self, query: BillingTotalCostQueryModel):
        """Prepare time filters"""
        time_column = query.time_column or 'day'
        time_filters = []
        query_parameters = []

        if query.start_date:
            time_filters.append(f'{time_column} >= TIMESTAMP(@start_date)')
            query_parameters.extend(
                [
                    bigquery.ScalarQueryParameter(
                        'start_date', 'STRING', query.start_date
                    ),
                ]
            )
        if query.end_date:
            time_filters.append(f'{time_column} <= TIMESTAMP(@end_date)')
            query_parameters.extend(
                [
                    bigquery.ScalarQueryParameter('end_date', 'STRING', query.end_date),
                ]
            )

        return time_filters, query_parameters

    def _prepare_filter_str(self, query: BillingTotalCostQueryModel):
        """Prepare filter string"""
        and_filters, query_parameters = self._prepare_time_filters(query)

        # No additional filters
        filters = []
        if not query.filters:
            filter_str = 'WHERE ' + ' AND '.join(and_filters) if and_filters else ''
            return filter_str, query_parameters

        # Add each of the filters in the query
        for filter_key, filter_value in query.filters.items():
            col_name = str(filter_key.value)

            if not isinstance(filter_value, dict):
                filter_, query_param = construct_filter(col_name, filter_value)
                filters.append(filter_)
                query_parameters.append(query_param)
            else:
                for label_key, label_value in filter_value.items():
                    filter_, query_param = construct_filter(
                        label_key, label_value, True
                    )
                    filters.append(filter_)
                    query_parameters.append(query_param)

        if query.filters_op == 'OR':
            if filters:
                and_filters.append('(' + ' OR '.join(filters) + ')')
        else:
            # if not specified, default to AND
            and_filters.extend(filters)

        filter_str = 'WHERE ' + ' AND '.join(and_filters) if and_filters else ''
        return filter_str, query_parameters

    def _convert_output(self, query_job_result):
        """Convert query result to json"""
        if not query_job_result or query_job_result.result().total_rows == 0:
            # return empty list if no record found
            return []

        records = query_job_result.result()
        results = []

        def transform_labels(row):
            return {r['key']: r['value'] for r in row}

        for record in records:
            drec = dict(record)
            if 'labels' in drec:
                drec.update(transform_labels(drec['labels']))

            results.append(drec)

        return results

    async def _budgets_by_gcp_project(
        self, field: BillingColumn, is_current_month: bool
    ) -> dict[str, float]:
        """
        Get budget for gcp-projects
        """
        if field != BillingColumn.GCP_PROJECT or not is_current_month:
            # only projects have budget and only for current month
            return {}

        _query = f"""
        WITH t AS (
            SELECT gcp_project, MAX(created_at) as last_created_at
            FROM `{BQ_BUDGET_VIEW}`
            GROUP BY 1
        )
        SELECT t.gcp_project, d.budget
        FROM t inner join `{BQ_BUDGET_VIEW}` d
        ON d.gcp_project = t.gcp_project AND d.created_at = t.last_created_at
        """

        query_job_result = self._execute_query(_query)
        if query_job_result:
            return {row.gcp_project: row.budget for row in query_job_result}

        return {}

    async def _last_loaded_day(self):
        """Get the most recent fully loaded day in db
        Go 2 days back as the data is not always available for the current day
        1 day back is not enough
        """

        _query = f"""
        SELECT TIMESTAMP_ADD(MAX(day), INTERVAL -2 DAY) as last_loaded_day
        FROM `{self.get_table_name()}`
        WHERE day > TIMESTAMP_ADD(
            CURRENT_TIMESTAMP(), INTERVAL @days DAY
        )
        """

        query_parameters = [
            bigquery.ScalarQueryParameter('days', 'INT64', -int(BQ_DAYS_BACK_OPTIMAL)),
        ]
        query_job_result = self._execute_query(_query, query_parameters)

        if query_job_result:
            return str(query_job_result[0].last_loaded_day)

        return None

    async def _prepare_daily_cost_subquery(self, field, query_params, last_loaded_day):
        """prepare daily cost subquery"""

        daily_cost_field = ', day.cost as daily_cost'
        daily_cost_join = f"""LEFT JOIN (
            SELECT
                {field.value} as field,
                cost_category,
                SUM(cost) as cost
            FROM
            `{self.get_table_name()}`
            WHERE {self._last_loaded_day_filter()}
            GROUP BY
                field,
                cost_category
        ) day
        ON month.field = day.field
        AND month.cost_category = day.cost_category
        """

        query_params.append(
            bigquery.ScalarQueryParameter('last_loaded_day', 'STRING', last_loaded_day),
        )
        return (query_params, daily_cost_field, daily_cost_join)

    async def _execute_running_cost_query(
        self,
        field: BillingColumn,
        invoice_month: str | None = None,
    ):
        """
        Run query to get running cost of selected field
        """
        # check if invoice month is valid first
        if not invoice_month or not re.match(r'^\d{6}$', invoice_month):
            raise ValueError('Invalid invoice month')

        invoice_month_date = datetime.strptime(invoice_month, '%Y%m')
        if invoice_month != invoice_month_date.strftime('%Y%m'):
            raise ValueError('Invalid invoice month')

        # get start day and current day for given invoice month
        # This is to optimise the query, BQ view is partitioned by day
        # and not by invoice month
        start_day_date, last_day_date = get_invoice_month_range(invoice_month_date)
        start_day = start_day_date.strftime('%Y-%m-%d')
        last_day = last_day_date.strftime('%Y-%m-%d')

        # start_day and last_day are in to optimise the query
        query_params = [
            bigquery.ScalarQueryParameter('start_day', 'STRING', start_day),
            bigquery.ScalarQueryParameter('last_day', 'STRING', last_day),
        ]

        current_day = datetime.now().strftime('%Y-%m-%d')
        is_current_month = last_day >= current_day
        last_loaded_day = None

        if is_current_month:
            # Only current month can have last 24 hours cost
            # Last 24H in UTC time
            # Find the last fully loaded day in the view
            last_loaded_day = await self._last_loaded_day()
            (
                query_params,
                daily_cost_field,
                daily_cost_join,
            ) = await self._prepare_daily_cost_subquery(
                field, query_params, last_loaded_day
            )
        else:
            # Do not calculate last 24H cost
            daily_cost_field = ', NULL as daily_cost'
            daily_cost_join = ''

        _query = f"""
        SELECT
            CASE WHEN month.field IS NULL THEN 'N/A' ELSE month.field END as field,
            month.cost_category,
            month.cost as monthly_cost
            {daily_cost_field}
        FROM
        (
            SELECT
            {field.value} as field,
            cost_category,
            SUM(cost) as cost
            FROM
            `{self.get_table_name()}`
            WHERE {self._filter_to_optimise_query()}
            AND invoice_month = @invoice_month
            GROUP BY
            field,
            cost_category
            HAVING cost > 0.1
        ) month
        {daily_cost_join}
        ORDER BY field ASC, daily_cost DESC, monthly_cost DESC;
        """

        query_params.append(
            bigquery.ScalarQueryParameter('invoice_month', 'STRING', invoice_month)
        )

        return (
            is_current_month,
            last_loaded_day,
            self._execute_query(_query, query_params),
        )

    async def _append_total_running_cost(
        self,
        field: BillingColumn,
        is_current_month: bool,
        last_loaded_day: str | None,
        total_monthly: dict,
        total_daily: dict,
        total_monthly_category: dict,
        total_daily_category: dict,
        results: list[BillingCostBudgetRecord],
    ) -> list[BillingCostBudgetRecord]:
        """
        Add total row: compute + storage to the results
        """
        # construct ALL fields details
        all_details = []
        for cat, mth_cost in total_monthly_category.items():
            all_details.append(
                {
                    'cost_group': abbrev_cost_category(cat),
                    'cost_category': cat,
                    'daily_cost': total_daily_category[cat]
                    if is_current_month
                    else None,
                    'monthly_cost': mth_cost,
                }
            )

        # add total row: compute + storage
        results.append(
            BillingCostBudgetRecord.from_json(
                {
                    'field': f'{BillingColumn.generate_all_title(field)}',
                    'total_monthly': (
                        total_monthly['C']['ALL'] + total_monthly['S']['ALL']
                    ),
                    'total_daily': (total_daily['C']['ALL'] + total_daily['S']['ALL'])
                    if is_current_month
                    else None,
                    'compute_monthly': total_monthly['C']['ALL'],
                    'compute_daily': (total_daily['C']['ALL'])
                    if is_current_month
                    else None,
                    'storage_monthly': total_monthly['S']['ALL'],
                    'storage_daily': (total_daily['S']['ALL'])
                    if is_current_month
                    else None,
                    'details': all_details,
                    'last_loaded_day': last_loaded_day,
                }
            )
        )

        return results

    async def _append_running_cost_records(
        self,
        field: BillingColumn,
        is_current_month: bool,
        last_loaded_day: str | None,
        total_monthly: dict,
        total_daily: dict,
        field_details: dict,
        results: list[BillingCostBudgetRecord],
    ) -> list[BillingCostBudgetRecord]:
        """
        Add all the selected field rows: compute + storage to the results
        """
        # get budget map per gcp project
        budgets_per_gcp_project = await self._budgets_by_gcp_project(
            field, is_current_month
        )

        # add rows by field
        for key, details in field_details.items():
            compute_daily = total_daily['C'][key] if key in total_daily['C'] else 0
            storage_daily = total_daily['S'][key] if key in total_daily['S'] else 0
            compute_monthly = (
                total_monthly['C'][key] if key in total_monthly['C'] else 0
            )
            storage_monthly = (
                total_monthly['S'][key] if key in total_monthly['S'] else 0
            )
            monthly = compute_monthly + storage_monthly
            budget_monthly = budgets_per_gcp_project.get(key)

            results.append(
                BillingCostBudgetRecord.from_json(
                    {
                        'field': key,
                        'total_monthly': monthly,
                        'total_daily': (compute_daily + storage_daily)
                        if is_current_month
                        else None,
                        'compute_monthly': compute_monthly,
                        'compute_daily': compute_daily,
                        'storage_monthly': storage_monthly,
                        'storage_daily': storage_daily,
                        'details': details,
                        'budget_spent': 100 * monthly / budget_monthly
                        if budget_monthly
                        else None,
                        'budget': budget_monthly,
                        'last_loaded_day': last_loaded_day,
                    }
                )
            )

        return results

    async def get_total_cost(
        self,
        query: BillingTotalCostQueryModel,
    ) -> list[dict] | None:
        """
        Get Total cost of selected fields for requested time interval from BQ view
        """
        if not query.start_date or not query.end_date or not query.fields:
            raise ValueError('Date and Fields are required')

        # Get columns to group by and check view to use
        grp_columns = []
        for field in query.fields:
            col_name = str(field.value)
            if not BillingColumn.can_group_by(field):
                # if the field cannot be grouped by, skip it
                continue

            grp_columns.append(col_name)

        grp_selected = ','.join(grp_columns)
        fields_selected = ','.join(
            (field.value for field in query.fields if field != BillingColumn.COST)
        )

        # prepare grouping by time periods
        time_group = TimeGroupingDetails('', '')
        if query.time_periods or query.time_column:
            # remove existing day column, if added to fields
            # this is to prevent duplicating various time periods in one query
            # if BillingColumn.DAY in query.fields:
            #     columns.remove(BillingColumn.DAY)
            time_group = prepare_time_periods(query)

        filter_str, query_parameters = self._prepare_filter_str(query)

        # construct order by
        order_by_cols = []
        if query.order_by:
            for order_field, reverse in query.order_by.items():
                col_name = str(order_field.value)
                col_order = 'DESC' if reverse else 'ASC'
                order_by_cols.append(f'{col_name} {col_order}')

        order_by_str = f'ORDER BY {",".join(order_by_cols)}' if order_by_cols else ''

        group_by = f'GROUP BY day, {grp_selected}' if query.group_by else ''
        cost = 'SUM(cost) as cost' if query.group_by else 'cost'

        _query = f"""
        CREATE TEMP FUNCTION getLabelValue(
            labels ARRAY<STRUCT<key STRING, value STRING>>, label STRING
        ) AS (
            (SELECT value FROM UNNEST(labels) WHERE key = label LIMIT 1)
        );

        WITH t AS (
            SELECT {time_group.field}{fields_selected}, {cost}
            FROM `{self.get_table_name()}`
            {filter_str}
            {group_by}
            {order_by_str}
        )
        SELECT {time_group.formula}{fields_selected}, cost FROM t
        """

        # append LIMIT and OFFSET if present
        if query.limit:
            _query += ' LIMIT @limit_val'
            query_parameters.append(
                bigquery.ScalarQueryParameter('limit_val', 'INT64', query.limit)
            )
        if query.offset:
            _query += ' OFFSET @offset_val'
            query_parameters.append(
                bigquery.ScalarQueryParameter('offset_val', 'INT64', query.offset)
            )

        query_job_result = self._execute_query(
            _query, query_parameters, results_as_list=False
        )
        return self._convert_output(query_job_result)

    async def get_running_cost(
        self,
        field: BillingColumn,
        invoice_month: str | None = None,
    ) -> list[BillingCostBudgetRecord]:
        """
        Get currently running cost of selected field
        """

        # accept only Topic, Dataset or Project at this stage
        if field not in (
            BillingColumn.TOPIC,
            BillingColumn.GCP_PROJECT,
            BillingColumn.DATASET,
            BillingColumn.STAGE,
            BillingColumn.COMPUTE_CATEGORY,
            BillingColumn.WDL_TASK_NAME,
            BillingColumn.CROMWELL_SUB_WORKFLOW_NAME,
            BillingColumn.NAMESPACE,
        ):
            raise ValueError(
                'Invalid field only topic, dataset, gcp-project, compute_category, '
                'wdl_task_name, cromwell_sub_workflow_name & namespace are allowed'
            )

        (
            is_current_month,
            last_loaded_day,
            query_job_result,
        ) = await self._execute_running_cost_query(field, invoice_month)
        if not query_job_result:
            # return empty list
            return []

        # prepare data
        results: list[BillingCostBudgetRecord] = []

        # reformat last_loaded_day if present
        last_loaded_day = reformat_datetime(
            last_loaded_day, '%Y-%m-%d %H:%M:%S+00:00', '%b %d'
        )

        total_monthly: dict[str, Counter[str]] = defaultdict(Counter)
        total_daily: dict[str, Counter[str]] = defaultdict(Counter)
        field_details: dict[str, list[Any]] = defaultdict(list)
        total_monthly_category: Counter[str] = Counter()
        total_daily_category: Counter[str] = Counter()

        for row in query_job_result:
            if row.field not in field_details:
                field_details[row.field] = []

            cost_group = abbrev_cost_category(row.cost_category)

            field_details[row.field].append(
                {
                    'cost_group': cost_group,
                    'cost_category': row.cost_category,
                    'daily_cost': row.daily_cost if is_current_month else None,
                    'monthly_cost': row.monthly_cost,
                }
            )

            total_monthly_category[row.cost_category] += row.monthly_cost
            if row.daily_cost:
                total_daily_category[row.cost_category] += row.daily_cost

            # cost groups totals
            total_monthly[cost_group]['ALL'] += row.monthly_cost
            total_monthly[cost_group][row.field] += row.monthly_cost
            if row.daily_cost and is_current_month:
                total_daily[cost_group]['ALL'] += row.daily_cost
                total_daily[cost_group][row.field] += row.daily_cost

        # add total row: compute + storage
        results = await self._append_total_running_cost(
            field,
            is_current_month,
            last_loaded_day,
            total_monthly,
            total_daily,
            total_monthly_category,
            total_daily_category,
            results,
        )

        # add rest of the records: compute + storage
        results = await self._append_running_cost_records(
            field,
            is_current_month,
            last_loaded_day,
            total_monthly,
            total_daily,
            field_details,
            results,
        )

        return results
