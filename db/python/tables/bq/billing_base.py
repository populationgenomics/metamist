import re
from abc import ABCMeta, abstractmethod
from collections import Counter, defaultdict, namedtuple
from datetime import datetime
from typing import Any

from google.cloud import bigquery

from api.settings import BQ_BUDGET_VIEW, BQ_COST_PER_TB, BQ_DAYS_BACK_OPTIMAL
from api.utils.dates import get_invoice_month_range, reformat_datetime
from db.python.gcp_connect import BqDbBase
from db.python.tables.bq.billing_filter import BillingFilter
from db.python.tables.bq.function_bq_filter import FunctionBQFilter
from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from models.enums import BillingTimeColumn, BillingTimePeriods
from models.models import (
    BillingColumn,
    BillingCostBudgetRecord,
    BillingCostDetailsRecord,
    BillingTotalCostQueryModel,
)

# Label added to each Billing Big Query request,
# so we can track the cost of metamist-api BQ usage
BQ_LABELS = {'source': 'metamist-api'}


# Day Time details used in grouping and parsing formulas
TimeGroupingDetails = namedtuple(
    'TimeGroupingDetails', ['field', 'formula', 'separator']
)

# constants to abbrevate (S)tores and (C)ompute
STORAGE = 'S'
COMPUTE = 'C'


def abbrev_cost_category(cost_category: str) -> str:
    """abbreviate cost category"""
    return STORAGE if cost_category.startswith('Cloud Storage') else COMPUTE


def prepare_time_periods(
    query: BillingTotalCostQueryModel,
) -> TimeGroupingDetails:
    """Prepare Time periods grouping and parsing formulas"""
    time_column: BillingTimeColumn = query.time_column or BillingTimeColumn.DAY

    # Based on specified time period, add the corresponding column
    if query.time_periods == BillingTimePeriods.DAY:
        return TimeGroupingDetails(
            field=f'FORMAT_DATE("%Y-%m-%d", {time_column.value}) as day',
            formula='PARSE_DATE("%Y-%m-%d", day) as day',
            separator=',',
        )

    if query.time_periods == BillingTimePeriods.WEEK:
        return TimeGroupingDetails(
            field=f'FORMAT_DATE("%Y%W", {time_column.value}) as day',
            formula='PARSE_DATE("%Y%W", day) as day',
            separator=',',
        )

    if query.time_periods == BillingTimePeriods.MONTH:
        return TimeGroupingDetails(
            field=f'FORMAT_DATE("%Y%m", {time_column.value}) as day',
            formula='PARSE_DATE("%Y%m", day) as day',
            separator=',',
        )

    if query.time_periods == BillingTimePeriods.INVOICE_MONTH:
        return TimeGroupingDetails(
            field='invoice_month as day',
            formula='PARSE_DATE("%Y%m", day) as day',
            separator=',',
        )

    return TimeGroupingDetails('', '', '')


def time_optimisation_parameter() -> bigquery.ScalarQueryParameter:
    """
    BQ tables and views are partitioned by day, to avoid full scans
    we need to limit the amount of data scanned
    """
    return bigquery.ScalarQueryParameter('days', 'INT64', -int(BQ_DAYS_BACK_OPTIMAL))


class BillingBaseTable(BqDbBase):
    """Billing Base Table
    This is abstract class, it should not be instantiated
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_table_name(self):
        """Get table name"""
        raise NotImplementedError('Calling Abstract method directly')

    def _execute_query(
        self, query: str, params: list[Any] | None = None, results_as_list: bool = True
    ) -> (
        list[Any] | bigquery.table.RowIterator | bigquery.table._EmptyRowIterator | None
    ):
        """Execute query, add BQ labels"""
        if params:
            job_config = bigquery.QueryJobConfig(
                query_parameters=params, labels=BQ_LABELS
            )
        else:
            job_config = bigquery.QueryJobConfig(labels=BQ_LABELS)

        # We need to dry run to calulate the costs
        # executing query does not provide the cost
        # more info here:
        # https://stackoverflow.com/questions/58561153/what-is-the-python-api-i-can-use-to-calculate-the-cost-of-a-bigquery-query/58561358#58561358
        job_config.dry_run = True
        job_config.use_query_cache = False
        query_job = self._connection.connection.query(query, job_config=job_config)

        # This should be thread/async safe as each request
        # creates a new connection instance
        # and queries per requests are run in sequencial order,
        # waiting for the previous one to finish
        self._connection.cost += (
            query_job.total_bytes_processed / 1024**4
        ) * BQ_COST_PER_TB

        # now execute the query
        job_config.dry_run = False
        job_config.use_query_cache = True
        query_job = self._connection.connection.query(query, job_config=job_config)
        if results_as_list:
            return list(query_job.result())

        # otherwise return as BQ iterator
        return query_job

    @staticmethod
    def _query_to_partitioned_filter(
        query: BillingTotalCostQueryModel,
    ) -> BillingFilter:
        """
        By default views are partitioned by 'day',
        if different then overwrite in the subclass
        """
        billing_filter = query.to_filter()

        # initial partition filter
        billing_filter.day = GenericBQFilter[datetime](
            gte=(
                datetime.strptime(query.start_date, '%Y-%m-%d')
                if query.start_date
                else None
            ),
            lte=(
                datetime.strptime(query.end_date, '%Y-%m-%d')
                if query.end_date
                else None
            ),
        )
        return billing_filter

    @staticmethod
    def _filter_to_optimise_query() -> str:
        """Filter string to optimise BQ query"""
        return 'day >= TIMESTAMP(@start_day) AND day <= TIMESTAMP(@last_day)'

    @staticmethod
    def _last_loaded_day_filter() -> str:
        """Last Loaded day filter string"""
        return 'day = TIMESTAMP(@last_loaded_day)'

    @staticmethod
    def _convert_output(query_job_result):
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
            GROUP BY gcp_project
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
            time_optimisation_parameter(),
        ]
        query_job_result = self._execute_query(_query, query_parameters)

        if query_job_result:
            return str(query_job_result[0].last_loaded_day)

        return None

    def _prepare_daily_cost_subquery(self, field, query_params, last_loaded_day):
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
            ) = self._prepare_daily_cost_subquery(field, query_params, last_loaded_day)
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

    @staticmethod
    async def _append_total_running_cost(
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
                BillingCostDetailsRecord(
                    cost_group=abbrev_cost_category(cat),
                    cost_category=cat,
                    daily_cost=total_daily_category[cat] if is_current_month else None,
                    monthly_cost=mth_cost,
                )
            )

        # add total row: compute + storage
        results.append(
            BillingCostBudgetRecord(
                field=f'{BillingColumn.generate_all_title(field)}',
                total_monthly=(
                    total_monthly[COMPUTE]['ALL'] + total_monthly[STORAGE]['ALL']
                ),
                total_daily=(
                    (total_daily[COMPUTE]['ALL'] + total_daily[STORAGE]['ALL'])
                    if is_current_month
                    else None
                ),
                compute_monthly=total_monthly[COMPUTE]['ALL'],
                compute_daily=(
                    (total_daily[COMPUTE]['ALL']) if is_current_month else None
                ),
                storage_monthly=total_monthly[STORAGE]['ALL'],
                storage_daily=(
                    (total_daily[STORAGE]['ALL']) if is_current_month else None
                ),
                details=all_details,
                budget_spent=None,
                budget=None,
                last_loaded_day=last_loaded_day,
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
            compute_daily = (
                total_daily[COMPUTE][key] if key in total_daily[COMPUTE] else 0
            )
            storage_daily = (
                total_daily[STORAGE][key] if key in total_daily[STORAGE] else 0
            )
            compute_monthly = (
                total_monthly[COMPUTE][key] if key in total_monthly[COMPUTE] else 0
            )
            storage_monthly = (
                total_monthly[STORAGE][key] if key in total_monthly[STORAGE] else 0
            )
            monthly = compute_monthly + storage_monthly
            budget_monthly = budgets_per_gcp_project.get(key)

            results.append(
                BillingCostBudgetRecord.from_json(
                    {
                        'field': key,
                        'total_monthly': monthly,
                        'total_daily': (
                            (compute_daily + storage_daily)
                            if is_current_month
                            else None
                        ),
                        'compute_monthly': compute_monthly,
                        'compute_daily': compute_daily,
                        'storage_monthly': storage_monthly,
                        'storage_daily': storage_daily,
                        'details': details,
                        'budget_spent': (
                            100 * monthly / budget_monthly if budget_monthly else None
                        ),
                        'budget': budget_monthly,
                        'last_loaded_day': last_loaded_day,
                    }
                )
            )

        return results

    @staticmethod
    def _prepare_order_by_string(order_by: dict[BillingColumn, bool] | None) -> str:
        """Prepare order by string"""
        if not order_by:
            return ''

        order_by_cols = []
        for order_field, reverse in order_by.items():
            col_name = str(order_field.value)
            col_order = 'DESC' if reverse else 'ASC'
            order_by_cols.append(f'{col_name} {col_order}')

        return f'ORDER BY {",".join(order_by_cols)}' if order_by_cols else ''

    @staticmethod
    def _prepare_aggregation(query: BillingTotalCostQueryModel) -> tuple[str, str]:
        """Prepare both fields for aggregation and group by string"""
        # Get columns to group by

        # if group by is populated, then we need to group by day as well
        grp_columns = ['day'] if query.group_by else []

        for field in query.fields:
            col_name = str(field.value)
            if not BillingColumn.can_group_by(field):
                # if the field cannot be grouped by, skip it
                continue

            # append to potential columns to group by
            grp_columns.append(col_name)

        fields_selected = ','.join(
            (field.value for field in query.fields if field != BillingColumn.COST)
        )

        grp_selected = ','.join(grp_columns)
        group_by = f'GROUP BY {grp_selected}' if query.group_by else ''

        return fields_selected, group_by

    @staticmethod
    def _prepare_labels_function(query: BillingTotalCostQueryModel):
        if not query.filters:
            return None

        if BillingColumn.LABELS in query.filters and isinstance(
            query.filters[BillingColumn.LABELS], dict
        ):
            # prepare labels as function filters, parameterized both sides
            func_filter = FunctionBQFilter(
                name='getLabelValue',
                implementation="""
                    CREATE TEMP FUNCTION getLabelValue(
                        labels ARRAY<STRUCT<key STRING, value STRING>>, label STRING
                    ) AS (
                        (SELECT value FROM UNNEST(labels) WHERE key = label LIMIT 1)
                    );
                """,
            )
            func_filter.to_sql(
                BillingColumn.LABELS,
                query.filters[BillingColumn.LABELS],
                query.filters_op,
            )
            return func_filter

        # otherwise
        return None

    async def get_total_cost(
        self,
        query: BillingTotalCostQueryModel,
    ) -> list[dict] | None:
        """
        Get Total cost of selected fields for requested time interval from BQ views
        """
        if not query.start_date or not query.end_date or not query.fields:
            raise ValueError('Date and Fields are required')

        # Get columns to select and to group by
        fields_selected, group_by = self._prepare_aggregation(query)

        # construct order by
        order_by_str = self._prepare_order_by_string(query.order_by)

        # prepare grouping by time periods
        time_group = TimeGroupingDetails('', '', '')
        if query.time_periods or query.time_column:
            time_group = prepare_time_periods(query)

        # overrides time specific fields with relevant time column name
        query_filter = self._query_to_partitioned_filter(query)

        # prepare where string and SQL parameters
        where_str, sql_parameters = query_filter.to_sql()

        # extract only BQ Query parameter, keys are not used in BQ SQL
        # have to declare empty list first as linting is not happy
        query_parameters: list[
            bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter
        ] = []
        query_parameters.extend(sql_parameters.values())

        # prepare labels as function filters if present
        func_filter = self._prepare_labels_function(query)
        if func_filter:
            # extend where_str and query_parameters
            query_parameters.extend(func_filter.func_sql_parameters)

            # now join Prepared Where with Labels Function Where
            where_str = ' AND '.join([where_str, func_filter.func_where])

        # if group by is populated, then we need SUM the cost, otherwise raw cost
        cost_column = 'SUM(cost) as cost' if query.group_by else 'cost'

        if where_str:
            # Where is not empty, prepend with WHERE
            where_str = f'WHERE {where_str}'

        _query = f"""
        {func_filter.func_implementation if func_filter else ''}

        WITH t AS (
            SELECT {time_group.field}{time_group.separator} {fields_selected},
                {cost_column}
            FROM `{self.get_table_name()}`
            {where_str}
            {group_by}
            {order_by_str}
        )
        SELECT {time_group.formula}{time_group.separator} {fields_selected}, cost FROM t
        """

        # append min cost condition
        if query.min_cost:
            _query += ' WHERE cost > @min_cost'
            query_parameters.append(
                bigquery.ScalarQueryParameter('min_cost', 'FLOAT64', query.min_cost)
            )

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
