import re
from abc import ABCMeta, abstractmethod
from collections import Counter, defaultdict, namedtuple
from datetime import datetime
from typing import Any

from google.cloud import bigquery

from api.settings import (
    BQ_AGGREG_EXT_VIEW,
    BQ_AGGREG_VIEW,
    BQ_BATCHES_VIEW,
    BQ_BUDGET_VIEW,
    BQ_COST_PER_TB,
    BQ_DAYS_BACK_OPTIMAL,
    COHORT_PREFIX,
    SAMPLE_PREFIX,
    SEQUENCING_GROUP_PREFIX,
)
from api.utils.dates import get_invoice_month_range, reformat_datetime
from api.utils.db import (
    Connection,
)
from db.python.gcp_connect import BqDbBase
from db.python.tables.bq.billing_filter import BillingFilter
from db.python.tables.bq.function_bq_filter import FunctionBQFilter
from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from db.python.tables.sequencing_group import SequencingGroupTable
from models.enums import BillingTimeColumn, BillingTimePeriods
from models.models import (
    BillingColumn,
    BillingCostBudgetRecord,
    BillingCostDetailsRecord,
    BillingTotalCostQueryModel,
)
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_format,
    sequencing_group_id_transform_to_raw,
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

        print('query', query)
        print('params', params)

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

    async def get_cost_by_sample(
        self,
        connection: Connection,
        query: BillingTotalCostQueryModel,
    ) -> list[dict] | None:
        """
        Get Sample cost of selected fields for requested time interval from BQ views

        SAMPLE_PREFIX , e.g 'XPG'
        SEQUENCING_GROUP_PREFIX , e.g 'CPG'
        COHORT_PREFIX , e.g  'COH'

        """
        print('query:', query)
        if not query.start_date or not query.end_date or not query.filters:
            raise ValueError('Date and Filters are required')

        # Get columns to select and to group by
        fields_selected, _group_by = self._prepare_aggregation(query)

        # 1. Extract all sequencing groups we need to include

        records_filter = query.filters.get(BillingColumn.SEQUENCING_GROUP, [])

        if not records_filter:
            raise ValueError('Sequencing_group containing is required')

        print('sequencing_group_id_format', sequencing_group_id_format(123))

        sequencing_groups = []
        sequencing_groups_as_ids: list[int] = []
        seq_id_map: dict[int, str] = {}
        for r in records_filter:
            if r.startswith(SEQUENCING_GROUP_PREFIX):
                sequencing_groups.append(r)
                # convert SEQ to IDs
                seq_id = sequencing_group_id_transform_to_raw(r)
                seq_id_map[seq_id] = r
                sequencing_groups_as_ids.append(seq_id)
            elif r.startswith(COHORT_PREFIX):
                # TODO: Get all seq groups for this cohort
                continue
            elif r.startswith(SAMPLE_PREFIX):
                # TODO: get all seq groups for this sample
                continue

        # 2. locate which project metamist project_id, and GCP project

        sgt = SequencingGroupTable(connection)
        projects = await sgt.get_projects_by_sequencing_group_ids(
            sequencing_groups_as_ids
        )

        if not projects:
            return []

        if len(projects) > 1:
            raise ValueError('Sequencing_group from one project only')

        # get the project dataset (GCP project names)
        gcp_project_name = connection.project_id_map[list(projects)[0]].dataset
        # TODO temp
        gcp_project_name = 'ourdna-browser'

        # Storage Calculation
        print('gcp_project_name:', gcp_project_name)

        # overrides time specific fields with relevant time column name
        query.filters = {BillingColumn.DAY: query.filters.get(BillingColumn.DAY, None)}
        query_filter = self._query_to_partitioned_filter(query)

        # prepare where string and SQL parameters
        where_str, sql_parameters = query_filter.to_sql()

        print('query_filter', query_filter)
        print('where_str', where_str)
        print('sql_parameters', sql_parameters)

        # 3. Get Total cost of cost_category = 'Cloud Storage' per invoice month for the project filtered by start and end date
        storage_cost = {}
        query_parameters: list[
            bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter
        ] = []
        query_parameters.extend(sql_parameters.values())

        # GCP project names can have a number suffix, so can only be matched using LIKE '%'
        query_parameters.append(
            bigquery.ScalarQueryParameter(
                'gcp_project', 'STRING', f'{gcp_project_name}%'
            )
        )

        _query = f"""
        SELECT invoice_month, sum(cost) as cost
        FROM `{BQ_AGGREG_VIEW}`
        WHERE {where_str}
        AND gcp_project LIKE @gcp_project
        AND cost_category = 'Cloud Storage'
        group by invoice_month
        """

        query_job_result = self._execute_query(_query, query_parameters)
        if query_job_result:
            for row in query_job_result:
                storage_cost[row.invoice_month] = row.cost

        print('storage_cost', storage_cost)

        # 4. get number of seq group per project
        _query = """
            SELECT COUNT(distinct sg.id) as cnt
            FROM sample s
            INNER JOIN sequencing_group sg ON sg.sample_id = s.id
            WHERE s.project IN :projects
        """
        info_record = await connection.connection.fetch_one(
            _query,
            {
                'projects': list(projects),
            },
        )
        number_of_seq_groups = dict(info_record).get('cnt', 0)

        # 4. Get the cram size per project, total size and count cram files

        _query = """
            SELECT sum(f.size) as total_crams_size, count(*) as cram_files_cnt
            FROM output_file f
            INNER JOIN analysis_outputs o ON o.file_id = f.id
            INNER JOIN analysis a ON a.id = o.analysis_id
            where a.type = 'CRAM'
            AND a.status = 'COMPLETED'
            and a.project IN :projects
        """
        info_record = await connection.connection.fetch_one(
            _query,
            {
                'projects': list(projects),
            },
        )

        total_cram_info = dict(info_record)

        print('total_cram_info', total_cram_info)
        total_crams_size = total_cram_info.get('total_crams_size', 0)
        _cram_files_cnt = total_cram_info.get('cram_files_cnt', 0)

        # 5. Get individual seq group cram size, list all seq even they do not have cram files
        if 'sequencing_group' in fields_selected:
            _query = """
                SELECT sg.id , sum(f.size) as crams_size
                FROM sequencing_group sg
                LEFT JOIN analysis_sequencing_group asg ON asg.sequencing_group_id = sg.id
                LEFT JOIN analysis a ON asg.analysis_id = a.id AND a.type = 'CRAM' AND a.status = 'COMPLETED' AND a.project IN :projects
                LEFT JOIN analysis_outputs o ON a.id = o.analysis_id
                LEFT JOIN output_file f ON o.file_id = f.id
                WHERE sg.id IN :seq_groups
                GROUP BY sg.id
            """
            records = await connection.connection.fetch_all(
                _query,
                {
                    'projects': list(projects),
                    'seq_groups': list(sequencing_groups_as_ids),
                },
            )

            print('records', records)

            sg_storage_cost = []
            for row in records:
                for sk, sv in storage_cost.items():
                    sg_storage_cost.append(
                        {
                            'invoice_month': sk,
                            'cost_category': 'Cloud Storage',
                            'sequencing_group': seq_id_map[row['id']],
                            'cost': (
                                sv * row['crams_size'] / total_crams_size
                                if total_crams_size
                                else sv / number_of_seq_groups
                            ),
                        }
                    )
        else:
            _query = """
                SELECT sum(f.size) as crams_size
                FROM sequencing_group sg
                LEFT JOIN analysis_sequencing_group asg ON asg.sequencing_group_id = sg.id
                LEFT JOIN analysis a ON asg.analysis_id = a.id AND a.type = 'CRAM' AND a.status = 'COMPLETED' AND a.project IN :projects
                LEFT JOIN analysis_outputs o ON a.id = o.analysis_id
                LEFT JOIN output_file f ON o.file_id = f.id
                WHERE sg.id IN :seq_groups
            """
            records = await connection.connection.fetch_all(
                _query,
                {
                    'projects': list(projects),
                    'seq_groups': list(sequencing_groups_as_ids),
                },
            )

            print('records', records)

            sg_storage_cost = []
            for row in records:
                for sk, sv in storage_cost.items():
                    sg_storage_cost.append(
                        {
                            'invoice_month': sk,
                            'cost_category': 'Cloud Storage',
                            'cost': (
                                sv * row['crams_size'] / total_crams_size
                                if total_crams_size
                                else len(list(sequencing_groups_as_ids))
                                * sv
                                / number_of_seq_groups
                            ),
                        }
                    )

        # Compute Calculation

        # TODO temp
        sequencing_groups = ['CPG280370', 'CPG280388']

        # 6. Get all ar-guid with min/max day for each of the seq group

        query_parameters: list[bigquery.ArrayQueryParameter] = [
            bigquery.ArrayQueryParameter(
                'sequencing_groups', 'STRING', sequencing_groups
            )
        ]

        # get all ar-guid with min/max day for each of the seq group
        # sequencing_group can be a comma separated list of seq group ids,
        # so we need to use REGEXP_CONTAINS
        _query = f"""
        SELECT ar_guid, min(min_day) as min_day, max(min_day) as max_day
        FROM `{BQ_BATCHES_VIEW}`
        WHERE REGEXP_CONTAINS(sequencing_group, ARRAY_TO_STRING(@sequencing_groups, '|'))
        group by ar_guid
        """

        ar_guids = {}
        query_job_result = self._execute_query(_query, query_parameters)
        if query_job_result:
            for row in query_job_result:
                ar_guids[row.ar_guid] = (row.min_day, row.max_day)

        print('ar_guids', ar_guids)

        # 7. Get total compute cost associated with ar-guid
        where_filter = []
        for k, v in ar_guids.items():
            where_filter.append(
                f"(ar_guid = '{k}' AND day >= TIMESTAMP('{v[0]}') AND day <= TIMESTAMP('{v[1]}'))"
            )

        _query = f"""
        WITH ag as (
            SELECT 
            invoice_month,
            ar_guid,
            topic,
            stage,
            cost_category,
            sequencing_group,
            SUM(cost) as cost,
            SUM(cost)/ARRAY_LENGTH(SPLIT(sequencing_group, ',')) as cost_adj
            FROM `{BQ_AGGREG_EXT_VIEW}`
            WHERE ({' OR '.join(where_filter) if where_filter else '1=1'})
            AND sequencing_group IS NOT NULL
            GROUP BY invoice_month,sequencing_group, ar_guid, topic, stage, cost_category
        ),
        t as (
        SELECT invoice_month, ar_guid, topic, stage, cost_category, sg as sequencing_group, cost_adj
        FROM ag, UNNEST(SPLIT(sequencing_group, ',')) as sg
        WHERE sg IN UNNEST(@sequencing_groups)
        )
        SELECT invoice_month {',' if fields_selected else ''} {fields_selected}, sum(cost_adj) as cost
        FROM t
        GROUP BY invoice_month {',' if fields_selected else ''} {fields_selected}
        """

        print('_query', _query)

        query_job_result = self._execute_query(
            _query, query_parameters, results_as_list=False
        )

        # 8. Combine all together
        result = self._convert_output(query_job_result)
        result.extend(sg_storage_cost)

        print('result', result)

        return result
