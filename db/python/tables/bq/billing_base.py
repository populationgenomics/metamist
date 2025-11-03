# pylint: disable=too-many-lines, too-many-nested-blocks, too-many-branches
import logging
import re
from abc import ABCMeta, abstractmethod
from collections import Counter, defaultdict
from datetime import date, datetime
from typing import Any

from google.cloud import bigquery

from api.settings import (
    BQ_AGGREG_EXT_VIEW,
    BQ_AGGREG_VIEW,
    BQ_BATCHES_VIEW,
    BQ_BUDGET_VIEW,
    BQ_COST_PER_TB,
)
from api.utils.dates import get_invoice_month_range, reformat_datetime
from api.utils.db import (
    Connection,
)
from db.python.gcp_connect import BqDbBase
from db.python.tables.bq.billing_filter import BillingFilter
from db.python.tables.bq.billing_utils import (
    TimeGroupingDetails,
    abbrev_cost_category,
    append_detailed_cost_records,
    append_sample_cost_record,
    append_total_running_cost,
    convert_output,
    filter_to_optimise_query,
    get_topic_to_project_map,
    last_loaded_day_filter,
    prepare_aggregation,
    prepare_labels_function,
    prepare_order_by_string,
    prepare_time_periods,
    time_optimisation_parameter,
)
from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from db.python.tables.sample import SampleTable
from models.models import (
    BillingColumn,
    BillingCostBudgetRecord,
    BillingRunningCostQueryModel,
    BillingTotalCostQueryModel,
)
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_transform_to_raw,
)

# Label added to each Billing Big Query request,
# so we can track the cost of metamist-api BQ usage
BQ_LABELS = {'source': 'metamist-api'}

# Set up logging for debugging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
            WHERE {last_loaded_day_filter()}
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

    async def _execute_running_cost_query_with_filters(
        self,
        query: BillingRunningCostQueryModel,
    ):
        """
        Run query to get running cost of selected field with filtering support
        """

        # check if invoice month is valid first
        if not query.invoice_month or not re.match(r'^\d{6}$', query.invoice_month):
            raise ValueError('Invalid invoice month')

        invoice_month_date = datetime.strptime(query.invoice_month, '%Y%m')
        if query.invoice_month != invoice_month_date.strftime('%Y%m'):
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
            ) = self._prepare_daily_cost_subquery(
                query.field, query_params, last_loaded_day
            )
        else:
            # Do not calculate last 24H cost
            daily_cost_field = ', NULL as daily_cost'
            daily_cost_join = ''

        # prepare filter for query
        query_filter = query.to_filter()
        where_str, sql_parameters = query_filter.to_sql()

        # extract only BQ Query parameter, keys are not used in BQ SQL
        query_parameters: list[
            bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter
        ] = []
        query_parameters.extend(sql_parameters.values())
        query_parameters.extend(query_params)

        # Add additional filters to where clause
        additional_filters = [filter_to_optimise_query()]
        if where_str:
            additional_filters.append(where_str)

        where_clause = ' AND '.join(additional_filters)

        _query = f"""
        SELECT
            CASE WHEN month.field IS NULL THEN 'N/A' ELSE month.field END as field,
            month.cost_category,
            month.cost as monthly_cost
            {daily_cost_field}
        FROM
        (
            SELECT
            {query.field.value} as field,
            cost_category,
            SUM(cost) as cost
            FROM
            `{self.get_table_name()}`
            WHERE {where_clause}
            AND invoice_month = @invoice_month
            GROUP BY
            field,
            cost_category
            HAVING cost > 0.1
        ) month
        {daily_cost_join}
        ORDER BY field ASC, daily_cost DESC, monthly_cost DESC;
        """

        query_parameters.append(
            bigquery.ScalarQueryParameter(
                'invoice_month', 'STRING', query.invoice_month
            )
        )

        return (
            is_current_month,
            last_loaded_day,
            self._execute_query(_query, query_parameters),
        )

    async def get_processed_sequencing_groups_per_month(
        self,
    ) -> dict[date, list[int]]:
        """
        Call BQ table to get all processed sequencing groups per month
        some of the sequencing_group values could be just rubbish
        so we only return those with length > 3
        3 is arbitrary, but should be enough to filter out most of the rubbish
        """
        _query = f"""
            WITH total AS (
            SELECT DISTINCT
                t.invoice_month,
                sequencing_group AS sg
            FROM
                `{BQ_BATCHES_VIEW}` AS t,
                UNNEST(SPLIT(t.sequencing_group, ',')) AS sequencing_group
            )
            SELECT invoice_month, STRING_AGG(sg, ',') as all_sequencing_groups
            FROM total
            WHERE LENGTH(sg) > 3
            GROUP BY 1
        """
        result: dict[date, list[int]] = {}
        query_job_result = self._execute_query(_query)
        if query_job_result:
            # need to reformat sg group ids into raw format
            # skip those sg ids that are invalid
            for row in query_job_result:
                valid_sg_ids = []
                for sg in str(row.all_sequencing_groups).split(','):
                    try:
                        valid_sg_ids.append(sequencing_group_id_transform_to_raw(sg))
                    except ValueError:
                        # skip any invalid sg id
                        continue
                result[
                    date.fromisoformat(
                        f'{row.invoice_month[:4]}-{row.invoice_month[4:]}-01'
                    )
                ] = valid_sg_ids

        return result

    async def append_sample_cost(
        self, connection: Connection | None = None, results: list[dict] | None = None
    ) -> list[dict] | None:
        """
        For each topic in results, calculate number of samples per metamist project
        and divide the cost by number of samples to get average sample storage cost
        """
        if not connection or not results:
            return results

        # get project_id to dataset/topic mapping
        topic_to_project = await get_topic_to_project_map(connection)

        # get number of samples per project per month
        samples = SampleTable(connection)
        project_sample_counts_per_month = (
            await samples.get_monthly_samples_count_per_project()
        )
        # if no samples found, return results as is
        if not project_sample_counts_per_month:
            return results

        # get all processes sequencing groups per invoice month
        sg_compute_per_month = await self.get_processed_sequencing_groups_per_month()

        # pass sg_compute_per_month to sample table
        # and per each month get number of samples per project used in compute
        project_sample_compute_counts_per_month = (
            await samples.get_sample_count_per_project_per_month(sg_compute_per_month)
        )

        # for each monthly record with cost_category == 'Cloud Storage' add one record with average_sample_cost
        for row in results:
            topic = row.get('topic')
            invoice_month = row.get('day')
            # only continue if both topic and invoice_month / day are present
            if topic is None or invoice_month is None:
                continue

            # convert topic to project id
            project_id = topic_to_project.get(topic)
            if project_id is None:
                continue

            # append new row based on Cloud Storage cost
            results = await append_sample_cost_record(
                results,
                'Average Sample Storage Cost',
                project_sample_counts_per_month,
                row,
                invoice_month,
                int(project_id),
                include_cost_category='Cloud Storage',
            )

            # aggregate all but Cloud Storage costs into one average sample compute cost
            results = await append_sample_cost_record(
                results,
                'Average Sample Compute Cost',
                project_sample_compute_counts_per_month,
                row,
                invoice_month,
                int(project_id),
                exclude_cost_category='Cloud Storage',
            )

        # order results by day, topic, cost_category
        results.sort(
            key=lambda x: (x.get('day'), x.get('topic'), x.get('cost_category'))
        )
        return results

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
        fields_selected, group_by = prepare_aggregation(query)

        # construct order by
        order_by_str = prepare_order_by_string(query.order_by)

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
        func_filter = prepare_labels_function(query)
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
        return convert_output(query_job_result)

    async def get_running_cost_with_filters(
        self,
        query: BillingRunningCostQueryModel,
    ) -> list[BillingCostBudgetRecord]:
        """
        Get currently running cost of selected field with filtering support
        """

        # accept only Topic, Dataset or Project at this stage
        if query.field not in (
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
        ) = await self._execute_running_cost_query_with_filters(query)
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
        results = await append_total_running_cost(
            query.field,
            is_current_month,
            last_loaded_day,
            total_monthly,
            total_daily,
            total_monthly_category,
            total_daily_category,
            results,
        )

        # get budget map per gcp project
        budgets_per_gcp_project = await self._budgets_by_gcp_project(
            query.field, is_current_month
        )

        # add rest of the records: compute + storage
        results = await append_detailed_cost_records(
            budgets_per_gcp_project,
            is_current_month,
            last_loaded_day,
            total_monthly,
            total_daily,
            field_details,
            results,
        )

        return results

    def get_compute_costs_by_seq_groups(
        self,
        batch_ids: dict[str, tuple[datetime, datetime]],
        sequencing_groups: list[str],
        fields_selected: str,
    ) -> list[dict]:
        """
        Get compute costs by sequencing groups
        """
        if not batch_ids:
            return []

        where_filter = []
        # append all batch time filters to optimise the cost of BQ retrieval
        for k, v in batch_ids.items():
            where_filter.append(
                f"(batch_id = '{k}' AND day >= TIMESTAMP('{v[0]}') AND day <= TIMESTAMP('{v[1]}'))"
            )

        query_parameters = [
            bigquery.ArrayQueryParameter(
                'sequencing_groups', 'STRING', sequencing_groups
            )
        ]

        # we do not want to include seqr cost as those are redistributed to other topics
        # if we included them we would be double counting the costs.
        _query = f"""
        WITH ag as (
            SELECT
            invoice_month,
            ar_guid,
            topic,
            stage,
            sequencing_group,
            SUM(cost) as cost,
            SUM(cost)/ARRAY_LENGTH(SPLIT(sequencing_group, ',')) as cost_adj
            FROM `{BQ_AGGREG_EXT_VIEW}`
            WHERE ({' OR '.join(where_filter) if where_filter else '1=1'})
            AND sequencing_group IS NOT NULL
            AND topic <> 'seqr'
            GROUP BY invoice_month,sequencing_group, ar_guid, topic, stage
        ),
        t as (
        SELECT invoice_month, ar_guid, topic, stage, 'Compute' as cost_category, sg as sequencing_group, cost_adj
        FROM ag, UNNEST(SPLIT(UPPER(sequencing_group), ',')) as sg
        WHERE sg IN UNNEST(@sequencing_groups)
        )
        SELECT invoice_month {',' if fields_selected else ''} {fields_selected}, sum(cost_adj) as cost
        FROM t
        GROUP BY invoice_month {',' if fields_selected else ''} {fields_selected}
        """

        query_job_result = self._execute_query(
            _query, query_parameters, results_as_list=False
        )

        return convert_output(query_job_result)

    def get_storage_costs_by_project(
        self, query: BillingTotalCostQueryModel, gcp_project_name: str
    ) -> dict:
        """
        Get storage costs by project
        """
        # overrides time specific fields with relevant time column name
        query_filter = self._query_to_partitioned_filter(query)

        # prepare where string and SQL parameters
        where_str, sql_parameters = query_filter.to_sql()

        # prepare BQ query parameters
        query_parameters: list[
            bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter
        ] = []
        query_parameters.extend(sql_parameters.values())

        # GCP project names can have a number suffix, so can only be matched using LIKE '%'
        # mapping of metamist project vs GCP project is not stored in metamist database or in BQ tables
        # therefore only '%' can be used here
        query_parameters.append(
            bigquery.ScalarQueryParameter(
                'gcp_project', 'STRING', f'{gcp_project_name}%'
            )
        )

        # prepare BQ query, get aggregated storage cost per invoice month
        _query = f"""
        SELECT invoice_month, sum(cost) as cost
        FROM `{BQ_AGGREG_VIEW}`
        WHERE {where_str}
        AND gcp_project LIKE @gcp_project
        AND cost_category = 'Cloud Storage'
        group by invoice_month
        """

        # 3. Get Total cost of cost_category = 'Cloud Storage' per invoice month for the project filtered by start and end date
        query_job_result = self._execute_query(_query, query_parameters)
        storage_cost = {}
        if query_job_result:
            for row in query_job_result:
                storage_cost[row.invoice_month] = row.cost

        return storage_cost
