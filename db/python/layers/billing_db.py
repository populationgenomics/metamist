import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

from google.cloud import bigquery

from api.settings import (
    BQ_AGGREG_EXT_VIEW,
    BQ_AGGREG_RAW,
    BQ_AGGREG_VIEW,
    BQ_BUDGET_VIEW,
    BQ_DAYS_BACK_OPTIMAL,
    BQ_GCP_BILLING_VIEW,
)
from api.utils.dates import get_invoice_month_range, reformat_datetime
from db.python.gcp_connect import BqDbBase
from db.python.tables.billing import BillingFilter
from models.models import (
    BillingColumn,
    BillingCostBudgetRecord,
    BillingRowRecord,
    BillingTotalCostQueryModel,
    BillingTotalCostRecord,
)

BQ_LABELS = {'source': 'metamist-api'}


def abbrev_cost_category(cost_category: str) -> str:
    """abbreviate cost category"""
    return 'S' if cost_category == 'Cloud Storage' else 'C'


class BillingDb(BqDbBase):
    """Db layer for billing related routes"""

    async def get_gcp_projects(self):
        """Get all GCP projects in database"""

        # cost of this BQ is 10MB on DEV is minimal, AU$ 0.000008 per query
        # @days is defined by env variable BQ_DAYS_BACK_OPTIMAL
        # this part_time > filter is to limit the amount of data scanned,
        # saving cost for running BQ
        _query = f"""
        SELECT DISTINCT gcp_project
        FROM `{BQ_GCP_BILLING_VIEW}`
        WHERE part_time > TIMESTAMP_ADD(
            CURRENT_TIMESTAMP(), INTERVAL @days DAY
        )
        AND gcp_project IS NOT NULL
        ORDER BY gcp_project ASC;
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    'days', 'INT64', -int(BQ_DAYS_BACK_OPTIMAL)
                ),
            ],
            labels=BQ_LABELS,
        )

        query_job_result = list(
            self._connection.connection.query(_query, job_config=job_config).result()
        )
        if query_job_result:
            return [str(dict(row)['gcp_project']) for row in query_job_result]

        # return empty list if no record found
        return []

    async def get_topics(self):
        """Get all topics in database"""

        # cost of this BQ is 10MB on DEV is minimal, AU$ 0.000008 per query
        # @days is defined by env variable BQ_DAYS_BACK_OPTIMAL
        # this day > filter is to limit the amount of data scanned,
        # saving cost for running BQ
        # aggregated views are partitioned by day
        _query = f"""
        SELECT DISTINCT topic
        FROM `{BQ_AGGREG_VIEW}`
        WHERE day > TIMESTAMP_ADD(
            CURRENT_TIMESTAMP(), INTERVAL @days DAY
        )
        -- TODO put this back when reloading is fixed
        AND NOT topic IN ('seqr', 'hail')
        ORDER BY topic ASC;
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    'days', 'INT64', -int(BQ_DAYS_BACK_OPTIMAL)
                ),
            ],
            labels=BQ_LABELS,
        )

        query_job_result = list(
            self._connection.connection.query(_query, job_config=job_config).result()
        )
        if query_job_result:
            return [str(dict(row)['topic']) for row in query_job_result]

        # return empty list if no record found
        return []

    async def get_invoice_months(self):
        """Get all invoice months in database"""

        _query = f"""
        SELECT DISTINCT FORMAT_DATE("%Y%m", day) as invoice_month
        FROM `{BQ_AGGREG_VIEW}`
        WHERE EXTRACT(day from day) = 1
        ORDER BY invoice_month DESC;
        """

        job_config = bigquery.QueryJobConfig(labels=BQ_LABELS)

        query_job_result = list(
            self._connection.connection.query(_query, job_config=job_config).result()
        )
        if query_job_result:
            return [str(dict(row)['invoice_month']) for row in query_job_result]

        # return empty list if no record found
        return []

    async def get_cost_categories(self):
        """Get all service description in database"""

        # cost of this BQ is 10MB on DEV is minimal, AU$ 0.000008 per query
        # @days is defined by env variable BQ_DAYS_BACK_OPTIMAL
        # this day > filter is to limit the amount of data scanned,
        # saving cost for running BQ
        # aggregated views are partitioned by day
        _query = f"""
        SELECT DISTINCT cost_category
        FROM `{BQ_AGGREG_VIEW}`
        WHERE day > TIMESTAMP_ADD(
            CURRENT_TIMESTAMP(), INTERVAL @days DAY
        )
        ORDER BY cost_category ASC;
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    'days', 'INT64', -int(BQ_DAYS_BACK_OPTIMAL)
                ),
            ],
            labels=BQ_LABELS,
        )

        query_job_result = list(
            self._connection.connection.query(_query, job_config=job_config).result()
        )
        if query_job_result:
            return [str(dict(row)['cost_category']) for row in query_job_result]

        # return empty list if no record found
        return []

    async def get_skus(
        self,
        limit: int | None = None,
        offset: int | None = None,
    ):
        """Get all SKUs in database"""

        # cost of this BQ is 10MB on DEV is minimal, AU$ 0.000008 per query
        # @days is defined by env variable BQ_DAYS_BACK_OPTIMAL
        # this day > filter is to limit the amount of data scanned,
        # saving cost for running BQ
        # aggregated views are partitioned by day
        _query = f"""
        SELECT DISTINCT sku
        FROM `{BQ_AGGREG_VIEW}`
        WHERE day > TIMESTAMP_ADD(
            CURRENT_TIMESTAMP(), INTERVAL @days DAY
        )
        ORDER BY sku ASC
        """

        # append LIMIT and OFFSET if present
        if limit:
            _query += ' LIMIT @limit_val'
        if offset:
            _query += ' OFFSET @offset_val'

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    'days', 'INT64', -int(BQ_DAYS_BACK_OPTIMAL)
                ),
                bigquery.ScalarQueryParameter('limit_val', 'INT64', limit),
                bigquery.ScalarQueryParameter('offset_val', 'INT64', offset),
            ],
            labels=BQ_LABELS,
        )

        query_job_result = list(
            self._connection.connection.query(_query, job_config=job_config).result()
        )
        if query_job_result:
            return [str(dict(row)['sku']) for row in query_job_result]

        # return empty list if no record found
        return []

    async def get_extended_values(self, field: str):
        """
        Get all extended values in database, for specified field.
        Field is one of extended coumns.
        """

        if field not in BillingColumn.extended_cols():
            raise ValueError('Invalid field value')

        # cost of this BQ is 10MB on DEV is minimal, AU$ 0.000008 per query
        # @days is defined by env variable BQ_DAYS_BACK_OPTIMAL
        # this day > filter is to limit the amount of data scanned,
        # saving cost for running BQ
        # aggregated views are partitioned by day
        _query = f"""
        SELECT DISTINCT {field}
        FROM `{BQ_AGGREG_EXT_VIEW}`
        WHERE {field} IS NOT NULL
        AND day > TIMESTAMP_ADD(
            CURRENT_TIMESTAMP(), INTERVAL @days DAY
        )
        ORDER BY 1 ASC;
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    'days', 'INT64', -int(BQ_DAYS_BACK_OPTIMAL)
                ),
            ],
            labels=BQ_LABELS,
        )

        query_job_result = list(
            self._connection.connection.query(_query, job_config=job_config).result()
        )
        if query_job_result:
            return [str(dict(row)[field]) for row in query_job_result]

        # return empty list if no record found
        return []

    async def query(
        self,
        filter_: BillingFilter,
        limit: int = 10,
    ) -> list[BillingRowRecord] | None:
        """Get Billing record from BQ"""

        # TODO: THis function is not going to be used most likely
        # get_total_cost will replace it

        # cost of this BQ is 30MB on DEV,
        # DEV is partition by day and date is required filter params,
        # cost is aprox per query: AU$ 0.000023 per query

        required_fields = [
            filter_.date,
        ]

        if not any(required_fields):
            raise ValueError('Must provide date to filter on')

        # construct filters
        filters = []
        query_parameters = []

        if filter_.topic:
            filters.append('topic IN UNNEST(@topic)')
            query_parameters.append(
                bigquery.ArrayQueryParameter('topic', 'STRING', filter_.topic.in_),
            )

        if filter_.date:
            filters.append('DATE_TRUNC(usage_end_time, DAY) = TIMESTAMP(@date)')
            query_parameters.append(
                bigquery.ScalarQueryParameter('date', 'STRING', filter_.date.eq),
            )

        if filter_.cost_category:
            filters.append('service.description IN UNNEST(@cost_category)')
            query_parameters.append(
                bigquery.ArrayQueryParameter(
                    'cost_category', 'STRING', filter_.cost_category.in_
                ),
            )

        filter_str = 'WHERE ' + ' AND '.join(filters) if filters else ''

        _query = f"""
        SELECT id, topic, service, sku, usage_start_time, usage_end_time, project,
        labels, export_time, cost, currency, currency_conversion_rate, invoice, cost_type
        FROM `{BQ_AGGREG_RAW}`
        {filter_str}
        """
        if limit:
            _query += ' LIMIT @limit_val'
            query_parameters.append(
                bigquery.ScalarQueryParameter('limit_val', 'INT64', limit)
            )

        job_config = bigquery.QueryJobConfig(
            query_parameters=query_parameters, labels=BQ_LABELS
        )
        query_job_result = list(
            self._connection.connection.query(_query, job_config=job_config).result()
        )

        if query_job_result:
            return [BillingRowRecord.from_json(dict(row)) for row in query_job_result]

        raise ValueError('No record found')

    async def get_total_cost(
        self,
        query: BillingTotalCostQueryModel,
    ) -> list[BillingTotalCostRecord] | None:
        """
        Get Total cost of selected fields for requested time interval from BQ view
        """
        if not query.start_date or not query.end_date or not query.fields:
            raise ValueError('Date and Fields are required')

        extended_cols = BillingColumn.extended_cols()

        # by default look at the normal view
        if query.source == 'gcp_billing':
            view_to_use = BQ_GCP_BILLING_VIEW
        else:
            view_to_use = BQ_AGGREG_VIEW

        columns = []
        for field in query.fields:
            col_name = str(field.value)
            if col_name == 'cost':
                # skip the cost field as it will be always present
                continue

            if col_name in extended_cols:
                # if one of the extended columns is needed, the view has to be extended
                view_to_use = BQ_AGGREG_EXT_VIEW

            columns.append(col_name)

        fields_selected = ','.join(columns)

        # construct filters
        and_filters = []
        query_parameters = []

        and_filters.append('day >= TIMESTAMP(@start_date)')
        query_parameters.append(
            bigquery.ScalarQueryParameter('start_date', 'STRING', query.start_date)
        )

        and_filters.append('day <= TIMESTAMP(@end_date)')
        query_parameters.append(
            bigquery.ScalarQueryParameter('end_date', 'STRING', query.end_date)
        )

        if query.source == 'gcp_billing':
            # BQ_GCP_BILLING_VIEW view is partitioned by different field
            # BQ has limitation, materialized view can only by partition by base table
            # partition or its subset, in our case _PARTITIONTIME
            # (part_time field in the view)
            # We are querying by day,
            # which can be up to a week behind regarding _PARTITIONTIME
            and_filters.append('part_time >= TIMESTAMP(@start_date)')
            and_filters.append(
                'part_time <= TIMESTAMP_ADD(TIMESTAMP(@end_date), INTERVAL 7 DAY)'
            )

        filters = []
        if query.filters:
            for filter_key, filter_value in query.filters.items():
                col_name = str(filter_key.value)
                filters.append(f'{col_name} = @{col_name}')
                query_parameters.append(
                    bigquery.ScalarQueryParameter(col_name, 'STRING', filter_value)
                )
                if col_name in extended_cols:
                    # if one of the extended columns is needed,
                    # the view has to be extended
                    view_to_use = BQ_AGGREG_EXT_VIEW

        if query.filters_op == 'OR':
            if filters:
                and_filters.append('(' + ' OR '.join(filters) + ')')
        else:
            # if not specified, default to AND
            and_filters.extend(filters)

        filter_str = 'WHERE ' + ' AND '.join(and_filters) if and_filters else ''

        # construct order by
        order_by_cols = []
        if query.order_by:
            for order_field, reverse in query.order_by.items():
                col_name = str(order_field.value)
                col_order = 'DESC' if reverse else 'ASC'
                order_by_cols.append(f'{col_name} {col_order}')

        order_by_str = f'ORDER BY {",".join(order_by_cols)}' if order_by_cols else ''

        _query = f"""
        SELECT {fields_selected}, SUM(cost) as cost
        FROM `{view_to_use}`
        {filter_str}
        GROUP BY {fields_selected}
        {order_by_str}
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

        job_config = bigquery.QueryJobConfig(
            query_parameters=query_parameters, labels=BQ_LABELS
        )
        print(_query)
        print(query_parameters)
        query_job_result = list(
            self._connection.connection.query(_query, job_config=job_config).result()
        )

        if query_job_result:
            return [
                BillingTotalCostRecord.from_json(dict(row)) for row in query_job_result
            ]

        # return empty list if no record found
        return []

    async def get_budgets_by_gcp_project(
        self, field: BillingColumn, is_current_month: bool
    ) -> dict[str, float]:
        """
        Get budget for gcp-projects
        """
        if field != BillingColumn.PROJECT or not is_current_month:
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

        job_config = bigquery.QueryJobConfig(labels=BQ_LABELS)
        query_job_result = list(
            self._connection.connection.query(_query, job_config=job_config).result()
        )

        if query_job_result:
            return {row.gcp_project: row.budget for row in query_job_result}

        return {}

    async def get_last_loaded_day(self):
        """Get the most recent fully loaded day in db
        Go 2 days back as the data is not always available for the current day
        1 day back is not enough
        """

        _query = f"""
        SELECT TIMESTAMP_ADD(MAX(day), INTERVAL -2 DAY) as last_loaded_day
        FROM `{BQ_AGGREG_VIEW}`
        WHERE day > TIMESTAMP_ADD(
            CURRENT_TIMESTAMP(), INTERVAL @days DAY
        )
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    'days', 'INT64', -int(BQ_DAYS_BACK_OPTIMAL)
                ),
            ],
            labels=BQ_LABELS,
        )

        query_job_result = list(
            self._connection.connection.query(_query, job_config=job_config).result()
        )
        if query_job_result:
            return str(query_job_result[0].last_loaded_day)

        return None

    async def prepare_daily_cost_subquery(
        self, field, view_to_use, source, query_params
    ):
        """prepare daily cost subquery"""

        if source == 'gcp_billing':
            # add extra filter to limit materialized view partition
            # Raw BQ billing table is partitioned by part_time (when data are loaded)
            # and not by end of usage time (day)
            # There is a delay up to 4-5 days between part_time and day
            # 7 days is added to be sure to get all data
            gcp_billing_optimise_filter = """
            AND part_time >= TIMESTAMP(@last_loaded_day)
            AND part_time <= TIMESTAMP_ADD(
                TIMESTAMP(@last_loaded_day), INTERVAL 7 DAY
            )
            """
        else:
            gcp_billing_optimise_filter = ''

        # Find the last fully loaded day in the view
        last_loaded_day = await self.get_last_loaded_day()

        daily_cost_field = ', day.cost as daily_cost'
        daily_cost_join = f"""LEFT JOIN (
            SELECT
                {field.value} as field,
                cost_category,
                SUM(cost) as cost
            FROM
            `{view_to_use}`
            WHERE day = TIMESTAMP(@last_loaded_day)
            {gcp_billing_optimise_filter}
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
        return (last_loaded_day, query_params, daily_cost_field, daily_cost_join)

    async def execute_running_cost_query(
        self,
        field: BillingColumn,
        invoice_month: str | None = None,
        source: str | None = None,
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

        # by default look at the normal view
        if field in BillingColumn.extended_cols():
            # if any of the extendeid fields are needed use the extended view
            view_to_use = BQ_AGGREG_EXT_VIEW
        elif source == 'gcp_billing':
            # if source is gcp_billing,
            # use the view on top of the raw billing table
            view_to_use = BQ_GCP_BILLING_VIEW
        else:
            # otherwise use the normal view
            view_to_use = BQ_AGGREG_VIEW

        if source == 'gcp_billing':
            # add extra filter to limit materialized view partition
            # Raw BQ billing table is partitioned by part_time (when data are loaded)
            # and not by end of usage time (day)
            # There is a delay up to 4-5 days between part_time and day
            # 7 days is added to be sure to get all data
            filter_to_optimise_query = """
            part_time >= TIMESTAMP(@start_day)
            AND part_time <= TIMESTAMP_ADD(
                TIMESTAMP(@last_day), INTERVAL 7 DAY
            )
            """
        else:
            # add extra filter to limit materialized view partition
            filter_to_optimise_query = """
            day >= TIMESTAMP(@start_day)
            AND day <= TIMESTAMP(@last_day)
            """

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
            (
                last_loaded_day,
                query_params,
                daily_cost_field,
                daily_cost_join,
            ) = await self.prepare_daily_cost_subquery(
                field, view_to_use, source, query_params
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
            `{view_to_use}`
            WHERE {filter_to_optimise_query}
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
            list(
                self._connection.connection.query(
                    _query,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=query_params, labels=BQ_LABELS
                    ),
                ).result()
            ),
        )

    async def append_total_running_cost(
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

    async def append_running_cost_records(
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
        budgets_per_gcp_project = await self.get_budgets_by_gcp_project(
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
                        'last_loaded_day': last_loaded_day,
                    }
                )
            )

        return results

    async def get_running_cost(
        self,
        field: BillingColumn,
        invoice_month: str | None = None,
        source: str | None = None,
    ) -> list[BillingCostBudgetRecord]:
        """
        Get currently running cost of selected field
        """

        # accept only Topic, Dataset or Project at this stage
        if field not in (
            BillingColumn.TOPIC,
            BillingColumn.PROJECT,
            BillingColumn.DATASET,
            BillingColumn.STAGE,
            BillingColumn.COMPUTE_CATEGORY,
            BillingColumn.WDL_TASK_NAME,
            BillingColumn.CROMWELL_SUB_WORKFLOW_NAME,
        ):
            raise ValueError(
                'Invalid field only topic, dataset, gcp-project, compute_category, '
                'wdl_task_name & cromwell_sub_workflow_name are allowed'
            )

        (
            is_current_month,
            last_loaded_day,
            query_job_result,
        ) = await self.execute_running_cost_query(field, invoice_month, source)
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
        results = await self.append_total_running_cost(
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
        results = await self.append_running_cost_records(
            field,
            is_current_month,
            last_loaded_day,
            total_monthly,
            total_daily,
            field_details,
            results,
        )

        return results
