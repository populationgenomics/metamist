# pylint: disable=ungrouped-imports,too-many-locals
import re

from typing import Any
from datetime import datetime
from collections import Counter, defaultdict
from google.cloud import bigquery
from api.utils.dates import get_invoice_month_range

from models.models import (
    BillingRowRecord,
    BillingTotalCostRecord,
    BillingTotalCostQueryModel,
    BillingColumn,
    BillingCostBudgetRecord,
)

from db.python.gcp_connect import BqDbBase
from db.python.layers.bq_base import BqBaseLayer
from db.python.tables.billing import BillingFilter

from api.settings import (
    BQ_DAYS_BACK_OPTIMAL,
    BQ_AGGREG_VIEW,
    BQ_AGGREG_RAW,
    BQ_AGGREG_EXT_VIEW,
    BQ_BUDGET_VIEW,
    BQ_GCP_BILLING_VIEW,
)


class BillingLayer(BqBaseLayer):
    """Billing layer"""

    async def get_gcp_projects(
        self,
    ) -> list[str] | None:
        """
        Get All GCP projects in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_gcp_projects()

    async def get_topics(
        self,
    ) -> list[str] | None:
        """
        Get All topics in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_topics()

    async def get_cost_categories(
        self,
    ) -> list[str] | None:
        """
        Get All service description / cost categories in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_cost_categories()

    async def get_skus(
        self,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[str] | None:
        """
        Get All SKUs in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_skus(limit, offset)

    async def get_datasets(
        self,
    ) -> list[str] | None:
        """
        Get All datasets in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_extended_values('dataset')

    async def get_stages(
        self,
    ) -> list[str] | None:
        """
        Get All stages in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_extended_values('stage')

    async def get_sequencing_types(
        self,
    ) -> list[str] | None:
        """
        Get All sequencing_types in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_extended_values('sequencing_type')

    async def get_sequencing_groups(
        self,
    ) -> list[str] | None:
        """
        Get All sequencing_groups in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_extended_values('sequencing_group')

    async def get_invoice_months(
        self,
    ) -> list[str] | None:
        """
        Get All invoice months in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_invoice_months()

    async def query(
        self,
        _filter: BillingFilter,
        limit: int = 10,
    ) -> list[BillingRowRecord] | None:
        """
        Get Billing record for the given gilter
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.query(_filter, limit)

    async def get_total_cost(
        self,
        query: BillingTotalCostQueryModel,
    ) -> list[BillingTotalCostRecord] | None:
        """
        Get Total cost of selected fields for requested time interval
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_total_cost(query)

    async def get_running_cost(
        self,
        field: BillingColumn,
        invoice_month: str | None = None,
        source: str | None = None,
    ) -> list[BillingCostBudgetRecord]:
        """
        Get Running costs including monthly budget
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_running_cost(field, invoice_month, source)


class BillingDb(BqDbBase):
    """Db layer for billing related routes"""

    async def get_gcp_projects(self):
        """Get all GCP projects in database"""

        # cost of this BQ is 10MB on DEV is minimal, AU$ 0.000008 per query
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
            ]
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
        _query = f"""
        SELECT DISTINCT topic
        FROM `{BQ_AGGREG_VIEW}`
        WHERE day > TIMESTAMP_ADD(
            CURRENT_TIMESTAMP(), INTERVAL @days DAY
        )
        ORDER BY topic ASC;
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    'days', 'INT64', -int(BQ_DAYS_BACK_OPTIMAL)
                ),
            ]
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

        # cost of this BQ is 10MB on DEV is minimal, AU$ 0.000008 per query
        _query = f"""
        SELECT DISTINCT FORMAT_DATE("%Y%m", day) as invoice_month
        FROM `{BQ_AGGREG_VIEW}`
        WHERE EXTRACT(day from day) = 1
        ORDER BY invoice_month DESC;
        """

        query_job_result = list(self._connection.connection.query(_query).result())
        if query_job_result:
            return [str(dict(row)['invoice_month']) for row in query_job_result]

        # return empty list if no record found
        return []

    async def get_cost_categories(self):
        """Get all service description in database"""

        # cost of this BQ is 10MB on DEV is minimal, AU$ 0.000008 per query
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
            ]
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
            ]
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
        Get all extended values in database,
        e.g. dataset, stage, sequencing_type or sequencing_group
        """

        # cost of this BQ is 10MB on DEV is minimal, AU$ 0.000008 per query
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
            ]
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

        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
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
        filters = []
        query_parameters = []

        filters.append('day >= TIMESTAMP(@start_date)')
        query_parameters.append(
            bigquery.ScalarQueryParameter('start_date', 'STRING', query.start_date)
        )

        filters.append('day <= TIMESTAMP(@end_date)')
        query_parameters.append(
            bigquery.ScalarQueryParameter('end_date', 'STRING', query.end_date)
        )

        if query.source == 'gcp_billing':
            # BQ_GCP_BILLING_VIEW view is partitioned by different field
            # BG has limitation, materialized view can only by partition by base table
            # partition or its subset, in our case _PARTITIONTIME
            # (part_time field in the view)
            # We are querying by day,
            # which can be up to a week behind regarding _PARTITIONTIME
            filters.append('part_time >= TIMESTAMP(@start_date)')
            filters.append(
                'part_time <= TIMESTAMP_ADD(TIMESTAMP(@end_date), INTERVAL 7 DAY)'
            )

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

        filter_str = 'WHERE ' + ' AND '.join(filters) if filters else ''

        # construct order by
        order_by_cols = []
        if query.order_by:
            for order_field, reverse in query.order_by.items():
                col_name = str(order_field.value)
                col_order = 'DESC' if reverse else 'ASC'
                order_by_cols.append(f'{col_name} {col_order}')

        order_by_str = f'ORDER BY {",".join(order_by_cols)}' if order_by_cols else ''

        _query = f"""
        SELECT * FROM
        (
            SELECT {fields_selected}, SUM(cost) as cost
            FROM `{view_to_use}`
            {filter_str}
            GROUP BY {fields_selected}

        )
        WHERE cost > 0.01
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

        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        query_job_result = list(
            self._connection.connection.query(_query, job_config=job_config).result()
        )

        if query_job_result:
            return [
                BillingTotalCostRecord.from_json(dict(row)) for row in query_job_result
            ]

        # return empty list if no record found
        return []

    async def get_budget(self) -> dict[str, float] | None:
        """
        Get budget for projects
        """
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

        query_job_result = list(self._connection.connection.query(_query).result())

        if query_job_result:
            return {row.gcp_project: row.budget for row in query_job_result}

        return None

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
            ]
        )

        query_job_result = list(
            self._connection.connection.query(_query, job_config=job_config).result()
        )
        if query_job_result:
            return str(query_job_result[0].last_loaded_day)

        return None

    async def prepare_daily_cost_query(self, field, view_to_use, source, query_params):
        """prepare_daily_cost_query"""

        if source == 'gcp_billing':
            # add extra filter to limit materialized view partition
            daily_cost_gcp_billing_filter = """
            AND part_time >= TIMESTAMP(@last_loaded_day)
            AND part_time <= TIMESTAMP_ADD(
                TIMESTAMP(@last_loaded_day), INTERVAL 7 DAY
            )
            """
        else:
            daily_cost_gcp_billing_filter = ''

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
            {daily_cost_gcp_billing_filter}
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

    async def execute_running_cost_query(
        self,
        field: BillingColumn,
        invoice_month: str | None = None,
        source: str | None = None,
    ):
        """
        Run query to get running cost of selected field
        """
        # by default look at the normal view
        if field in BillingColumn.extended_cols():
            view_to_use = BQ_AGGREG_EXT_VIEW
        elif source == 'gcp_billing':
            view_to_use = BQ_GCP_BILLING_VIEW
        else:
            view_to_use = BQ_AGGREG_VIEW

        is_current_month = True
        invoice_month_filter = ''
        last_loaded_day = None
        query_params = []

        # Get start day and current day for given invoice month
        # This is for partitioning efficiency. The query is effectively just a filter
        # by invoice_month. The partitioning is by day, so we need to find the
        # start and end day of the invoice month to filter on the partition
        invoice_month_date = datetime.strptime(invoice_month, '%Y%m')
        start_day_date, last_day_date = get_invoice_month_range(invoice_month_date)
        start_day = start_day_date.strftime('%Y-%m-%d')
        last_day = last_day_date.strftime('%Y-%m-%d')
        current_day = datetime.now().strftime('%Y-%m-%d')

        invoice_month_filter = ' AND invoice_month = @invoice_month'
        query_params.append(
            bigquery.ScalarQueryParameter('invoice_month', 'STRING', invoice_month)
        )

        if last_day < current_day:
            # not current invoice month, do not show daily cost & budget
            is_current_month = False

        query_params.append(
            bigquery.ScalarQueryParameter('start_day', 'STRING', start_day)
        )
        query_params.append(
            bigquery.ScalarQueryParameter('last_day', 'STRING', last_day)
        )

        if is_current_month:
            # Only current month can have last 24 hours cost
            # Last 24H in UTC time
            (
                query_params,
                daily_cost_field,
                daily_cost_join,
            ) = await self.prepare_daily_cost_query(
                field, view_to_use, source, query_params
            )
        else:
            # Do not calculate last 24H cost
            daily_cost_field = ', NULL as daily_cost'
            daily_cost_join = ''

        if source == 'gcp_billing':
            # add extra filter to limit materialized view partition
            monthly_cost_gcp_billing_filter = """
            AND part_time >= TIMESTAMP(@start_day)
            AND part_time <= TIMESTAMP_ADD(
                TIMESTAMP(@last_day), INTERVAL 7 DAY
            )
            """
        else:
            monthly_cost_gcp_billing_filter = ''

        _query = f"""
        SELECT
            CASE WHEN month.field IS NULL THEN 'N/A' ELSE month.field END as field,
            month.cost_category,
            month.cost as monthly_cost
            {daily_cost_field}
        FROM
        (
            SELECT
            *
            FROM
            (
                SELECT
                {field.value} as field,
                cost_category,
                SUM(cost) as cost
                FROM
                `{view_to_use}`
                WHERE day >= TIMESTAMP(@start_day) AND
                day <= TIMESTAMP(@last_day)
                {invoice_month_filter}
                {monthly_cost_gcp_billing_filter}
                GROUP BY
                field,
                cost_category
            )
            WHERE
            cost > 0.1
        ) month
        {daily_cost_join}
        ORDER BY field ASC, daily_cost DESC, monthly_cost DESC;
        """

        return (
            is_current_month,
            last_loaded_day,
            list(
                self._connection.connection.query(
                    _query,
                    job_config=bigquery.QueryJobConfig(query_parameters=query_params),
                ).result()
            ),
        )

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
        ):
            raise ValueError(
                (
                    'Invalid field. Select on of '
                    f'({BillingColumn.TOPIC}, {BillingColumn.PROJECT})'
                )
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
        results = []

        # reformat last_loaded_day if present
        if last_loaded_day:
            last_loaded_day = datetime.strptime(
                last_loaded_day, '%Y-%m-%d %H:%M:%S+00:00'
            )
            last_loaded_day = last_loaded_day.strftime('%b %d')

        total_monthly: dict[str, Counter[str]] = {
            'C': Counter(),
            'S': Counter(),
        }
        total_daily: dict[str, Counter[str]] = {
            'C': Counter(),
            'S': Counter(),
        }
        # detail category cost for each field
        field_details: dict[str, list[Any]] = defaultdict(list)
        total_monthly_category: Counter[str] = Counter()
        total_daily_category: Counter[str] = Counter()
        for row in query_job_result:
            field_details[row.field].append(
                {
                    'cost_group': 'S' if row.cost_category == 'Cloud Storage' else 'C',
                    'cost_category': row.cost_category,
                    'daily_cost': row.daily_cost if is_current_month else None,
                    'monthly_cost': row.monthly_cost,
                }
            )

            total_monthly_category[row.cost_category] += row.monthly_cost
            if row.daily_cost:
                total_daily_category[row.cost_category] += row.daily_cost

            # cost groups S/C
            if row.cost_category == 'Cloud Storage':
                total_monthly['S']['ALL'] += row.monthly_cost
                total_monthly['S'][row.field] += row.monthly_cost
                if row.daily_cost and is_current_month:
                    total_daily['S']['ALL'] += row.daily_cost
                    total_daily['S'][row.field] += row.daily_cost
            else:
                total_monthly['C']['ALL'] += row.monthly_cost
                total_monthly['C'][row.field] += row.monthly_cost
                if row.daily_cost and is_current_month:
                    total_daily['C']['ALL'] += row.daily_cost
                    total_daily['C'][row.field] += row.daily_cost

        # add total
        # construct ALL fields details
        all_details = []
        for cat, mth_cost in total_monthly_category.items():
            all_details.append(
                {
                    'cost_group': 'S' if cat == 'Cloud Storage' else 'C',
                    'cost_category': cat,
                    'daily_cost': total_daily_category[cat]
                    if is_current_month
                    else None,
                    'monthly_cost': mth_cost,
                }
            )

        # add total row first
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

        # get budget map per gcp project
        if field == BillingColumn.PROJECT and is_current_month:
            budget = await self.get_budget()
        else:
            # only projects have budget and only for current month
            budget = None

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
            budget_monthly = budget.get(key, 0) if budget else 0
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
