from google.cloud import bigquery

from models.models import (
    BillingRowRecord,
    BillingTotalCostRecord,
    BillingTotalCostQueryModel,
    BillingColumn,
)

from db.python.gcp_connect import BqDbBase
from db.python.layers.bq_base import BqBaseLayer
from db.python.tables.billing import BillingFilter

from api.settings import BQ_GCP_BILLING_PROJECT, BQ_DAYS_BACK_OPTIMAL

# TODO update beore merging into DEV
BQ_AGGREG_VIEW = f'{BQ_GCP_BILLING_PROJECT}.billing_aggregate.aggregate_daily_cost-dev'
BQ_AGGREG_RAW = f'{BQ_GCP_BILLING_PROJECT}.billing_aggregate.aggregate-dev'
BQ_AGGREG_EXT_VIEW = (
    f'{BQ_GCP_BILLING_PROJECT}.billing_aggregate.aggregate_daily_cost_extended-dev'
)


class BillingLayer(BqBaseLayer):
    """Billing layer"""

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


class BillingDb(BqDbBase):
    """Db layer for billing related routes"""

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
