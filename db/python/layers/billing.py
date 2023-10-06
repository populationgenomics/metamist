from models.models import (
    BillingTopicCostCategoryRecord,
    BillingRowRecord,
    BillingTotalCostRecord,
)

from db.python.gcp_connect import BqDbBase
from db.python.layers.bq_base import BqBaseLayer
from db.python.tables.billing import BillingFilter

# TODO setup as ENV or config
BQ_AGGREG_VIEW = 'billing-admin-290403.billing_aggregate.aggregate_daily_cost-dev'
BQ_AGGREG_RAW = 'billing-admin-290403.billing_aggregate.aggregate-dev'


class BillingLayer(BqBaseLayer):
    """Billing layer"""

    async def get_topic(
        self,
    ) -> list[str] | None:
        """
        Get All topics in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_topic()

    async def get_cost_category(
        self,
    ) -> list[str] | None:
        """
        Get All service description / cost categories in database
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_cost_category()

    async def get_cost_by_topic(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        topic: str | None = None,
        cost_category: str | None = None,
        order_by: str | None = None,
        limit: int = 10,
    ) -> list[BillingTopicCostCategoryRecord] | None:
        """
        Get Billing record for the given time interval, topic and cost category
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_cost_by_topic(
            start_date, end_date, topic, cost_category, order_by, limit
        )

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
        fields: str,
        start_date: str,
        end_date: str,
        topic: str | None = None,
        cost_category: str | None = None,
        sku: str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[BillingTotalCostRecord] | None:
        """
        Get Total cost of selected fields for requested time interval
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.get_total_cost(
            fields,
            start_date,
            end_date,
            topic,
            cost_category,
            sku,
            order_by,
            limit,
            offset,
        )


class BillingDb(BqDbBase):
    """Db layer for billing related routes"""

    async def get_topic(self):
        """Get all topics in database"""

        # cost of this BQ is 10MB on DEV is minimal, AU$ 0.000008 per query
        _query = f"""
        SELECT DISTINCT topic
        FROM `{BQ_AGGREG_VIEW}`
        ORDER BY topic ASC;
        """

        query_job_result = list(self._connection.connection.query(_query).result())
        if query_job_result:
            return [str(dict(row)['topic']) for row in query_job_result]

        raise ValueError('No record found')

    async def get_cost_category(self):
        """Get all service description in database"""

        # cost of this BQ is 10MB on DEV is minimal, AU$ 0.000008 per query
        _query = f"""
        SELECT DISTINCT cost_category
        FROM `{BQ_AGGREG_VIEW}`
        ORDER BY cost_category ASC;
        """

        query_job_result = list(self._connection.connection.query(_query).result())
        if query_job_result:
            return [str(dict(row)['cost_category']) for row in query_job_result]

        raise ValueError('No record found')

    async def get_cost_by_topic(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        topic: str | None = None,
        cost_category: str | None = None,
        order_by: str | None = None,
        limit: int = 10,
    ) -> list[BillingTopicCostCategoryRecord] | None:
        """Get Billing record from BQ"""
        # ORDER BY day ASC, topic ASC, cost_category ASC;
        filters = []

        if start_date:
            filters.append(f'day >= TIMESTAMP("{start_date}")')
        if end_date:
            filters.append(f'day <= TIMESTAMP("{end_date}")')
        if topic:
            filters.append(f'topic = "{topic}"')
        if cost_category:
            filters.append(f'cost_category = "{cost_category}"')

        filter_str = 'WHERE ' + ' AND '.join(filters) if filters else ''
        order_by_str = f'ORDER BY {order_by}' if order_by else ''

        _query = f"""
        SELECT * FROM
        (
            SELECT day, topic, cost_category, currency, cost
            FROM `{BQ_AGGREG_VIEW}`
            {filter_str}
        )
        WHERE cost > 0.01
        {order_by_str}
        LIMIT {limit}
        """
        query_job_result = list(self._connection.connection.query(_query).result())

        if query_job_result:
            return [
                BillingTopicCostCategoryRecord.from_json(dict(row))
                for row in query_job_result
            ]

        raise ValueError('No record found')

    async def query(
        self,
        filter_: BillingFilter,
        limit: int = 10,
    ) -> list[BillingRowRecord] | None:
        """Get Billing record from BQ"""

        # cost of this BQ is 30MB on DEV,
        # DEV is partition by day and date is required filter params,
        # cost is aprox per query: AU$ 0.000023 per query

        required_fields = [
            filter_.date,
        ]

        if not any(required_fields):
            raise ValueError('Must provide date to filter on')

        filters = []
        if filter_.topic:
            filters.append(f'topic IN ("{filter_.topic.eq}")')

        if filter_.date:
            filters.append(
                f'DATE_TRUNC(usage_end_time, DAY) = TIMESTAMP("{filter_.date.eq}")'
            )

        if filter_.cost_category:
            filters.append(f'service.description IN ("{filter_.cost_category.eq}")')

        filter_str = 'WHERE ' + ' AND '.join(filters) if filters else ''

        _query = f"""
        SELECT id, topic, service, sku, usage_start_time, usage_end_time, project,
        labels, export_time, cost, currency, currency_conversion_rate, invoice, cost_type
        FROM `{BQ_AGGREG_RAW}`
        {filter_str}
        LIMIT {limit}
        """

        query_job_result = list(self._connection.connection.query(_query).result())

        if query_job_result:
            return [BillingRowRecord.from_json(dict(row)) for row in query_job_result]

        raise ValueError('No record found')

    async def get_total_cost(
        self,
        fields: str,
        start_date: str,
        end_date: str,
        topic: str | None = None,
        cost_category: str | None = None,
        sku: str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[BillingTotalCostRecord] | None:
        """
        Get Total cost of selected fields for requested time interval from BQ view
        """
        if not start_date or not end_date or not fields:
            raise ValueError('Date and Fields are required')

        allow_fields = ['day', 'topic', 'cost_category', 'currency', 'cost', 'sku']

        columns = []
        for field in fields.split(','):
            col_name = field.strip()
            if col_name not in allow_fields:
                raise ValueError(f'Invalid field: {field}')

            if col_name == 'cost':
                # skip the cost field as it will be always present
                continue

            columns.append(col_name)

        fields_selected = ','.join(columns)

        filters = []
        filters.append(f'day >= TIMESTAMP("{start_date}")')
        filters.append(f'day <= TIMESTAMP("{end_date}")')
        if topic:
            filters.append(f'topic = "{topic}"')
        if cost_category:
            filters.append(f'cost_category = "{cost_category}"')
        if sku:
            filters.append(f'sku = "{sku}"')

        filter_str = 'WHERE ' + ' AND '.join(filters) if filters else ''

        # construct order by
        order_by_cols = []
        if order_by:
            for field in order_by.split(','):
                col_name = field.strip()
                col_sign = col_name[0]
                if col_sign not in ['+', '-']:
                    # default is ASC
                    col_order = 'ASC'
                else:
                    col_order = 'DESC' if col_sign == '-' else 'ASC'
                    col_name = col_name[1:]

                if col_name not in allow_fields:
                    raise ValueError(f'Invalid field: {field}')

                order_by_cols.append(f'{col_name} {col_order}')

        order_by_str = f'ORDER BY {",".join(order_by_cols)}' if order_by_cols else ''

        _query = f"""
        SELECT * FROM
        (
            SELECT {fields_selected}, SUM(cost) as cost
            FROM `{BQ_AGGREG_VIEW}`
            {filter_str}
            GROUP BY {fields_selected}

        )
        WHERE cost > 0.01
        {order_by_str}
        """

        # append LIMIT and OFFSET if present
        if limit:
            _query += f' LIMIT {limit}'
        if offset:
            _query += f' OFFSET {offset}'

        query_job_result = list(self._connection.connection.query(_query).result())

        if query_job_result:
            return [
                BillingTotalCostRecord.from_json(dict(row)) for row in query_job_result
            ]

        raise ValueError('No record found')
