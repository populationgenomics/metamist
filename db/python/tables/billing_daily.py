from google.cloud import bigquery

from api.settings import BQ_AGGREG_VIEW, BQ_DAYS_BACK_OPTIMAL
from db.python.tables.billing_base import BillingBaseTable


class BillingDailyTable(BillingBaseTable):
    """Billing Aggregated Daily Biq Query table"""

    table_name = BQ_AGGREG_VIEW

    def get_table_name(self):
        """Get table name"""
        return self.table_name

    async def get_topics(self):
        """Get all topics in database"""

        # cost of this BQ is 10MB on DEV is minimal, AU$ 0.000008 per query
        # @days is defined by env variable BQ_DAYS_BACK_OPTIMAL
        # this day > filter is to limit the amount of data scanned,
        # saving cost for running BQ
        # aggregated views are partitioned by day
        _query = f"""
        SELECT DISTINCT topic
        FROM `{self.table_name}`
        WHERE day > TIMESTAMP_ADD(
            CURRENT_TIMESTAMP(), INTERVAL @days DAY
        )
        ORDER BY topic ASC;
        """

        query_parameters = [
            bigquery.ScalarQueryParameter('days', 'INT64', -int(BQ_DAYS_BACK_OPTIMAL)),
        ]
        query_job_result = self._execute_query(_query, query_parameters)

        if query_job_result:
            return [str(dict(row)['topic']) for row in query_job_result]

        # return empty list if no record found
        return []

    async def get_invoice_months(self):
        """Get all invoice months in database"""

        _query = f"""
        SELECT DISTINCT FORMAT_DATE("%Y%m", day) as invoice_month
        FROM `{self.table_name}`
        WHERE EXTRACT(day from day) = 1
        ORDER BY invoice_month DESC;
        """

        query_job_result = self._execute_query(_query)
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

        query_parameters = [
            bigquery.ScalarQueryParameter('days', 'INT64', -int(BQ_DAYS_BACK_OPTIMAL)),
        ]
        query_job_result = self._execute_query(_query, query_parameters)

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
        FROM `{self.table_name}`
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

        query_parameters = [
            bigquery.ScalarQueryParameter('days', 'INT64', -int(BQ_DAYS_BACK_OPTIMAL)),
            bigquery.ScalarQueryParameter('limit_val', 'INT64', limit),
            bigquery.ScalarQueryParameter('offset_val', 'INT64', offset),
        ]
        query_job_result = self._execute_query(_query, query_parameters)

        if query_job_result:
            return [str(dict(row)['sku']) for row in query_job_result]

        # return empty list if no record found
        return []
