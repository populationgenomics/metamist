from google.cloud import bigquery

from api.settings import BQ_AGGREG_EXT_VIEW, BQ_DAYS_BACK_OPTIMAL
from db.python.tables.billing_base import BillingBaseTable
from models.models import BillingColumn


class BillingDailyExtendedTable(BillingBaseTable):
    """Billing Aggregated Daily Extended Biq Query table"""

    table_name = BQ_AGGREG_EXT_VIEW

    def get_table_name(self):
        """Get table name"""
        return self.table_name

    async def get_extended_values(self, field: str):
        """
        Get all extended values in database, for specified field.
        Field is one of extended columns.
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
        FROM `{self.table_name}`
        WHERE {field} IS NOT NULL
        AND day > TIMESTAMP_ADD(
            CURRENT_TIMESTAMP(), INTERVAL @days DAY
        )
        ORDER BY 1 ASC;
        """

        query_parameters = [
            bigquery.ScalarQueryParameter('days', 'INT64', -int(BQ_DAYS_BACK_OPTIMAL)),
        ]
        query_job_result = self._execute_query(_query, query_parameters)

        if query_job_result:
            return [str(dict(row)[field]) for row in query_job_result]

        # return empty list if no record found
        return []
