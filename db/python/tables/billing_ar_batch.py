from datetime import datetime, timedelta

from google.cloud import bigquery

from api.settings import BQ_BATCHES_VIEW
from db.python.tables.billing_base import BillingBaseTable


class BillingArBatchTable(BillingBaseTable):
    """Billing AR - BatchID lookup Big Query table"""

    table_name = BQ_BATCHES_VIEW

    def get_table_name(self):
        """Get table name"""
        return self.table_name

    async def get_batches_by_ar_guid(
        self, ar_guid: str
    ) -> tuple[datetime, datetime, list[str]]:
        """
        Get batches for given ar_guid
        """
        _query = f"""
        SELECT
            batch_id,
            MIN(min_day) as start_day,
            MAX(max_day) as end_day
        FROM `{self.table_name}`
        WHERE ar_guid = @ar_guid
        AND batch_id IS NOT NULL
        GROUP BY batch_id
        ORDER BY 1;
        """

        query_parameters = [
            bigquery.ScalarQueryParameter('ar_guid', 'STRING', ar_guid),
        ]
        query_job_result = self._execute_query(_query, query_parameters)

        if query_job_result:
            start_day = min((row.start_day for row in query_job_result))
            end_day = max((row.end_day for row in query_job_result)) + timedelta(days=1)
            return start_day, end_day, [row.batch_id for row in query_job_result]

        # return empty list if no record found
        return None, None, []

    async def get_ar_guid_by_batch_id(self, batch_id: str) -> str:
        """
        Get ar_guid for given batch_id
        """
        _query = f"""
        SELECT ar_guid
        FROM `{self.table_name}`
        WHERE batch_id = @batch_id
        AND ar_guid IS NOT NULL
        LIMIT 1;
        """

        query_parameters = [
            bigquery.ScalarQueryParameter('batch_id', 'STRING', batch_id),
        ]
        query_job_result = self._execute_query(_query, query_parameters)
        if query_job_result:
            return query_job_result[0]['ar_guid']

        # return None if no ar_guid found
        return None
