from google.cloud import bigquery

from api.settings import BQ_DAYS_BACK_OPTIMAL, BQ_GCP_BILLING_VIEW
from db.python.tables.billing_base import BillingBaseTable
from models.models import BillingTotalCostQueryModel


class BillingGcpDailyTable(BillingBaseTable):
    """Billing GCP Daily Big Query table"""

    table_name = BQ_GCP_BILLING_VIEW

    def get_table_name(self):
        """Get table name"""
        return self.table_name

    def _filter_to_optimise_query(self) -> str:
        """Filter string to optimise BQ query
        override base class method as gcp table has different partition field
        """
        # add extra filter to limit materialized view partition
        # Raw BQ billing table is partitioned by part_time (when data are loaded)
        # and not by end of usage time (day)
        # There is a delay up to 4-5 days between part_time and day
        # 7 days is added to be sure to get all data
        return (
            'part_time >= TIMESTAMP(@start_day)'
            'AND part_time <= TIMESTAMP_ADD(TIMESTAMP(@last_day), INTERVAL 7 DAY)'
        )

    def _last_loaded_day_filter(self) -> str:
        """Filter string to optimise BQ query
        override base class method as gcp table has different partition field
        """
        # add extra filter to limit materialized view partition
        # Raw BQ billing table is partitioned by part_time (when data are loaded)
        # and not by end of usage time (day)
        # There is a delay up to 4-5 days between part_time and day
        # 7 days is added to be sure to get all data
        return (
            'day = TIMESTAMP(@last_loaded_day)'
            'AND part_time >= TIMESTAMP(@last_loaded_day)'
            'AND part_time <= TIMESTAMP_ADD(TIMESTAMP(@last_loaded_day),INTERVAL 7 DAY)'
        )

    def _prepare_time_filters(self, query: BillingTotalCostQueryModel):
        """Prepare time filters, append to time_filters list"""
        time_filters, query_parameters = super()._prepare_time_filters(query)

        # BQ_GCP_BILLING_VIEW view is partitioned by different field
        # BQ has limitation, materialized view can only by partition by base table
        # partition or its subset, in our case _PARTITIONTIME
        # (part_time field in the view)
        # We are querying by day,
        # which can be up to a week behind regarding _PARTITIONTIME
        time_filters.append('part_time >= TIMESTAMP(@start_date)')
        time_filters.append(
            'part_time <= TIMESTAMP_ADD(TIMESTAMP(@end_date), INTERVAL 7 DAY)'
        )
        return time_filters, query_parameters

    async def _last_loaded_day(self):
        """Get the most recent fully loaded day in db
        Go 2 days back as the data is not always available for the current day
        1 day back is not enough
        """

        _query = f"""
        SELECT TIMESTAMP_ADD(MAX(part_time), INTERVAL -2 DAY) as last_loaded_day
        FROM `{self.table_name}`
        WHERE part_time > TIMESTAMP_ADD(
            CURRENT_TIMESTAMP(), INTERVAL @days DAY
        )
        """

        query_parameters = [
            bigquery.ScalarQueryParameter('days', 'INT64', -int(BQ_DAYS_BACK_OPTIMAL)),
        ]
        query_job_result = self._execute_query(_query, query_parameters)

        if query_job_result:
            return str(query_job_result[0].last_loaded_day)

        return None

    def _prepare_daily_cost_subquery(self, field, query_params, last_loaded_day):
        """prepare daily cost subquery"""

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

        daily_cost_field = ', day.cost as daily_cost'
        daily_cost_join = f"""LEFT JOIN (
            SELECT
                {field.value} as field,
                cost_category,
                SUM(cost) as cost
            FROM
            `{self.get_table_name()}`
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
        return (query_params, daily_cost_field, daily_cost_join)

    async def get_gcp_projects(self):
        """Get all GCP projects in database"""

        # cost of this BQ is 10MB on DEV is minimal, AU$ 0.000008 per query
        # @days is defined by env variable BQ_DAYS_BACK_OPTIMAL
        # this part_time > filter is to limit the amount of data scanned,
        # saving cost for running BQ
        _query = f"""
        SELECT DISTINCT gcp_project
        FROM `{self.table_name}`
        WHERE part_time > TIMESTAMP_ADD(
            CURRENT_TIMESTAMP(), INTERVAL @days DAY
        )
        AND gcp_project IS NOT NULL
        ORDER BY gcp_project ASC;
        """

        query_parameters = [
            bigquery.ScalarQueryParameter('days', 'INT64', -int(BQ_DAYS_BACK_OPTIMAL)),
        ]
        query_job_result = self._execute_query(_query, query_parameters)

        if query_job_result:
            return [str(dict(row)['gcp_project']) for row in query_job_result]

        # return empty list if no record found
        return []
