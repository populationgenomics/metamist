from datetime import datetime
from typing import Any

import rapidjson
from google.cloud import bigquery

from api.settings import BQ_AGGREG_EXT_VIEW
from db.python.tables.bq.billing_base import (
    BillingBaseTable,
    time_optimisation_parameter,
)
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
            time_optimisation_parameter(),
        ]
        query_job_result = self._execute_query(_query, query_parameters)

        if query_job_result:
            return [str(dict(row)[field]) for row in query_job_result]

        # return empty list if no record found
        return []

    async def get_batch_cost_details(
        self,
        start_time: datetime,
        end_time: datetime,
        batch_ids: list[str] | None,
        ar_guid: str | None,
    ) -> list[Any]:
        """ """
        # get all batch details f
        query_parameters = [
            bigquery.ScalarQueryParameter('start_time', 'TIMESTAMP', start_time),
            bigquery.ScalarQueryParameter('end_time', 'TIMESTAMP', end_time),
        ]

        if batch_ids:
            condition = 'batch_id in (@batch_ids)'
            query_parameters.append(
                bigquery.ScalarQueryParameter(
                    'batch_ids', 'STRING', ','.join([f"'{b}'" for b in batch_ids])
                )
            )

        else:
            condition = 'ar_guid = @ar_guid'
            query_parameters.append(
                bigquery.ScalarQueryParameter('ar_guid', 'STRING', ar_guid)
            )

        condition = "batch_id IN ('432380', '432376')"

        _query = f"""
            with d as (
                SELECT topic, ar_guid, batch_id, CAST(job_id AS INT) job_id, sku, cost,
                usage_start_time, usage_end_time,
                namespace, JSON_VALUE(PARSE_JSON(labels), '$.batch_name') as batch_name
                --, labels
                FROM `billing-admin-290403.billing_aggregate.aggregate_daily_extended`
                        WHERE day >= @start_time
                        AND day <= @end_time
                        AND {condition}
            )
            , batch_jobs_cnt as (
                -- sum al jobs first
                SELECT  batch_id,
                COUNT(DISTINCT job_id) as jobs
                FROM d
                GROUP BY batch_id
            )
            , jtop as (
                -- sum al jobs first
                SELECT  d.batch_id, job_id,
                SUM(cost) as cost
                FROM d inner join batch_jobs_cnt ON batch_jobs_cnt.batch_id = d.batch_id
                WHERE batch_jobs_cnt.jobs > 10
                GROUP BY d.batch_id, job_id
                ORDER BY cost DESC
                LIMIT 20
            )
            , j as (
                -- show all jobs if under 10, otherwised only the most costly
                SELECT  topic, ar_guid, d.batch_id, namespace, job_id, sku,
                MIN(usage_start_time) as usage_start_time,
                MAX(usage_end_time) as usage_end_time, SUM(cost) as cost
                FROM d inner join batch_jobs_cnt ON batch_jobs_cnt.batch_id = d.batch_id
                WHERE batch_jobs_cnt.jobs <= 10
                GROUP BY topic, ar_guid, d.batch_id, namespace, job_id, sku
            )
            , t as (
            -- ar_guid
                SELECT  topic, ar_guid, NULL as batch_id, NULL as namespace, NULL as batch_name, NULL as job_id, sku,
                NULL as jobs,
                MIN(usage_start_time) as usage_start_time,
                MAX(usage_end_time) as usage_end_time, SUM(cost) as cost
                --, MAX(labels) as labels
                FROM d
                GROUP BY topic, ar_guid, sku
            UNION ALL
            -- batch_id
                SELECT  topic, ar_guid, d.batch_id, namespace, batch_name, NULL as job_id, sku,
                batch_jobs_cnt.jobs as jobs,
                MIN(usage_start_time) as usage_start_time,
                MAX(usage_end_time) as usage_end_time, SUM(cost) as cost
                --, MAX(labels) as labels
                FROM d INNER JOIN batch_jobs_cnt ON d.batch_id = batch_jobs_cnt.batch_id
                GROUP BY topic, ar_guid, batch_id, batch_name, namespace, sku, jobs
            UNION ALL
            -- job_id
              SELECT topic, ar_guid, batch_id, namespace, NULL AS batch_name, job_id, sku, NULL AS jobs, usage_start_time,usage_end_time,cost
              from j
            UNION ALL
            -- job_id onlly top 10
                SELECT  topic, ar_guid, d.batch_id, namespace, batch_name, d.job_id, sku,
                NULL as jobs,
                MIN(usage_start_time) as usage_start_time,
                MAX(usage_end_time) as usage_end_time, SUM(d.cost) as cost
                --, MAX(labels) as labels
                FROM d INNER JOIN jtop ON d.batch_id = jtop.batch_id AND d.job_id = jtop.job_id
                GROUP BY topic, ar_guid, batch_id, batch_name, namespace, d.job_id, sku
            )
            SELECT topic, ar_guid, batch_id, namespace, batch_name, job_id, jobs,
            MIN(usage_start_time) as start_time,
            MAX(usage_end_time) as end_time,
            SUM(cost) as cost,
            ARRAY_AGG(STRUCT(
                sku, cost
            )) as details
            --, MAX(labels) as labels
            FROM t
            GROUP BY topic, ar_guid, batch_id, namespace, batch_name, job_id, jobs
            ORDER BY topic, ar_guid, batch_id, job_id
        """
        query_job_result = self._execute_query(_query, query_parameters, False)

        if query_job_result:
            return [dict(row) for row in query_job_result]

        # return empty list if no record found
        return []
