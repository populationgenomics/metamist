from datetime import datetime

from google.cloud import bigquery

from api.settings import BQ_AGGREG_EXT_VIEW
from db.python.tables.bq.billing_base import (
    BillingBaseTable,
    time_optimisation_parameter,
)
from models.models import BillingBatchCostRecord, BillingColumn


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
    ) -> list[BillingBatchCostRecord]:
        """
        Get all batch details for given time range and batch_ids or ar_guid
        """
        query_parameters = [
            bigquery.ScalarQueryParameter('start_time', 'TIMESTAMP', start_time),
            bigquery.ScalarQueryParameter('end_time', 'TIMESTAMP', end_time),
        ]

        if ar_guid:
            condition = 'ar_guid = @ar_guid'
            query_parameters.append(
                bigquery.ScalarQueryParameter('ar_guid', 'STRING', ar_guid)
            )
        elif batch_ids:
            condition = 'batch_id in UNNEST(@batch_ids)'
            query_parameters.append(
                bigquery.ArrayQueryParameter('batch_ids', 'STRING', batch_ids)
            )
        else:
            raise ValueError('Either ar_guid or batch_ids must be provided')

        _query = f"""
            with d as (
                SELECT topic, ar_guid, namespace,
                compute_category, batch_id, CAST(job_id AS INT) job_id,
                cromwell_workflow_id, cromwell_sub_workflow_name, wdl_task_name,
                sku, cost,
                usage_start_time, usage_end_time,
                labels
                FROM `{self.table_name}`
                        WHERE day >= @start_time
                        AND day <= @end_time
                        AND {condition}
            )
            , batch_jobs_cnt as (
                -- count all jobs first
                SELECT  batch_id,
                COUNT(DISTINCT job_id) as jobs
                FROM d
                WHERE batch_id IS NOT NULL
                GROUP BY batch_id
            )
            , t as (
            -- ar_guid
                SELECT  topic, ar_guid, NULL as namespace, compute_category,
                NULL as batch_id, NULL as job_id,
                NULL as cromwell_workflow_id, NULL as cromwell_sub_workflow_name, NULL as wdl_task_name,
                sku,
                NULL as jobs,
                MIN(usage_start_time) as usage_start_time,
                MAX(usage_end_time) as usage_end_time, SUM(cost) as cost
                , MAX(labels) as labels
                FROM d
                GROUP BY topic, ar_guid, compute_category, sku
            UNION ALL
            -- batch_id
                SELECT  topic, ar_guid, namespace, compute_category,
                d.batch_id, NULL as job_id,
                NULL as cromwell_workflow_id, NULL as cromwell_sub_workflow_name, NULL as wdl_task_name,
                sku,
                batch_jobs_cnt.jobs as jobs,
                MIN(usage_start_time) as usage_start_time,
                MAX(usage_end_time) as usage_end_time, SUM(cost) as cost
                , MAX(labels) as labels
                FROM d
                INNER JOIN batch_jobs_cnt ON d.batch_id = batch_jobs_cnt.batch_id
                WHERE d.batch_id IS NOT NULL
                GROUP BY topic, ar_guid, batch_id, compute_category, namespace, sku, jobs
            UNION ALL
            -- job_id
                SELECT  topic, ar_guid, namespace, compute_category,
                d.batch_id, job_id,
                NULL as cromwell_workflow_id, NULL as cromwell_sub_workflow_name, NULL as wdl_task_name,
                sku,
                NULL as jobs,
                MIN(usage_start_time) as usage_start_time,
                MAX(usage_end_time) as usage_end_time, SUM(cost) as cost
                , MAX(labels) as labels
                FROM d
                WHERE job_id IS NOT NULL
                GROUP BY topic, ar_guid, d.batch_id, namespace, compute_category, job_id, sku

            UNION ALL
            -- cromwell-workflow-id
                SELECT  topic, ar_guid, namespace, compute_category,
                NULL as batch_id, NULL as job_id,
                cromwell_workflow_id, NULL as cromwell_sub_workflow_name, NULL as wdl_task_name,
                sku,
                NULL as jobs,
                MIN(usage_start_time) as usage_start_time,
                MAX(usage_end_time) as usage_end_time, SUM(cost) as cost
                , MAX(labels) as labels
                FROM d
                WHERE cromwell_workflow_id IS NOT NULL
                GROUP BY topic, ar_guid, compute_category, namespace, cromwell_workflow_id, sku
            UNION ALL
            -- cromwell-sub-workflow-id
                SELECT  topic, ar_guid, namespace, compute_category,
                NULL as batch_id, NULL as job_id,
                NULL as cromwell_workflow_id, cromwell_sub_workflow_name, NULL as wdl_task_name,
                sku,
                NULL as jobs,
                MIN(usage_start_time) as usage_start_time,
                MAX(usage_end_time) as usage_end_time, SUM(cost) as cost
                , MAX(labels) as labels
                FROM d
                WHERE cromwell_sub_workflow_name IS NOT NULL
                GROUP BY topic, ar_guid, compute_category, namespace, cromwell_sub_workflow_name, sku
            UNION ALL
            -- wdl-task-name
                SELECT  topic, ar_guid, namespace, compute_category,
                NULL as batch_id, NULL as job_id,
                NULL cromwell_workflow_id, NULL as cromwell_sub_workflow_name, wdl_task_name,
                sku,
                NULL as jobs,
                MIN(usage_start_time) as usage_start_time,
                MAX(usage_end_time) as usage_end_time, SUM(cost) as cost
                , MAX(labels) as labels
                FROM d
                WHERE wdl_task_name IS NOT NULL
                GROUP BY topic, ar_guid, compute_category, namespace, wdl_task_name, sku
            )
            -- final aggregation
            SELECT  topic, ar_guid, namespace, compute_category,
            batch_id, job_id,
            cromwell_workflow_id, cromwell_sub_workflow_name, wdl_task_name,
            jobs,
            MIN(usage_start_time) as start_time,
            MAX(usage_end_time) as end_time,
            SUM(cost) as cost,
            ARRAY_AGG(STRUCT(
                sku, cost
            )) as details
            , MAX(labels) as labels
            FROM t
            WHERE cost != 0 -- do not want to include empty cost records

            GROUP BY topic, ar_guid, batch_id, namespace, compute_category, job_id, jobs,
            cromwell_workflow_id, cromwell_sub_workflow_name, wdl_task_name

            ORDER BY topic, ar_guid, batch_id, job_id, cromwell_workflow_id,
            cromwell_sub_workflow_name, wdl_task_name
        """
        query_job_result = self._execute_query(_query, query_parameters, False)

        if query_job_result:
            return [
                BillingBatchCostRecord.from_json(dict(row)) for row in query_job_result
            ]

        # return empty list if no record found
        return []
