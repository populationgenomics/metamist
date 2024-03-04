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

    async def get_batch_cost_summary(
        self,
        start_time: datetime,
        end_time: datetime,
        batch_ids: list[str] | None,
        ar_guid: str | None,
    ) -> list[BillingBatchCostRecord]:
        """
        Get summary of AR run
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

        # The query will locate all the records for the given time range and batch_ids or ar_guid
        # Then it aggregate the cost per topic (top 3),
        # get counts of batches,
        # cost per hail, cromwell and dataproc
        # start and end date of the usage
        # total cost,
        # including cost by sku
        # by all labels (if any) - only predfined labels are used

        _query = f"""
            WITH d AS (
                    SELECT topic, ar_guid,
                    CASE WHEN compute_category IS NULL AND batch_id IS NOT NULL THEN
                    'hail batch' ELSE compute_category END AS category,
                    batch_id, CAST(job_id AS INT) job_id,
                    sku, cost,
                    usage_start_time, usage_end_time, sequencing_group, stage, wdl_task_name, cromwell_sub_workflow_name, cromwell_workflow_id,
                    JSON_VALUE(PARSE_JSON(labels), '$.batch_name') as batch_name,
                    JSON_VALUE(PARSE_JSON(labels), '$.job_name') as job_name
                    FROM `{self.table_name}`
                        WHERE day >= @start_time
                        AND day <= @end_time
                        AND {condition}
                )
            , arsku AS (
                SELECT sku, SUM(cost) as cost
                FROM d
                WHERE cost != 0
                GROUP BY sku
                ORDER BY cost DESC
            )
            , arskuacc AS (
                SELECT ARRAY_AGG(STRUCT(sku, cost)) AS skus,
                FROM arsku
            )
            ,seqgrp AS (
                select sequencing_group, stage, sum(cost) as cost from d group by stage, sequencing_group
            )
            ,seqgrpacc AS (
                SELECT ARRAY_AGG(STRUCT(sequencing_group, stage, cost)) as seq_groups
                FROM seqgrp
            )
            , t3 AS (
                    SELECT topic, SUM(cost) AS cost
                    FROM d
                    WHERE topic IS NOT NULL
                    GROUP BY topic
                    ORDER BY cost DESC
                    LIMIT 3 -- pick top 3 topics
                )
            , t3acc AS (
                SELECT ARRAY_AGG(STRUCT(topic, cost)) AS topics,
                FROM t3
                )
            , cc AS (
                SELECT category, sum(cost) AS cost,
                CASE WHEN category = 'hail batch' THEN COUNT(DISTINCT batch_id) ELSE NULL END as workflows
                FROM d
                GROUP BY category
                )
            , ccacc AS (
                SELECT ARRAY_AGG(STRUCT(cc.category, cc.cost, cc.workflows)) AS categories
                FROM cc
                )
            ,t AS (
                SELECT STRUCT(
                    ar_guid, sum(d.cost) AS cost,
                    MIN(usage_start_time) AS usage_start_time,
                    max(usage_end_time) AS usage_end_time
                ) AS total,
                FROM d
                GROUP BY ar_guid
            )
            , jsku AS (
                SELECT batch_id, job_id, sku, SUM(cost) as cost
                FROM d
                WHERE cost != 0
                AND batch_id IS NOT NULL
                AND job_id IS NOT NULL
                GROUP BY batch_id, job_id, sku
                ORDER BY batch_id, job_id, cost desc
            )
            , jskuacc AS (
                SELECT batch_id, job_id, ARRAY_AGG(STRUCT(sku, cost)) as skus
                FROM jsku
                GROUP BY batch_id, job_id
            )
            ,j AS (
                SELECT d.batch_id, d.job_id, d.job_name, sum(d.cost) AS cost,
                    MIN(usage_start_time) AS usage_start_time,
                    max(usage_end_time) AS usage_end_time
                FROM d
                WHERE d.batch_id IS NOT NULL AND d.job_id IS NOT NULL
                GROUP BY d.batch_id, d.job_id, d.job_name
                ORDER BY job_id
            )
            , jacc AS (
                SELECT j.batch_id, ARRAY_AGG(STRUCT(j.job_id, j.batch_id, job_name, cost, usage_start_time, usage_end_time, jskuacc.skus)) AS jobs
                from j INNER JOIN jskuacc on jskuacc.batch_id = j.batch_id AND jskuacc.job_id = j.job_id
                GROUP BY j.batch_id
            )
            , bsku AS (
                SELECT batch_id, sku, SUM(cost) as cost
                FROM d
                WHERE cost != 0
                AND batch_id IS NOT NULL
                GROUP BY batch_id, sku
                ORDER BY batch_id, cost desc
            )
            , bskuacc AS (
                SELECT batch_id, ARRAY_AGG(STRUCT(sku, cost)) as skus
                FROM bsku
                GROUP BY batch_id
            )
            ,b AS (
                SELECT d.batch_id, d.batch_name,
                    sum(d.cost) AS cost,
                    MIN(d.usage_start_time) AS usage_start_time,
                    max(d.usage_end_time) AS usage_end_time,
                    COUNT(DISTINCT d.job_id) as jobs_cnt
                FROM d
                WHERE d.batch_id IS NOT NULL
                GROUP BY batch_id, batch_name
                ORDER BY batch_id
            )
            ,bseqgrp AS (
                select batch_id, sequencing_group, stage, sum(cost) as cost from d group by batch_id, sequencing_group, stage
            )
            ,bseqgrpacc AS (
                SELECT batch_id, ARRAY_AGG(STRUCT(sequencing_group, stage, cost)) as seq_groups
                FROM bseqgrp
                GROUP BY batch_id
            )
            ,bacc as (
                SELECT ARRAY_AGG(STRUCT(b.batch_id, batch_name, cost, usage_start_time, usage_end_time, jobs_cnt, bskuacc.skus, jacc.jobs, bseqgrpacc.seq_groups)) AS batches
                from b
                INNER JOIN bskuacc on bskuacc.batch_id = b.batch_id
                INNER JOIN jacc ON jacc.batch_id = b.batch_id
                INNER JOIN bseqgrpacc ON bseqgrpacc.batch_id = b.batch_id
            )
            -- cromwell specific
            , wdlsku AS (
                SELECT wdl_task_name, sku, SUM(cost) as cost
                FROM d
                WHERE cost != 0
                AND wdl_task_name IS NOT NULL
                GROUP BY wdl_task_name, sku
                ORDER BY wdl_task_name, cost desc
            )
            , wdlskuacc AS (
                SELECT wdl_task_name, ARRAY_AGG(STRUCT(sku, cost)) as skus
                FROM wdlsku
                GROUP BY wdl_task_name
            )
            , wdl AS (
                SELECT d.wdl_task_name,
                    sum(d.cost) AS cost,
                    MIN(d.usage_start_time) AS usage_start_time,
                    max(d.usage_end_time) AS usage_end_time,
                    COUNT(DISTINCT d.job_id) as jobs_cnt
                FROM d
                WHERE d.wdl_task_name IS NOT NULL
                GROUP BY wdl_task_name
                ORDER BY wdl_task_name
            )
            ,wdlacc as (
                SELECT ARRAY_AGG(STRUCT(wdl.wdl_task_name, cost, usage_start_time, usage_end_time, wdlskuacc.skus)) AS wdl_tasks
                from wdl
                INNER JOIN wdlskuacc on wdlskuacc.wdl_task_name = wdl.wdl_task_name
            )
            , cwisku AS (
                SELECT cromwell_workflow_id, sku, SUM(cost) as cost
                FROM d
                WHERE cost != 0
                AND cromwell_workflow_id IS NOT NULL
                GROUP BY cromwell_workflow_id, sku
                ORDER BY cromwell_workflow_id, cost desc
            )
            , cwiskuacc AS (
                SELECT cromwell_workflow_id, ARRAY_AGG(STRUCT(sku, cost)) as skus
                FROM cwisku
                GROUP BY cromwell_workflow_id
            )
            , cwi AS (
                SELECT d.cromwell_workflow_id,
                    sum(d.cost) AS cost,
                    MIN(d.usage_start_time) AS usage_start_time,
                    max(d.usage_end_time) AS usage_end_time,
                    COUNT(DISTINCT d.job_id) as jobs_cnt
                FROM d
                WHERE d.cromwell_workflow_id IS NOT NULL
                GROUP BY cromwell_workflow_id
                ORDER BY cromwell_workflow_id
            )
            ,cwiacc as (
                SELECT ARRAY_AGG(STRUCT(cwi.cromwell_workflow_id, cost, usage_start_time, usage_end_time, cwiskuacc.skus)) AS cromwell_workflows
                from cwi
                INNER JOIN cwiskuacc on cwiskuacc.cromwell_workflow_id = cwi.cromwell_workflow_id
            )
            , cswsku AS (
                SELECT cromwell_sub_workflow_name, sku, SUM(cost) as cost
                FROM d
                WHERE cost != 0
                AND cromwell_sub_workflow_name IS NOT NULL
                GROUP BY cromwell_sub_workflow_name, sku
                ORDER BY cromwell_sub_workflow_name, cost desc
            )
            , cswskuacc AS (
                SELECT cromwell_sub_workflow_name, ARRAY_AGG(STRUCT(sku, cost)) as skus
                FROM cswsku
                GROUP BY cromwell_sub_workflow_name
            )
            , csw AS (
                SELECT d.cromwell_sub_workflow_name,
                    sum(d.cost) AS cost,
                    MIN(d.usage_start_time) AS usage_start_time,
                    max(d.usage_end_time) AS usage_end_time,
                    COUNT(DISTINCT d.job_id) as jobs_cnt
                FROM d
                WHERE d.cromwell_sub_workflow_name IS NOT NULL
                GROUP BY cromwell_sub_workflow_name
                ORDER BY cromwell_sub_workflow_name
            )
            ,cswacc as (
                SELECT ARRAY_AGG(STRUCT(csw.cromwell_sub_workflow_name, cost, usage_start_time, usage_end_time, cswskuacc.skus)) AS cromwell_sub_workflows
                from csw
                INNER JOIN cswskuacc on cswskuacc.cromwell_sub_workflow_name = csw.cromwell_sub_workflow_name
            )
            -- merge all in one record
            SELECT t.total, t3acc.topics, ccacc.categories, bacc.batches, arskuacc.skus, wdlacc.wdl_tasks, cswacc.cromwell_sub_workflows, cwiacc.cromwell_workflows, seqgrpacc.seq_groups
            FROM t, t3acc, ccacc, bacc, arskuacc, wdlacc, cswacc, cwiacc, seqgrpacc


        """

        query_job_result = self._execute_query(_query, query_parameters, False)

        if query_job_result:
            return [
                BillingBatchCostRecord.from_json(dict(row)) for row in query_job_result
            ]

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
                SELECT topic, ar_guid, NULL as namespace, MAX(compute_category) as compute_category,
                NULL as batch_id, NULL as job_id,
                NULL as cromwell_workflow_id, NULL as cromwell_sub_workflow_name, NULL as wdl_task_name,
                sku,
                NULL as jobs,
                MIN(usage_start_time) as usage_start_time,
                MAX(usage_end_time) as usage_end_time, SUM(cost) as cost
                , MAX(labels) as labels
                FROM d
                GROUP BY topic, ar_guid, sku
            UNION ALL
            -- batch_id
                SELECT  topic, ar_guid, namespace, MAX(compute_category) as compute_category,
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
                GROUP BY topic, ar_guid, batch_id, namespace, sku, jobs
            UNION ALL
            -- job_id
                SELECT  topic, ar_guid, namespace, MAX(compute_category) as compute_category,
                d.batch_id, job_id,
                NULL as cromwell_workflow_id, NULL as cromwell_sub_workflow_name, NULL as wdl_task_name,
                sku,
                NULL as jobs,
                MIN(usage_start_time) as usage_start_time,
                MAX(usage_end_time) as usage_end_time, SUM(cost) as cost
                , MAX(labels) as labels
                FROM d
                WHERE job_id IS NOT NULL
                GROUP BY topic, ar_guid, d.batch_id, namespace, job_id, sku

            UNION ALL
            -- cromwell-workflow-id
                SELECT  topic, ar_guid, namespace, MAX(compute_category) as compute_category,
                NULL as batch_id, NULL as job_id,
                cromwell_workflow_id, NULL as cromwell_sub_workflow_name, NULL as wdl_task_name,
                sku,
                NULL as jobs,
                MIN(usage_start_time) as usage_start_time,
                MAX(usage_end_time) as usage_end_time, SUM(cost) as cost
                , MAX(labels) as labels
                FROM d
                WHERE cromwell_workflow_id IS NOT NULL
                GROUP BY topic, ar_guid, namespace, cromwell_workflow_id, sku
            UNION ALL
            -- cromwell-sub-workflow-id
                SELECT  topic, ar_guid, namespace, MAX(compute_category) as compute_category,
                NULL as batch_id, NULL as job_id,
                NULL as cromwell_workflow_id, cromwell_sub_workflow_name, NULL as wdl_task_name,
                sku,
                NULL as jobs,
                MIN(usage_start_time) as usage_start_time,
                MAX(usage_end_time) as usage_end_time, SUM(cost) as cost
                , MAX(labels) as labels
                FROM d
                WHERE cromwell_sub_workflow_name IS NOT NULL
                GROUP BY topic, ar_guid, namespace, cromwell_sub_workflow_name, sku
            UNION ALL
            -- wdl-task-name
                SELECT  topic, ar_guid, namespace, MAX(compute_category) as compute_category,
                NULL as batch_id, NULL as job_id,
                NULL cromwell_workflow_id, NULL as cromwell_sub_workflow_name, wdl_task_name,
                sku,
                NULL as jobs,
                MIN(usage_start_time) as usage_start_time,
                MAX(usage_end_time) as usage_end_time, SUM(cost) as cost
                , MAX(labels) as labels
                FROM d
                WHERE wdl_task_name IS NOT NULL
                GROUP BY topic, ar_guid, namespace, wdl_task_name, sku
            )
            -- final aggregation
            SELECT  topic, ar_guid, namespace, MAX(compute_category) as compute_category,
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

            GROUP BY topic, ar_guid, batch_id, namespace, job_id, jobs,
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
