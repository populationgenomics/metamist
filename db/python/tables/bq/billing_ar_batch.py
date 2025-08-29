from datetime import datetime, timedelta

from google.cloud import bigquery

from api.settings import (
    BQ_BATCHES_VIEW,
    COHORT_PREFIX,
    SAMPLE_PREFIX,
    SEQUENCING_GROUP_PREFIX,
)
from api.utils.db import (
    Connection,
)
from db.python.tables.bq.billing_base import BillingBaseTable
from db.python.tables.sequencing_group import SequencingGroupTable
from models.models import BillingColumn, BillingTotalCostQueryModel
from models.utils.sequencing_group_id_format import sequencing_group_id_transform_to_raw


class BillingArBatchTable(BillingBaseTable):
    """Billing AR - BatchID lookup Big Query table"""

    table_name = BQ_BATCHES_VIEW

    def get_table_name(self):
        """Get table name"""
        return self.table_name

    async def get_batches_by_ar_guid(
        self, ar_guid: str
    ) -> tuple[datetime | None, datetime | None, list[str]]:
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
        ORDER BY batch_id;
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

    async def get_ar_guid_by_batch_id(
        self, batch_id: str | None
    ) -> tuple[datetime | None, datetime | None, str | None]:
        """
        Get ar_guid for given batch_id,
        if batch_id is found, but not ar_guid, then return batch_id back
        """
        if batch_id is None:
            return None, None, None

        _query = f"""
        SELECT ar_guid,
            MIN(min_day) as start_day,
            MAX(max_day) as end_day
        FROM `{self.table_name}`
        WHERE batch_id = @batch_id
        GROUP BY ar_guid
        ORDER BY ar_guid DESC;  -- make NULL values appear last
        """

        query_parameters = [
            bigquery.ScalarQueryParameter('batch_id', 'STRING', batch_id),
        ]
        query_job_result = self._execute_query(_query, query_parameters)
        if query_job_result:
            if len(query_job_result) > 1 and query_job_result[1]['ar_guid'] is not None:
                raise ValueError(f'Multiple ARs found for batch_id: {batch_id}')

            ar_guid = query_job_result[0]['ar_guid']
            start_day = query_job_result[0]['start_day']
            end_day = query_job_result[0]['end_day'] + timedelta(days=1)
            if ar_guid:
                return start_day, end_day, ar_guid

            # if ar_guid not found but batch_id exists
            return start_day, end_day, batch_id

        # return None if no ar_guid found
        return None, None, None

    async def get_sequencing_groups(
        self,
        records_filter: list[str],
    ) -> tuple[list[str], list[int], dict[int, str]]:
        """
        Get sequencing groups from records filter
        """
        if not records_filter:
            raise ValueError('Sequencing_group containing is required')

        sequencing_groups: list[str] = []
        sequencing_groups_as_ids: list[int] = []
        seq_id_map: dict[int, str] = {}
        for r in records_filter:
            if r.startswith(SEQUENCING_GROUP_PREFIX):
                sequencing_groups.append(r.upper())
                # convert SEQ to IDs
                seq_id = sequencing_group_id_transform_to_raw(r.upper())
                seq_id_map[seq_id] = r
                sequencing_groups_as_ids.append(seq_id)
            elif r.startswith(COHORT_PREFIX):
                # TODO: Get all seq groups for this cohort
                continue
            elif r.startswith(SAMPLE_PREFIX):
                # TODO: get all seq groups for this sample
                continue

        return (sequencing_groups, sequencing_groups_as_ids, seq_id_map)

    def get_ar_guid_by_seq_groups(
        self, sequencing_groups: list[str]
    ) -> dict[str, tuple[str, str]]:
        """Get all AR-GUIDs by sequencing groups"""
        query_parameters = [
            bigquery.ArrayQueryParameter(
                'sequencing_groups', 'STRING', sequencing_groups
            )
        ]

        # get all ar-guid with min/max day for each of the seq group
        # sequencing_group can be a comma separated list of seq group ids,
        # so we need to use REGEXP_CONTAINS
        _query = f"""
        SELECT ar_guid, min(min_day) as min_day, max(min_day) as max_day
        FROM `{self.table_name}`
        WHERE REGEXP_CONTAINS(UPPER(sequencing_group), ARRAY_TO_STRING(@sequencing_groups, '|'))
        group by ar_guid
        """

        ar_guids = {}
        query_job_result = self._execute_query(_query, query_parameters)
        if query_job_result:
            for row in query_job_result:
                ar_guids[row.ar_guid] = (row.min_day, row.max_day)

        return ar_guids

    async def get_project_name(
        self, connection: Connection, sequencing_groups_as_ids
    ) -> tuple[int | None, str | None]:
        """
        Get GCP project name by sequencing groups
        """
        sgt = SequencingGroupTable(connection)
        projects = await sgt.get_projects_by_sequencing_group_ids(
            sequencing_groups_as_ids
        )
        if not projects:
            return (None, None)

        if len(projects) > 1:
            raise ValueError('Sequencing_group from one project only')

        # get the project dataset (GCP project names)
        project_id = list(projects)[0]
        project_name = connection.project_id_map[project_id].dataset
        return (project_id, project_name)

    async def get_cost_by_sample(
        self,
        connection: Connection,
        query: BillingTotalCostQueryModel,
        ar_guids,
        sequencing_groups,
        sequencing_groups_as_ids,
        seq_id_map,
        project_id,
        project_name,
    ) -> list[dict] | None:
        """
        Get Sample cost of selected fields for requested time interval from BQ views

        SAMPLE_PREFIX , e.g 'XPG'
        SEQUENCING_GROUP_PREFIX , e.g 'CPG'
        COHORT_PREFIX , e.g  'COH'

        """
        # Get columns to select and to group by
        fields_selected, _group_by = self._prepare_aggregation(query)

        # 1. Extract all sequencing groups we need to include

        # 7. Get total compute cost associated with ar-guid
        compute_results = self.get_compute_costs_by_seq_groups(
            ar_guids, sequencing_groups, fields_selected
        )

        # get the min date from compute_results
        min_compute_date = datetime.strptime(
            min(result['invoice_month'] for result in compute_results), '%Y%m'
        )

        # combine compute start day and first day seq group were recorded in metamist
        (start_date, end_date) = await self.get_min_max_dates(
            connection,
            datetime.strptime(query.start_date, '%Y-%m-%d'),
            datetime.strptime(query.end_date, '%Y-%m-%d'),
            min_compute_date,
            sequencing_groups_as_ids,
        )

        # adjust Query dates filter
        query.start_date = start_date.strftime('%Y-%m-%d')
        query.end_date = end_date.strftime('%Y-%m-%d')
        # overrides time specific fields with relevant time column name
        query.filters = {BillingColumn.DAY: query.filters.get(BillingColumn.DAY, None)}

        # Storage Calculation
        storage_cost = self.get_storage_costs_by_project(query, project_name)

        # 4. get number of seq groups
        number_of_seq_groups = await self.get_number_of_sequencing_groups(
            connection, project_id
        )

        # 4. Get the cram size per project, total size and count cram files
        total_crams_size, _cram_files_cnt = await self.get_total_crams_info(
            connection, project_id
        )

        # 5. Get individual seq group cram size, list all seq even they do not have cram files
        sg_storage_cost = await self.get_cram_files_info(
            connection,
            project_id,
            sequencing_groups_as_ids,
            number_of_seq_groups,
            storage_cost,
            total_crams_size,
            seq_id_map,
            'sequencing_group' in fields_selected,
        )

        # combine
        result = compute_results + sg_storage_cost
        result = sorted(result, key=lambda k: k['invoice_month'])

        print('result', result)

        return result
