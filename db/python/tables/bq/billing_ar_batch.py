from datetime import datetime, timedelta

from google.cloud import bigquery

from api.settings import (
    BQ_BATCHES_VIEW,
    SAMPLE_PREFIX,
    SEQUENCING_GROUP_PREFIX,
)
from api.utils.db import (
    Connection,
)
from db.python.tables.bq.billing_base import BillingBaseTable
from models.enums.billing import BillingSource
from models.models import BillingSampleQueryModel, BillingTotalCostQueryModel
from models.utils.sample_id_format import sample_id_transform_to_raw
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_format,
    sequencing_group_id_format_list,
    sequencing_group_id_transform_to_raw,
)


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
        connection: Connection,
        records_filter: list[str],
    ) -> tuple[
        list[str], list[int], dict[int, str], dict[int, str], dict[int, list[str]]
    ]:
        """
        Get sequencing groups from records filter
        """
        if not records_filter:
            raise ValueError('Sequencing_group containing is required')

        sequencing_groups: list[str] = []
        sequencing_groups_as_ids: list[int] = []
        seq_id_map: dict[int, str] = {}
        sample_id_map: dict[int, str] = {}
        sample_ids: list[int] = []
        sample_to_seq_grp: dict[int, list[str]] = {}
        for r in records_filter:
            if r.startswith(SEQUENCING_GROUP_PREFIX):
                sequencing_groups.append(r.upper())
                # convert SEQ to IDs
                seq_id = sequencing_group_id_transform_to_raw(r.upper())
                seq_id_map[seq_id] = r
                sequencing_groups_as_ids.append(seq_id)
            elif r.startswith(SAMPLE_PREFIX):
                # get all seq groups for this sample
                sample_id = sample_id_transform_to_raw(r.upper())
                sample_id_map[sample_id] = r
                sample_ids.append(sample_id)

        # map sample to seq groups
        if sample_ids:
            sample_id_to_seq_grp = await self.get_seq_groups_per_sample(
                connection, sample_ids
            )
            # append to sequencing_groups_as_ids
            for sample_id, seq_groups in sample_id_to_seq_grp.items():
                sequencing_groups_as_ids.extend(seq_groups)
                for seq_id in seq_groups:
                    r = sequencing_group_id_format(seq_id)
                    seq_id_map[seq_id] = r
                    sequencing_groups.append(r)

            # reformat the map, keep formatted list
            sample_to_seq_grp = {
                k: sequencing_group_id_format_list(v)
                for k, v in sample_id_to_seq_grp.items()
            }
        return (
            sequencing_groups,
            sequencing_groups_as_ids,
            seq_id_map,
            sample_id_map,
            sample_to_seq_grp,
        )

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

    async def get_projects_per_sq(
        self, connection: Connection, sequencing_groups_as_ids
    ) -> tuple[dict[int, list[int]], dict[int, str]]:
        """
        Get GCP project name by sequencing groups
        """
        _query = """
            SELECT s.project, GROUP_CONCAT(sg.id ORDER BY sg.id ASC SEPARATOR ',') as seq_grps
            FROM sequencing_group sg
            INNER JOIN sample s ON s.id = sg.sample_id
            WHERE sg.id in :sequencing_group_ids
            GROUP BY s.project
        """
        rows = await connection.connection.fetch_all(
            _query, {'sequencing_group_ids': sequencing_groups_as_ids}
        )
        projects = {
            row.project: [int(seq_id) for seq_id in row.seq_grps.split(',')]
            for row in rows
        }
        if not projects:
            return (None, None)

        # get the project dataset (GCP project names)
        project_names: dict[int, str] = {}
        for project_id in projects.keys():
            project_names[project_id] = connection.project_id_map[project_id].dataset
        return (projects, project_names)

    async def get_seq_groups_per_sample(
        self, connection: Connection, sample_ids: list[int]
    ) -> dict[int, list[int]]:
        """
        Get sequencing groups per sample ids
        Return map of Sample id -> seq group
        """
        query = """
            SELECT sample_id, GROUP_CONCAT(id ORDER BY id ASC SEPARATOR ',') as seq_grps
            FROM sequencing_group
            WHERE
                sample_id IN :sample_ids
                AND NOT archived
            GROUP BY sample_id
        """
        sample_seq_records = await connection.connection.fetch_all(
            query,
            {
                'sample_ids': sample_ids,
            },
        )
        sample_to_seq_groups = {
            row.sample_id: [int(seq_id) for seq_id in row.seq_grps.split(',')]
            for row in sample_seq_records
        }
        return sample_to_seq_groups

    def aggregate_per_sample(
        self, result, sequencing_groups, sample_id_map, sample_to_seq_grp
    ) -> list[dict]:
        """
        Aggregate results per sample if needed
        """
        # remap results to sample if needed
        mapped_results = []
        included_seq_groups = []
        for sample_id, seq_groups in sample_to_seq_grp.items():
            # aggregate all seq_groups into one record
            # sequencing_group contain formatted id
            # Group by all fields except 'sequencing_group', and 'cost'
            # Aggregate cost for each unique combination of other fields
            grouped: dict[tuple, list[dict]] = {}
            for r in result:
                # Build a key from all fields except 'sequencing_group' and 'cost'
                key = tuple(
                    (k, v)
                    for k, v in r.items()
                    if k not in ('sequencing_group', 'cost')
                )
                grouped.setdefault(key, []).append(r)

            for sample_id, seq_groups in sample_to_seq_grp.items():
                included_seq_groups.extend(seq_groups)
                for key, records in grouped.items():
                    # Filter records for this sample's seq_groups
                    filtered = [
                        r for r in records if r['sequencing_group'] in seq_groups
                    ]
                    if filtered:
                        # Use the first record for other fields
                        base = dict(dict(key).items())
                        mapped_results.append(
                            {
                                **base,
                                'sequencing_group': ','.join(seq_groups),
                                'sample': sample_id_map[sample_id],
                                'cost': sum(r['cost'] for r in filtered),
                            }
                        )

        # append to mapped results the seq groups which do not belong to any samples
        remaining_seq_groups = set(sequencing_groups) - set(included_seq_groups)
        for seq_group in remaining_seq_groups:
            # append all the records from result where sequencing_group == seq_group
            filtered = [r for r in result if r['sequencing_group'] == seq_group]
            mapped_results.extend(filtered)

        return mapped_results

    async def get_storage_info(
        self,
        connection: Connection,
        query: BillingTotalCostQueryModel,
        projects: dict,
        project_names: dict,
    ) -> tuple[dict[int, dict], dict, dict]:
        """
        Get storage information for the specified projects.
        """
        storage_cost = {}
        number_of_seq_groups = {}
        total_crams_size = {}
        for project_id in projects.keys():
            storage_cost[project_id] = self.get_storage_costs_by_project(
                query, project_names[project_id]
            )

            # 4. get number of seq groups
            number_of_seq_groups[
                project_id
            ] = await self.get_number_of_sequencing_groups(connection, project_id)

            # 4. Get the cram size per project, total size and count cram files
            (
                total_crams_size[project_id],
                _cram_files_cnt,
            ) = await self.get_total_crams_info(connection, project_id)
        return (storage_cost, number_of_seq_groups, total_crams_size)

    async def get_cost_by_sample(
        self,
        connection: Connection,
        query: BillingSampleQueryModel,
    ) -> list[dict] | None:
        """
        Get Sample cost of selected fields for requested time interval from BQ views

        SAMPLE_PREFIX , e.g 'XPG'
        SEQUENCING_GROUP_PREFIX , e.g 'CPG'
        COHORT_PREFIX , e.g  'COH'

        """
        # Get sequencing groups
        (
            sequencing_groups,
            sequencing_groups_as_ids,
            seq_id_map,
            sample_id_map,
            sample_to_seq_grp,
        ) = await self.get_sequencing_groups(connection, query.search_ids)

        # Get metamist project id and gcp project prefix
        (projects, project_names) = await self.get_projects_per_sq(
            connection, sequencing_groups_as_ids
        )
        if not projects:
            return []

        # 6. Get all ar-guid with min/max day for each of the seq group
        ar_guids = self.get_ar_guid_by_seq_groups(sequencing_groups)
        if not ar_guids:
            return []

        # get storage cost
        # 7. Get total compute cost associated with ar-guid
        compute_results = self.get_compute_costs_by_seq_groups(
            ar_guids, sequencing_groups, ','.join(query.fields)
        )

        # TODO min compute per seq group !!!

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
        # overrides time specific fields with relevant time column name
        q = BillingTotalCostQueryModel(
            fields=query.fields,
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            source=BillingSource.AGGREGATE,
        )

        # Storage Calculation per project
        (
            storage_cost,
            number_of_seq_groups,
            total_crams_size,
        ) = await self.get_storage_info(connection, q, projects, project_names)

        # 5. Get individual seq group cram size, list all seq even they do not have cram files
        result = compute_results
        for project_id in projects.keys():
            result += await self.get_cram_files_info(
                connection,
                project_id,
                sequencing_groups_as_ids,
                number_of_seq_groups[project_id],
                storage_cost[project_id],
                total_crams_size[project_id],
                seq_id_map,
                'sequencing_group' in query.fields,
            )

        if 'sequencing_group' in query.fields and sample_to_seq_grp:
            result = self.aggregate_per_sample(
                result, sequencing_groups, sample_id_map, sample_to_seq_grp
            )

        # sort by invoice month
        result = sorted(result, key=lambda k: k['invoice_month'])
        return result
