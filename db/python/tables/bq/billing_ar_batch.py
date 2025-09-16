from datetime import datetime, timedelta
from typing import Any

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
from models.models import (
    BillingColumn,
    BillingSampleQueryModel,
    BillingTotalCostQueryModel,
)
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

    async def get_records_first_timestamps(
        self,
        connection: Connection,
        sequencing_groups_as_ids: list[int],
        seq_id_map: dict[int, str],
    ) -> dict[str, datetime]:
        """
        Get min timestamp by sequencing groups
        """
        # Find the minimum day when samples has been added
        # using FOR SYSTEM_TIME ALL gives us full history
        _query = """
        SELECT sg.id, MIN(l.timestamp) as start_date
        FROM sequencing_group FOR SYSTEM_TIME ALL sg
        LEFT JOIN audit_log l on l.id = sg.audit_log_id
        WHERE sg.id IN :seq_groups
        GROUP BY sg.id
        """
        info_record = await connection.connection.fetch_all(
            _query,
            {
                'seq_groups': list(sequencing_groups_as_ids),
            },
        )

        # Map sequencing group IDs to their first record date
        # if audit log not present, use current datetime
        first_seq_record_date = {
            seq_id_map[row.id]: row.start_date if row.start_date else datetime.now()
            for row in info_record
        }
        return first_seq_record_date

    async def get_number_of_sequencing_groups(
        self, connection: Connection, project_id: int
    ) -> int:
        """
        Get number of sequencing groups for the given projects
        """
        _query = """
            SELECT COUNT(distinct sg.id) as cnt
            FROM sample s
            INNER JOIN sequencing_group sg ON sg.sample_id = s.id
            WHERE s.project IN :projects
        """
        info_record = await connection.connection.fetch_one(
            _query,
            {
                'projects': [project_id],
            },
        )
        return dict(info_record).get('cnt', 0)

    async def get_total_crams_info(
        self, connection: Connection, project_id: int
    ) -> float:
        """
        Get the cram size per project, total size and count cram files
        """
        _query = """
            SELECT sum(f.size) as total_crams_size
            FROM output_file f
            INNER JOIN analysis_outputs o ON o.file_id = f.id
            INNER JOIN analysis a ON a.id = o.analysis_id
            where a.type = 'CRAM'
            AND a.status = 'COMPLETED'
            and a.project IN :projects
        """
        info_record = await connection.connection.fetch_one(
            _query,
            {
                'projects': [project_id],
            },
        )

        total_cram_info = dict(info_record)
        total_crams_size = float(total_cram_info.get('total_crams_size') or 0)
        return total_crams_size

    async def calculate_storage_cost_using_cram_files_info(
        self,
        connection: Connection,
        project_id: int,
        sequencing_groups_as_ids: list[int],
        number_of_seq_groups: int,
        storage_cost: dict[str, float],
        total_crams_size: float,
        seq_id_map: dict[int, str],
        seq_grp_dates: dict[str, datetime],
        individual_sq: bool,
    ) -> list[dict]:
        """
        Get CRAM files information for the given sequencing groups
        """
        _query = """
            SELECT sg.id, sum(f.size) as crams_size
            FROM sequencing_group sg
            LEFT JOIN analysis_sequencing_group asg ON asg.sequencing_group_id = sg.id
            LEFT JOIN analysis a ON asg.analysis_id = a.id AND a.type = 'CRAM' AND a.status = 'COMPLETED' AND a.project = :project
            LEFT JOIN analysis_outputs o ON a.id = o.analysis_id
            LEFT JOIN output_file f ON o.file_id = f.id
            WHERE sg.id IN :seq_groups
            GROUP BY sg.id
        """
        records = await connection.connection.fetch_all(
            _query,
            {
                'project': project_id,
                'seq_groups': list(sequencing_groups_as_ids),
            },
        )
        sg_storage_cost: list[dict] = []
        cram_sizes = dict(
            (row['id'], float(row['crams_size'] if row['crams_size'] else 0))
            for row in records
        )
        for k, v in cram_sizes.items():
            for sk, sv in storage_cost.items():
                if sk >= seq_grp_dates[seq_id_map[k]].strftime('%Y%m'):
                    sg_storage_cost.append(
                        {
                            'invoice_month': sk,
                            'cost_category': 'Cloud Storage',
                            'sequencing_group': seq_id_map[k],
                            'cost': (
                                sv * float(v) / total_crams_size
                                if total_crams_size
                                else sv / number_of_seq_groups
                            ),
                        }
                    )

        if individual_sq:
            return sg_storage_cost

        # aggregate per invoice month
        aggregated_storage_cost = []
        mths = [r['invoice_month'] for r in sg_storage_cost]
        for mth in mths:
            total_cost = sum(
                r['cost'] for r in sg_storage_cost if r['invoice_month'] == mth
            )
            aggregated_storage_cost.append(
                {
                    'invoice_month': mth,
                    'cost_category': 'Cloud Storage',
                    'cost': total_cost,
                }
            )

        return aggregated_storage_cost

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

        # we need few mapping structures
        # metamist stores seq/sample by int ID
        # billing contains the string representation, not raw ID
        # we need to be able to map from one to another
        # we need sample to seq group map to aggregate by sample later
        sequencing_groups: list[str] = []
        sequencing_groups_as_ids: list[int] = []
        seq_id_map: dict[int, str] = {}
        sample_id_map: dict[int, str] = {}
        sample_ids: list[int] = []
        sample_to_seq_grp: dict[int, list[str]] = {}
        for r in records_filter:
            if r.startswith(SEQUENCING_GROUP_PREFIX):
                sequencing_groups.append(r.upper())
                # convert seq group to ID
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

        # return all the mappings
        return (
            sequencing_groups,
            sequencing_groups_as_ids,
            seq_id_map,
            sample_id_map,
            sample_to_seq_grp,
        )

    def get_batch_ids_by_seq_groups(
        self, sequencing_groups: list[str]
    ) -> dict[str, tuple[datetime, datetime]]:
        """Get all batch IDs and relevant dates by sequencing groups"""
        # get all batch IDs with min/max day for each of the seq group
        # sequencing_group can be a comma separated list of seq group ids,
        # so we need to use REGEXP_CONTAINS

        query_parameters = [
            bigquery.ArrayQueryParameter(
                'sequencing_groups', 'STRING', sequencing_groups
            )
        ]

        _query = f"""
        SELECT batch_id, min(min_day) as min_day, max(max_day) as max_day
        FROM `{self.table_name}`
        WHERE REGEXP_CONTAINS(UPPER(sequencing_group), ARRAY_TO_STRING(@sequencing_groups, '|'))
        AND batch_id IS NOT NULL
        group by batch_id
        """

        batch_ids = {}
        query_job_result = self._execute_query(_query, query_parameters)
        if query_job_result:
            for row in query_job_result:
                batch_ids[row.batch_id] = (row.min_day, row.max_day)

        return batch_ids

    async def get_projects_per_sq(
        self, connection: Connection, sequencing_groups_as_ids
    ) -> tuple[dict[int, list[int]], dict[int, str]]:
        """
        Create map project to sequencing groups and project names
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
        fields: list[BillingColumn],
        projects: dict,
        project_names: dict,
        project_dates: dict[int, tuple[datetime, datetime]],
    ) -> tuple[dict[int, dict], dict, dict]:
        """
        Get storage information for the specified projects.
        """
        storage_cost = {}
        number_of_seq_groups = {}
        total_crams_size = {}
        for project_id in projects.keys():
            # adjust Query dates filter
            # overrides time specific fields with relevant time column name
            start_date, end_date = project_dates[project_id]
            query = BillingTotalCostQueryModel(
                fields=fields,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                source=BillingSource.AGGREGATE,
            )
            storage_cost[project_id] = self.get_storage_costs_by_project(
                query, project_names[project_id]
            )

            # get number of seq groups
            number_of_seq_groups[project_id] = (
                await self.get_number_of_sequencing_groups(connection, project_id)
            )

            # Get the cram size per project, total size and count cram files
            total_crams_size[project_id] = await self.get_total_crams_info(
                connection, project_id
            )

        return (storage_cost, number_of_seq_groups, total_crams_size)

    async def get_dates_by_seq_groups(
        self,
        connection: Connection,
        seq_groups: list[int],
        seq_id_map: dict[int, str],
        compute_results: list[dict],
        query: BillingSampleQueryModel,
    ) -> tuple[dict[str, datetime], datetime]:
        """
        Combine seq group first stored record with compute results invoice month
        Get the min dates per seq_groups in compute results
        and max dates based on the initial query arguments
        """

        # combine compute start day and first day seq group were recorded in metamist
        first_seq_record_date = await self.get_records_first_timestamps(
            connection,
            seq_groups,
            seq_id_map,
        )
        # first_seq_record_date combine with the first compute record per sample
        combined_seq_record_date: dict[str, datetime] = {}
        for sid, first_date in first_seq_record_date.items():
            combined_seq_record_date[sid] = min(
                first_date,
                min(
                    # get the min invoice month from compute results for this seq group
                    (
                        datetime.strptime(result['invoice_month'], '%Y%m')
                        for result in compute_results
                        if result['sequencing_group'] == sid
                    ),
                    # if no compute results, use the first record date
                    default=first_date,
                ),
            )

        # now combined all for the project
        project_min_date = min(combined_seq_record_date.values())
        # combine project_min_date with query.start_date
        start_date = max(
            project_min_date, datetime.strptime(query.start_date, '%Y-%m-%d')
        )

        return (combined_seq_record_date, start_date)

    async def calc_sample_storage_costs(
        self,
        connection: Connection,
        query: BillingSampleQueryModel,
        projects,
        project_names,
        project_dates,
        seq_id_map,
        seq_grp_dates,
    ) -> list[Any]:
        """
        Storage Calculation per project
        """
        (
            storage_cost,
            number_of_seq_groups,
            total_crams_size,
        ) = await self.get_storage_info(
            connection, query.fields, projects, project_names, project_dates
        )

        # Get individual seq group cram size, all seq groups even they do not have cram files
        result: list[Any] = []
        for project_id, seq_group_ids in projects.items():
            result += await self.calculate_storage_cost_using_cram_files_info(
                connection,
                project_id,
                seq_group_ids,
                number_of_seq_groups[project_id],
                storage_cost[project_id],
                total_crams_size[project_id],
                seq_id_map,
                seq_grp_dates,
                'sequencing_group' in query.fields,
            )

        return result

    async def get_cost_by_sample(
        self,
        connection: Connection,
        query: BillingSampleQueryModel,
    ) -> list[Any]:
        """
        Calculate sample cost with selected fields for requested time interval
        """
        # Get sequencing groups and all the mappings
        (
            sequencing_groups,
            sequencing_groups_as_ids,
            seq_id_map,
            sample_id_map,
            sample_to_seq_grp,
        ) = await self.get_sequencing_groups(connection, query.search_ids)

        # Get all batch_id with min/max day for each of the seq group
        batch_ids = self.get_batch_ids_by_seq_groups(sequencing_groups)

        if not batch_ids:
            # There are no jobs containing the sequencing groups
            return []

        # Get total compute cost associated with batch_ids,
        # include 'sequencing_group' even if not in the required lists
        total_cost: list[Any] = self.get_compute_costs_by_seq_groups(
            batch_ids,
            sequencing_groups,
            ','.join(set(query.fields) | {'sequencing_group'}),
        )

        # Get metamist project id and gcp project prefix
        (projects, project_names) = await self.get_projects_per_sq(
            connection, sequencing_groups_as_ids
        )

        if projects:
            # if projects exist, we can calculate storage costs
            project_dates: dict[int, tuple[datetime, datetime]] = {}

            # loop through all the projects, get all seq groups and its compute results to get min/max dates
            seq_grp_dates: dict[str, datetime] = {}
            for project_id, seq_groups in projects.items():
                (seq_record_dates, start_date) = await self.get_dates_by_seq_groups(
                    connection, seq_groups, seq_id_map, total_cost, query
                )
                seq_grp_dates.update(seq_record_dates)
                project_dates[project_id] = (
                    start_date,
                    datetime.strptime(query.end_date, '%Y-%m-%d'),
                )

            # get storage costs and add to total_cost
            total_cost.extend(
                await self.calc_sample_storage_costs(
                    connection,
                    query,
                    projects,
                    project_names,
                    project_dates,
                    seq_id_map,
                    seq_grp_dates,
                )
            )

        if 'sequencing_group' in query.fields and sample_to_seq_grp:
            # re-map results to sample if needed
            total_cost = self.aggregate_per_sample(
                total_cost, sequencing_groups, sample_id_map, sample_to_seq_grp
            )

        # filter out any invoice months outside the original request
        # this can be due to cost itself is charged in the next invoice month
        # for compute costs we used all the sample available time frames,
        # to figure out when samples were first occured
        min_invoice_mth = query.start_date[:7].replace('-', '')
        max_invoice_mth = query.end_date[:7].replace('-', '')
        total_cost = [
            item
            for item in total_cost
            if item['invoice_month'] >= min_invoice_mth
            and item['invoice_month'] <= max_invoice_mth
        ]

        # sort by invoice month ASC
        total_cost = sorted(total_cost, key=lambda k: k['invoice_month'])
        return total_cost
