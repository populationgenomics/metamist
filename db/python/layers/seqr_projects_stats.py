# pylint: disable=too-many-locals, too-many-instance-attributes, too-many-lines
import asyncio
from collections import defaultdict
from typing import Any
from db.python.layers.base import BaseLayer
from db.python.tables.base import DbBase
from db.python.tables.project import ProjectPermissionsTable
from models.models import (
    AnalysisStats,
    SeqrProjectsDetailsInternal,
    SeqrProjectsSummaryInternal,
)
from models.models.project import ProjectId


class SeqrProjectsStatsLayer(BaseLayer):
    """Seqr Projects Stats layer - business logic for the seqr projects stats dashboards"""

    async def get_seqr_projects_stats_summary(
        self,
        project_ids: list[ProjectId],
        sequencing_types: list[str],
    ) -> list[SeqrProjectsSummaryInternal]:
        """
        Get summary and analysis stats for a list of projects
        """
        spsdb = SeqrProjectsStatsDb(self.connection)
        return await spsdb.get_seqr_projects_stats_summary(
            project_ids=project_ids, sequencing_types=sequencing_types
        )

    async def get_seqr_projects_stats_details(
        self,
        project_ids: list[ProjectId],
        sequencing_types: list[str],
    ) -> list[SeqrProjectsDetailsInternal]:
        """
        Get details for a list of projects
        """
        spsdb = SeqrProjectsStatsDb(self.connection)
        return await spsdb.get_seqr_projects_stats_details(
            project_ids=project_ids, sequencing_types=sequencing_types
        )


class SeqrProjectsStatsDb(DbBase):
    """
    Db layer for seqr projects stats summary and details routes
    
    The following queries are used to get the summary and details for the seqr projects stats dashboard
    For the summary stats, the queries return a dictionary with the project and sequencing type as the key 
    and the number of families, participants, samples, sequencing groups, CRAMs, etc. as the values.
    """
    # Summary queries
    async def _total_families_by_project_id_and_seq_type(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[tuple[ProjectId, str], int]:
        _query = """
SELECT
    f.project,
    sg.type as sequencing_type,
    COUNT(DISTINCT f.id) as num_families
FROM
    family f
    LEFT JOIN family_participant fp ON f.id = fp.family_id
    LEFT JOIN sample s ON fp.participant_id = s.participant_id
    LEFT JOIN sequencing_group sg on sg.sample_id = s.id
WHERE
    f.project IN :projects
    AND sg.type IN :sequencing_types
GROUP BY
    f.project,
    sg.type;
        """
        
        _query_results = await self.connection.fetch_all(
            _query,
            {
                'projects': project_ids,
                'sequencing_types': sequencing_types,
            },
        )
        total_families_by_project_id_and_seq_type = { (row['project'], row['sequencing_type']): row['num_families'] for row in _query_results }
        return defaultdict(int, total_families_by_project_id_and_seq_type)

    async def _total_participants_by_project_id_and_seq_type(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[tuple[ProjectId, str], int]:
        _query = """
SELECT
    p.project,
    sg.type as sequencing_type,
    COUNT(DISTINCT p.id) as num_participants
FROM
    participant p
    LEFT JOIN sample s ON p.id = s.participant_id
    LEFT JOIN sequencing_group sg on sg.sample_id = s.id
WHERE
    p.project IN :projects
    AND sg.type IN :sequencing_types
GROUP BY
    p.project,
    sg.type;
        """
        _query_results = await self.connection.fetch_all(
            _query,
            {
                'projects': project_ids,
                'sequencing_types': sequencing_types,
            },
        )
        total_participants_by_project_id_and_seq_type = { (row['project'], row['sequencing_type']): row['num_participants'] for row in _query_results}
        return defaultdict(int, total_participants_by_project_id_and_seq_type)

    async def _total_samples_by_project_id_and_seq_type(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[tuple[ProjectId, str], int]:
        _query = """
SELECT
    s.project,
    sg.type as sequencing_type,
    COUNT(DISTINCT s.id) as num_samples
FROM
    sample s
    LEFT JOIN sequencing_group sg on sg.sample_id = s.id
WHERE
    s.project IN :projects
    AND sg.type IN :sequencing_types
GROUP BY
    s.project,
    sg.type;
        """
        _query_results = await self.connection.fetch_all(
            _query,
            {
                'projects': project_ids,
                'sequencing_types': sequencing_types,
            },
        )
        total_samples_by_project_id_and_seq_type = { (row['project'], row['sequencing_type']): row['num_samples'] for row in _query_results}
        return defaultdict(int, total_samples_by_project_id_and_seq_type)

    async def _total_sequencing_groups_by_project_id_and_seq_type(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[tuple[ProjectId, str], int]:
        _query = """
SELECT
    s.project,
    sg.type as sequencing_type,
    COUNT(DISTINCT sg.id) as num_sgs
FROM
    sequencing_group sg
    LEFT JOIN sample s on s.id = sg.sample_id
WHERE
    s.project IN :projects
    AND sg.type IN :sequencing_types
GROUP BY
    s.project,
    sg.type;
        """
        _query_results = (
            await self.connection.fetch_all(
                _query,
                {
                    'projects': project_ids,
                    'sequencing_types': sequencing_types,
                },
            )
        )
        total_sequencing_groups_by_project_id_and_seq_type = { (row['project'], row['sequencing_type']): row['num_sgs'] for row in _query_results}
        return defaultdict(int, total_sequencing_groups_by_project_id_and_seq_type)

    async def _crams_by_project_id_and_seq_type(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> defaultdict[tuple[ProjectId, str], list[int]]:
        # Select distinct because there can be multiple completed CRAM analyses for a single sequencing group
        _query = """
SELECT DISTINCT
    a.project,
    sg.type as sequencing_type,
    asg.sequencing_group_id
FROM
    analysis a
    LEFT JOIN analysis_sequencing_group asg ON a.id = asg.analysis_id
    LEFT JOIN sequencing_group sg ON sg.id = asg.sequencing_group_id
WHERE
    a.project IN :projects
    AND sg.type IN :sequencing_types
    AND a.type = 'CRAM'
    and a.status = 'COMPLETED';
        """
        _query_results = await self.connection.fetch_all(
            _query,
            {
                'projects': project_ids,
                'sequencing_types': sequencing_types,
            },
        )
        # Return a dictionary with the project and sequencing type as the key and a list of sequencing group ids as the value
        crams_by_project_id_and_seq_type = defaultdict(list)
        for row in _query_results:
            crams_by_project_id_and_seq_type[(row['project'], row['sequencing_type'])].append(row['sequencing_group_id'])
            
        return crams_by_project_id_and_seq_type

    async def _latest_annotate_dataset_by_project_id_and_seq_type(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> defaultdict[tuple[ProjectId, str], dict[str, Any]]:
        _query = """
SELECT
    a.project,
    JSON_UNQUOTE(JSON_EXTRACT(a.meta, '$.sequencing_type')) as sequencing_type,
    a.id,
    a.output,
    a.timestamp_completed
FROM analysis a
INNER JOIN (
    SELECT
        project,
        MAX(timestamp_completed) as max_timestamp,
        JSON_UNQUOTE(JSON_EXTRACT(meta, '$.sequencing_type')) as sequencing_type
    FROM analysis
    WHERE
        status = 'COMPLETED'
        AND type = 'CUSTOM'
        AND JSON_EXTRACT(meta, '$.stage') = 'AnnotateDataset'
        AND JSON_UNQUOTE(JSON_EXTRACT(meta, '$.sequencing_type')) IN :sequencing_types
    GROUP BY project, JSON_EXTRACT(meta, '$.sequencing_type')
) max_timestamps ON a.project = max_timestamps.project
AND a.timestamp_completed = max_timestamps.max_timestamp
AND JSON_UNQUOTE(JSON_EXTRACT(a.meta, '$.sequencing_type')) = max_timestamps.sequencing_type
WHERE a.type = 'CUSTOM'
AND a.status = 'COMPLETED'
AND a.project IN :projects
AND JSON_UNQUOTE(JSON_EXTRACT(a.meta, '$.sequencing_type')) IN :sequencing_types
AND JSON_EXTRACT(a.meta, '$.stage') = 'AnnotateDataset';
    -- JSON_UNQUOTE is necessary to compare JSON values with IN operator
        """
        _query_results = (
            await self.connection.fetch_all(
                _query,
                {
                    'projects': project_ids,
                    'sequencing_types': sequencing_types,
                },
            )
        )
        latest_annotate_dataset_by_project_id_and_seq_type = { (row['project'], row['sequencing_type']): {'id': row['id'], 'output': row['output'], 'timestamp_completed': row['timestamp_completed']} for row in _query_results}
        return defaultdict(dict, latest_annotate_dataset_by_project_id_and_seq_type)

    async def _latest_es_indices_by_project_id_and_seq_type_and_stage(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[tuple[ProjectId, str, str], dict[str, Any]]:
        _query = """
SELECT
    a.project,
    JSON_UNQUOTE(JSON_EXTRACT(a.meta, '$.sequencing_type')) as sequencing_type,
    a.id,
    JSON_UNQUOTE(JSON_EXTRACT(a.meta, '$.stage')) as stage,
    a.output,
    a.timestamp_completed
FROM analysis a
INNER JOIN (
    SELECT
        project,
        MAX(timestamp_completed) as max_timestamp,
        JSON_UNQUOTE(JSON_EXTRACT(meta, '$.sequencing_type')) as sequencing_type,
        JSON_UNQUOTE(JSON_EXTRACT(meta, '$.stage')) as stage
    FROM analysis
    WHERE type='es-index'
    GROUP BY project, JSON_EXTRACT(meta, '$.sequencing_type'), JSON_EXTRACT(meta, '$.stage')
) max_timestamps ON a.project = max_timestamps.project
AND a.timestamp_completed = max_timestamps.max_timestamp
AND JSON_UNQUOTE(JSON_EXTRACT(a.meta, '$.sequencing_type')) = max_timestamps.sequencing_type
AND JSON_EXTRACT(a.meta, '$.stage') = max_timestamps.stage
WHERE a.project IN :projects
AND JSON_UNQUOTE(JSON_EXTRACT(a.meta, '$.sequencing_type')) in :sequencing_types;
        """
        _query_results = await self.connection.fetch_all(
            _query,
            {
                'projects': project_ids,
                'sequencing_types': sequencing_types,
            },
        )
        latest_es_indices_by_project_id_and_seq_type_and_stage = { (row['project'], row['sequencing_type'], row['stage']): {'id': row['id'], 'output': row['output'], 'timestamp_completed': row['timestamp_completed']} for row in _query_results}
        return defaultdict(dict, latest_es_indices_by_project_id_and_seq_type_and_stage)
    
    # Details queries
    async def _families_by_project_and_sequencing_type(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> defaultdict[tuple[ProjectId, str], list[dict]]:
        _query = """
SELECT
    f.project,
    sg.type as sequencing_type,
    s.type as sample_type,
    f.id as family_id,
    f.external_id as family_external_id,
    fp.participant_id as participant_id,
    p.external_id as participant_external_id,
    s.id as sample_id,
    s.external_id as sample_external_ids,
    sg.id as sequencing_group_id
FROM
    family f
    LEFT JOIN family_participant fp ON f.id = fp.family_id
    LEFT JOIN participant p ON fp.participant_id = p.id
    LEFT JOIN sample s ON p.id = s.participant_id
    LEFT JOIN sequencing_group sg on sg.sample_id = s.id
WHERE
    f.project IN :projects
    AND sg.type IN :sequencing_types
ORDER BY
    f.project,
    sg.type,
    s.type,
    f.id,
    fp.participant_id,
    sg.id;
        """

        families = await self.connection.fetch_all(
            _query,
            {
                'projects': project_ids,
                'sequencing_types': sequencing_types,
            },
        )
        families_by_project_id_and_seq_type = defaultdict(list)
        for row in families:
            families_by_project_id_and_seq_type[(row['project'], row['sequencing_type'])].append(row)

        return families_by_project_id_and_seq_type
    
    
    async def _details_sequencing_groups_report_links(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[tuple[ProjectId, str, int], dict[str, bool]]:
        """Get sequencing group web report links"""
        _query = """
WITH mito AS (
    SELECT DISTINCT asg.sequencing_group_id
    FROM analysis a
    LEFT JOIN analysis_sequencing_group asg on asg.analysis_id=a.id
    WHERE status = 'COMPLETED'
    AND type = 'WEB'
    AND meta LIKE '%mitoreport%'
),
stripy AS (
    SELECT DISTINCT asg.sequencing_group_id
    FROM analysis a
    LEFT JOIN analysis_sequencing_group asg on asg.analysis_id=a.id
    WHERE status = 'COMPLETED'
    AND type = 'WEB'
    AND meta LIKE '%stripy%'
)

SELECT DISTINCT
    a.project,
    sg.type as sequencing_type,
    sg.id as sequencing_group_id,
    CASE WHEN stripy.sequencing_group_id IS NOT NULL THEN TRUE ELSE FALSE END as stripy,
    CASE WHEN mito.sequencing_group_id IS NOT NULL THEN TRUE ELSE FALSE END as mito
FROM
    sequencing_group sg
    INNER JOIN analysis_sequencing_group asg ON sg.id = asg.sequencing_group_id
    INNER JOIN analysis a ON asg.analysis_id = a.id
    LEFT JOIN mito on mito.sequencing_group_id = sg.id
    LEFT JOIN stripy on stripy.sequencing_group_id = sg.id
WHERE
    a.project IN :projects
    AND sg.type IN :sequencing_types
    AND a.status = 'COMPLETED'
    AND a.type = 'WEB';
        """

        _query_results = await self.connection.fetch_all(
            _query,
            {
                'projects': project_ids,
                'sequencing_types': sequencing_types,
            },
        )
        return { (row['project'], row['sequencing_type'], row['sequencing_group_id']): {'stripy': row['stripy'], 'mito': row['mito']} for row in _query_results}

    # Helper functions    
    async def _get_sequencing_groups_by_analysis_ids(self, analysis_ids: list[int]) -> defaultdict[int, list[int]]:
        """Get sequencing groups for a list of analysis ids"""
        _query = """
SELECT
    analysis_id,
    sequencing_group_id
FROM 
    analysis_sequencing_group
WHERE 
    analysis_id IN :analysis_ids;
        """
        _query_results = await self.connection.fetch_all(
            _query,
            {
                'analysis_ids': analysis_ids,
            },
        )
        # Return a dictionary with the analysis ids as keys and their sequencing group ids as values
        sequencing_groups_by_analysis_id = defaultdict(list)
        for row in _query_results:
            sequencing_groups_by_analysis_id[row['analysis_id']].append(row['sequencing_group_id'])

        return sequencing_groups_by_analysis_id

    async def get_analysis_sequencing_groups(self, all_group_analysis_rows: list[dict]):
        """
        Get the analysis IDs from the group analysis rows, which is a list of group analysis record dicts
        """
        analyses_to_query_sequencing_groups: list[int] = []
        for group_analysis_rows in all_group_analysis_rows:
            for row in group_analysis_rows.values():
                analyses_to_query_sequencing_groups.append(row['id'])
        
        return await self._get_sequencing_groups_by_analysis_ids(analyses_to_query_sequencing_groups)
    
    def get_sg_web_report_links(self, sequencing_group_web_reports, project, sequencing_type: str, sequencing_group_id: int):
        """
        Get the web report links for a sequencing group
        """
        report_links = {}
        sg_reports = sequencing_group_web_reports.get((project.id, sequencing_type, sequencing_group_id))
        if not sg_reports:
            return report_links
        
        stripy = sequencing_group_web_reports[(project.id, sequencing_type, sequencing_group_id)]['stripy']
        if stripy:
            report_links['stripy'] = (
                f'https://main-web.populationgenomics.org.au/'
                f'{project.name}/stripy/'
                f'{sequencing_group_id}.stripy.html'
            )

        mito = sequencing_group_web_reports[(project.id, sequencing_type, sequencing_group_id)]['mito']
        if mito:
            report_links['mito'] = (
                f'https://main-web.populationgenomics.org.au/'
                f'{project.name}/mito/'
                f'mitoreport-{sequencing_group_id}/index.html'
            )
        
        return report_links
    
    # Main functions
    async def get_seqr_projects_stats_summary(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ):
        """Combines the results of the above queries into a response"""

        ptable = ProjectPermissionsTable(self._connection)
        projects = await ptable.get_and_check_access_to_projects_for_ids(
            user=self.author, project_ids=project_ids, readonly=True
        )

        (   # each of these is keyed by (project_id, sequencing_type)
            total_families_by_project_id_and_seq_type,
            total_participants_by_project_id_and_seq_type,
            total_samples_by_project_id_and_seq_type,
            total_sequencing_groups_by_project_id_and_seq_type,
            crams_by_project_id_and_seq_type,
            latest_annotate_dataset_by_project_id_and_seq_type,
            latest_es_indices_by_project_id_and_seq_type_and_stage, # keyed by (project_id, sequencing_type, stage)
        ) = await asyncio.gather(
            self._total_families_by_project_id_and_seq_type(project_ids, sequencing_types),
            self._total_participants_by_project_id_and_seq_type(project_ids, sequencing_types),
            self._total_samples_by_project_id_and_seq_type(project_ids, sequencing_types),
            self._total_sequencing_groups_by_project_id_and_seq_type(
                project_ids, sequencing_types
            ),
            self._crams_by_project_id_and_seq_type(project_ids, sequencing_types),
            self._latest_annotate_dataset_by_project_id_and_seq_type(
                project_ids, sequencing_types
            ),
            self._latest_es_indices_by_project_id_and_seq_type_and_stage(project_ids, sequencing_types,),
        )
        
        # Get the sequencing groups for each of the analyses in the grouped analyses rows 
        # (latest_annotate_dataset and latest_es_indices) 
        # TODO: Add multiQC and other grouped analyses to this
        analysis_sequencing_groups = await self.get_analysis_sequencing_groups([latest_annotate_dataset_by_project_id_and_seq_type, latest_es_indices_by_project_id_and_seq_type_and_stage])

        response = []
        for project in projects:
            for sequencing_type in sequencing_types:
                crams_in_project_and_sequencing_type = crams_by_project_id_and_seq_type[(project.id, sequencing_type)]
                latest_annotate_dataset_id = latest_annotate_dataset_by_project_id_and_seq_type[(project.id, sequencing_type)].get('id')
                latest_snv_index_id = latest_es_indices_by_project_id_and_seq_type_and_stage[(project.id, sequencing_type, 'MtToEs')].get('id')
                # SV index is only available for genome, treated as SV_WGS by seqr
                # GCNV is only available for exome, treated as SV_WES by seqr
                if sequencing_type == 'genome':
                    sv_index_stage = 'MtToEsSv'
                    latest_sv_index_id = latest_es_indices_by_project_id_and_seq_type_and_stage[(project.id, sequencing_type, sv_index_stage)].get('id')
                elif sequencing_type == 'exome':
                    sv_index_stage = 'MtToEsCNV'
                    latest_sv_index_id = latest_es_indices_by_project_id_and_seq_type_and_stage[(project.id, sequencing_type, sv_index_stage)].get('id')
                else:
                    latest_sv_index_id = None
                response.append(
                    SeqrProjectsSummaryInternal(
                        project=project.id,
                        dataset=project.name,
                        sequencing_type=sequencing_type,
                        total_families=total_families_by_project_id_and_seq_type[(project.id, sequencing_type)],
                        total_participants=total_participants_by_project_id_and_seq_type[(project.id, sequencing_type)],
                        total_samples=total_samples_by_project_id_and_seq_type[(project.id, sequencing_type)],
                        total_sequencing_groups=total_sequencing_groups_by_project_id_and_seq_type[(project.id, sequencing_type)],
                        total_crams=len(set(crams_in_project_and_sequencing_type)),
                        latest_annotate_dataset=AnalysisStats(
                            id=latest_annotate_dataset_id,
                            sg_count=len(analysis_sequencing_groups[latest_annotate_dataset_id])
                        ),
                        latest_snv_es_index=AnalysisStats(
                            id=latest_snv_index_id,
                            sg_count=len(analysis_sequencing_groups[latest_snv_index_id]),
                        ),
                        latest_sv_es_index=AnalysisStats(
                            id=latest_sv_index_id,
                            sg_count=len(analysis_sequencing_groups[latest_sv_index_id]),
                        ),
                    )
                )

        return response

    async def get_seqr_projects_stats_details(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ):
        """Combines the results of the queries above into a response"""
        ptable = ProjectPermissionsTable(self._connection)
        projects = await ptable.get_and_check_access_to_projects_for_ids(
            user=self.author, project_ids=project_ids, readonly=True
        )

        (
            families_by_project_id_and_seq_type,
            crams_by_project_id_and_seq_type,
            latest_annotate_dataset_by_project_id_and_seq_type,
            latest_es_indices_by_project_id_and_seq_type_and_stage,
            sequencing_group_web_reports,
        ) = await asyncio.gather(
            self._families_by_project_and_sequencing_type(project_ids, sequencing_types),
            self._crams_by_project_id_and_seq_type(project_ids, sequencing_types),
            self._latest_annotate_dataset_by_project_id_and_seq_type(
                project_ids, sequencing_types
            ),
            self._latest_es_indices_by_project_id_and_seq_type_and_stage(project_ids, sequencing_types),
            self._details_sequencing_groups_report_links(
                project_ids, sequencing_types
            ),
        )
        # Get the sequencing groups for each of the analyses in the grouped analyses rows 
        # (latest_annotate_dataset and latest_es_indices) 
        # TODO: Add multiQC and other grouped analyses to this
        analysis_sequencing_groups = await self.get_analysis_sequencing_groups([latest_annotate_dataset_by_project_id_and_seq_type, latest_es_indices_by_project_id_and_seq_type_and_stage])

        response = []
        for project in projects:
            for sequencing_type in sequencing_types:
                sequencing_groups_with_crams = crams_by_project_id_and_seq_type[(project.id, sequencing_type)]
                latest_annotate_dataset_id = latest_annotate_dataset_by_project_id_and_seq_type[(project.id, sequencing_type)].get('id')
                sequencing_groups_in_latest_annotate_dataset = analysis_sequencing_groups[latest_annotate_dataset_id]
                latest_snv_es_index_id = latest_es_indices_by_project_id_and_seq_type_and_stage[(project.id, sequencing_type, 'MtToEs')].get('id')
                sequencing_groups_in_latest_snv_es_index = analysis_sequencing_groups[latest_snv_es_index_id]
                if sequencing_type == 'genome':
                    latest_sv_es_index_id = latest_es_indices_by_project_id_and_seq_type_and_stage[(project.id, sequencing_type, 'MtToEsSv')].get('id')
                elif sequencing_type == 'exome':
                    latest_sv_es_index_id = latest_es_indices_by_project_id_and_seq_type_and_stage[(project.id, sequencing_type, 'MtToEsCNV')].get('id')
                else:
                    latest_sv_es_index_id = None
                sequencing_groups_in_latest_sv_es_index = analysis_sequencing_groups[latest_sv_es_index_id]
                for family_row in families_by_project_id_and_seq_type[(project.id, sequencing_type)]:
                    sequencing_group_id = family_row['sequencing_group_id']
                    report_links = self.get_sg_web_report_links(sequencing_group_web_reports, project, sequencing_type, sequencing_group_id)

                    response.append(
                        SeqrProjectsDetailsInternal(
                            project=project.id,
                            dataset=project.name,
                            sequencing_type=sequencing_type,
                            sample_type=family_row['sample_type'],
                            family_id=family_row['family_id'],
                            family_ext_id=family_row['family_external_id'],
                            participant_id=family_row['participant_id'],
                            participant_ext_id=family_row['participant_external_id'],
                            sample_id=family_row['sample_id'],
                            sample_ext_ids=[family_row['sample_external_ids']],
                            sequencing_group_id=sequencing_group_id,
                            completed_cram=family_row['sequencing_group_id']
                            in sequencing_groups_with_crams,
                            in_latest_annotate_dataset=family_row['sequencing_group_id']
                            in sequencing_groups_in_latest_annotate_dataset,
                            in_latest_snv_es_index=family_row['sequencing_group_id']
                            in sequencing_groups_in_latest_snv_es_index,
                            in_latest_sv_es_index=family_row['sequencing_group_id']
                            in sequencing_groups_in_latest_sv_es_index,
                            sequencing_group_report_links=report_links,
                        )
                    )

        return response
