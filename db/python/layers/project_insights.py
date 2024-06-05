# pylint: disable=too-many-locals, too-many-instance-attributes
import asyncio
import itertools
from collections import defaultdict
from typing import Any
import json

from db.python.enum_tables import SequencingPlatformTable, SequencingTechnologyTable
from db.python.layers.base import BaseLayer
from db.python.tables.base import DbBase
from db.python.tables.project import ProjectPermissionsTable
from models.models import (
    AnalysisStatsInternal,
    ProjectInsightsDetailsInternal,
    ProjectInsightsSummaryInternal,
)
from models.models.project import ProjectId


class ProjectInsightsLayer(BaseLayer):
    """Project Insights layer - business logic for the project insights dashboards"""

    async def get_project_insights_summary(
        self,
        project_ids: list[ProjectId],
        sequencing_types: list[str],
    ) -> list[ProjectInsightsSummaryInternal]:
        """
        Get summary and analysis stats for a list of projects
        """
        spsdb = ProjectInsightsSummaryDb(self.connection)
        return await spsdb.get_project_insights_summary(
            project_ids=project_ids, sequencing_types=sequencing_types
        )

    async def get_project_insights_details(
        self,
        project_ids: list[ProjectId],
        sequencing_types: list[str],
    ) -> list[ProjectInsightsDetailsInternal]:
        """
        Get extensive sequencing group details for a list of projects
        """
        spsdb = ProjectInsightsSummaryDb(self.connection)
        return await spsdb.get_project_insights_details(
            project_ids=project_ids, sequencing_types=sequencing_types
        )


class ProjectInsightsSummaryDb(DbBase):
    """
    Db layer for project insights summary and details routes

    Used to get the summary and details for the projects stats dashboard
        - Summary
            - One row per project, sequencing type, and technology (platform not needed for now)
            - Total families, participants, samples, sequencing groups, CRAMs, and latest analyses
        - Details
            - One row per sequencing group
            - Only for sequencing groups that belong to participants with a family record
            - Gets web report links for each sequencing group
            - Checks if the sequencing group is in the latest completed analyses
                (CRAM, AnnotateDataset, SNV es-index, SV/gCNV es-index)
            - Get the family, participant, sample, and sequencing group details
    """

    # Summary queries
    async def _total_families_by_project_id_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[tuple[ProjectId, str, str], int]:
        _query = """
SELECT
    f.project,
    sg.type as sequencing_type,
    sg.technology as sequencing_technology,
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
    sg.type,
    sg.technology;
        """

        _query_results = await self.connection.fetch_all(
            _query,
            {
                "projects": project_ids,
                "sequencing_types": sequencing_types,
            },
        )
        total_families_by_project_id_and_seq_fields = {
            (row["project"], row["sequencing_type"], row["sequencing_technology"]): row[
                "num_families"
            ]
            for row in _query_results
        }
        # return defaultdict(int, total_families_by_project_id_and_seq_fields)
        return total_families_by_project_id_and_seq_fields

    async def _total_participants_by_project_id_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[tuple[ProjectId, str, str], int]:
        _query = """
SELECT
    p.project,
    sg.type as sequencing_type,
    sg.technology as sequencing_technology,
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
    sg.type,
    sg.technology;
        """
        _query_results = await self.connection.fetch_all(
            _query,
            {
                "projects": project_ids,
                "sequencing_types": sequencing_types,
            },
        )
        total_participants_by_project_id_and_seq_fields = {
            (row["project"], row["sequencing_type"], row["sequencing_technology"]): row[
                "num_participants"
            ]
            for row in _query_results
        }
        return total_participants_by_project_id_and_seq_fields

    async def _total_samples_by_project_id_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[tuple[ProjectId, str, str], int]:
        _query = """
SELECT
    s.project,
    sg.type as sequencing_type,
    sg.technology as sequencing_technology,
    COUNT(DISTINCT s.id) as num_samples
FROM
    sample s
    LEFT JOIN sequencing_group sg on sg.sample_id = s.id
WHERE
    s.project IN :projects
    AND sg.type IN :sequencing_types
GROUP BY
    s.project,
    sg.type,
    sg.technology;
        """
        _query_results = await self.connection.fetch_all(
            _query,
            {
                "projects": project_ids,
                "sequencing_types": sequencing_types,
            },
        )
        total_samples_by_project_id_and_seq_fields = {
            (row["project"], row["sequencing_type"], row["sequencing_technology"]): row[
                "num_samples"
            ]
            for row in _query_results
        }
        return total_samples_by_project_id_and_seq_fields

    async def _total_sequencing_groups_by_project_id_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[tuple[ProjectId, str, str], int]:
        _query = """
SELECT
    s.project,
    sg.type as sequencing_type,
    sg.technology as sequencing_technology,
    COUNT(DISTINCT sg.id) as num_sgs
FROM
    sequencing_group sg
    LEFT JOIN sample s on s.id = sg.sample_id
WHERE
    s.project IN :projects
    AND sg.type IN :sequencing_types
GROUP BY
    s.project,
    sg.type,
    sg.technology;
        """
        _query_results = await self.connection.fetch_all(
            _query,
            {
                "projects": project_ids,
                "sequencing_types": sequencing_types,
            },
        )
        total_sequencing_groups_by_project_id_and_seq_fields: dict[
            tuple[ProjectId, str, str], int
        ] = {
            (row["project"], row["sequencing_type"], row["sequencing_technology"]): row[
                "num_sgs"
            ]
            for row in _query_results
        }
        return total_sequencing_groups_by_project_id_and_seq_fields

    async def _crams_by_project_id_and_seq_fields(
        self,
        project_ids: list[ProjectId],
        sequencing_types: list[str],
    ) -> defaultdict[tuple[ProjectId, str, str], list[int]]:
        # Select distinct because there can be multiple completed CRAM analyses for a single sequencing group
        _query = """
SELECT DISTINCT
    a.project,
    sg.type as sequencing_type,
    sg.technology as sequencing_technology,
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
                "projects": project_ids,
                "sequencing_types": sequencing_types,
            },
        )
        # Return a dictionary with the project and sequencing fields as the key and a list of sequencing group ids as the value
        crams_by_project_id_and_seq_fields = defaultdict(list)
        for row in _query_results:
            crams_by_project_id_and_seq_fields[
                (row["project"], row["sequencing_type"], row["sequencing_technology"])
            ].append(row["sequencing_group_id"])

        return crams_by_project_id_and_seq_fields

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
        _query_results = await self.connection.fetch_all(
            _query,
            {
                "projects": project_ids,
                "sequencing_types": sequencing_types,
            },
        )
        latest_annotate_dataset_by_project_id_and_seq_type: defaultdict[
            tuple[ProjectId, str], dict[str, Any]
        ] = defaultdict(dict)
        for row in _query_results:
            latest_annotate_dataset_by_project_id_and_seq_type[
                (row["project"], row["sequencing_type"])
            ] = {
                "id": row["id"],
                "output": row["output"],
                "timestamp_completed": row["timestamp_completed"],
            }

        return latest_annotate_dataset_by_project_id_and_seq_type

    async def _latest_es_indices_by_project_id_and_seq_type_and_stage(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> defaultdict[tuple[ProjectId, str, str], dict[str, Any]]:
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
                "projects": project_ids,
                "sequencing_types": sequencing_types,
            },
        )
        latest_es_indices_by_project_id_and_seq_type_and_stage: defaultdict[
            tuple[ProjectId, str, str], dict[str, Any]
        ] = defaultdict(dict)
        for row in _query_results:
            latest_es_indices_by_project_id_and_seq_type_and_stage[
                (row["project"], row["sequencing_type"], row["stage"])
            ] = {
                "id": row["id"],
                "output": row["output"],
                "timestamp_completed": row["timestamp_completed"],
            }
        return latest_es_indices_by_project_id_and_seq_type_and_stage

    # Details queries
    async def _families_by_project_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> defaultdict[tuple[ProjectId, str, str, str], list[dict]]:
        _query = """
SELECT
    f.project,
    sg.type as sequencing_type,
    sg.platform as sequencing_platform,
    sg.technology as sequencing_technology,
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
    sg.platform,
    sg.technology,
    s.type,
    f.id,
    fp.participant_id,
    sg.id;
        """

        families = await self.connection.fetch_all(
            _query,
            {
                "projects": project_ids,
                "sequencing_types": sequencing_types,
            },
        )
        families_by_project_id_and_seq_fields = defaultdict(list)
        for row in families:
            families_by_project_id_and_seq_fields[
                (
                    row["project"],
                    row["sequencing_type"],
                    row["sequencing_platform"],
                    row["sequencing_technology"],
                )
            ].append(row)

        return families_by_project_id_and_seq_fields
    
    async def _details_stripy_reports(self, project_ids: list[ProjectId]) -> dict[ProjectId, int]:
        """Get stripy web report links"""
        _query = """
SELECT 
    a.project,
    a.id,
    a.type,
    a.timestamp_completed,
    asg.sequencing_group_id,
    JSON_EXTRACT(meta, '$.outliers_detected') as outliers_detected,
    JSON_QUERY(meta, '$.outlier_loci') as outlier_loci
FROM analysis a
LEFT JOIN analysis_sequencing_group asg on asg.analysis_id=a.id
INNER JOIN (
    SELECT
        asg.sequencing_group_id,
        MAX(timestamp_completed) as max_timestamp
    FROM analysis a
    LEFT JOIN analysis_sequencing_group asg on asg.analysis_id=a.id
    WHERE type='web'
    AND project IN :projects
    AND JSON_EXTRACT(meta, '$.stage') = 'Stripy'
    GROUP BY asg.sequencing_group_id
) max_timestamps ON asg.sequencing_group_id = max_timestamps.sequencing_group_id
AND a.timestamp_completed = max_timestamps.max_timestamp
WHERE a.project IN :projects
AND a.type = 'web'
AND JSON_EXTRACT(meta, '$.stage') = 'Stripy';
        """
        
        _query_results = await self.connection.fetch_all(
            _query,
            {
                "projects": project_ids,
            },
        )
        return {(row['project'], row['sequencing_group_id']): row for row in _query_results}
    
    async def _details_mito_reports(self, project_ids: list[ProjectId]) -> dict[ProjectId, int]:
        """Get mito web report links"""
        _query = """
SELECT 
    a.project,
    a.id,
    a.type,
    a.timestamp_completed,
    asg.sequencing_group_id
FROM analysis a
LEFT JOIN analysis_sequencing_group asg on asg.analysis_id=a.id
INNER JOIN (
    SELECT
        asg.sequencing_group_id,
        MAX(timestamp_completed) as max_timestamp
    FROM analysis a
    LEFT JOIN analysis_sequencing_group asg on asg.analysis_id=a.id
    WHERE type='web'
    AND project IN :projects
    AND JSON_EXTRACT(meta, '$.stage') = 'MitoReport'
    GROUP BY asg.sequencing_group_id
) max_timestamps ON asg.sequencing_group_id = max_timestamps.sequencing_group_id
AND a.timestamp_completed = max_timestamps.max_timestamp
WHERE a.project IN :projects
AND a.type = 'web'
AND JSON_EXTRACT(meta, '$.stage') = 'MitoReport';
        """
        _query_results = await self.connection.fetch_all(
            _query,
            {
                "projects": project_ids,
            },
        )
        return {(row['project'], row['sequencing_group_id']): row for row in _query_results}
    
    # Helper functions
    async def _get_sequencing_groups_by_analysis_ids(
        self, analysis_ids: list[int]
    ) -> dict[int, list[int]]:
        """Get sequencing groups for a list of analysis ids"""
        _query = """
SELECT
    analysis_id,
    GROUP_CONCAT(sequencing_group_id) as sequencing_group_ids
FROM
    analysis_sequencing_group
WHERE
    analysis_id IN :analysis_ids
GROUP BY
    analysis_id;
        """
        _query_results = await self.connection.fetch_all(
            _query,
            {
                "analysis_ids": analysis_ids,
            },
        )
        # Return a dictionary with the analysis ids as keys and their sequencing group ids as values
        sequencing_groups_by_analysis_id = {
            row["analysis_id"]: [
                int(sgid) for sgid in row["sequencing_group_ids"].split(",")
            ]
            for row in _query_results
        }
        return sequencing_groups_by_analysis_id

    async def get_analysis_sequencing_groups(self, all_group_analysis_rows: list[dict]):
        """
        Get the analysis IDs from the group analysis rows, which is a list of group analysis record dicts
        """
        analyses_to_query_sequencing_groups: list[int] = []
        for group_analysis_rows in all_group_analysis_rows:
            for row in group_analysis_rows.values():
                analyses_to_query_sequencing_groups.append(row["id"])

        return await self._get_sequencing_groups_by_analysis_ids(
            analyses_to_query_sequencing_groups
        )

    def get_sg_web_report_links(
        self,
        sequencing_group_stripy_reports,
        sequencing_group_mito_reports,
        project,
        sequencing_group_id: int,
    ):
        """
        Get the web report links for a sequencing group
        """
        report_links: defaultdict[str, dict[str, str]] = defaultdict(dict)
        
        stripy_report = sequencing_group_stripy_reports.get(
            (project.id, sequencing_group_id)
        )
        if stripy_report:
            report_links['stripy'] = {
                'stripy_url': (
                    f'https://main-web.populationgenomics.org.au/'
                    f'{project.name}/stripy/'
                    f'{sequencing_group_id}.stripy.html'
                ),
                'outliers_detected': stripy_report['outliers_detected'],
                'outlier_loci': json.loads(stripy_report['outlier_loci']) if stripy_report['outlier_loci'] else None,
                'timestamp_completed': stripy_report['timestamp_completed'].isoformat(),
            }

        mito_report = sequencing_group_mito_reports.get(
            (project.id, sequencing_group_id)
        )
        if mito_report:
            report_links['mito'] = {
                'mito_url': (
                    f'https://main-web.populationgenomics.org.au/'
                    f'{project.name}/mito/'
                    f'mitoreport-{sequencing_group_id}/index.html'
                ),
                'timestamp_completed': mito_report['timestamp_completed'].isoformat(),
            }

        return report_links

    # Main functions
    async def get_project_insights_summary(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ):
        """Combines the results of the above queries into a response"""
        ptable = ProjectPermissionsTable(self._connection)
        projects = await ptable.get_and_check_access_to_projects_for_ids(
            user=self.author, project_ids=project_ids, readonly=True
        )

        (
            total_families_by_project_id_and_seq_fields,
            total_participants_by_project_id_and_seq_fields,
            total_samples_by_project_id_and_seq_fields,
            total_sequencing_groups_by_project_id_and_seq_fields,
            crams_by_project_id_and_seq_fields,
            latest_annotate_dataset_by_project_id_and_seq_type,
            latest_es_indices_by_project_id_and_seq_type_and_stage,  # keyed by (project_id, sequencing_type, stage)
        ) = await asyncio.gather(
            self._total_families_by_project_id_and_seq_fields(
                project_ids, sequencing_types
            ),
            self._total_participants_by_project_id_and_seq_fields(
                project_ids, sequencing_types
            ),
            self._total_samples_by_project_id_and_seq_fields(
                project_ids, sequencing_types
            ),
            self._total_sequencing_groups_by_project_id_and_seq_fields(
                project_ids, sequencing_types
            ),
            self._crams_by_project_id_and_seq_fields(project_ids, sequencing_types),
            self._latest_annotate_dataset_by_project_id_and_seq_type(
                project_ids, sequencing_types
            ),
            self._latest_es_indices_by_project_id_and_seq_type_and_stage(
                project_ids,
                sequencing_types,
            ),
        )

        # Get the sequencing groups for each of the analyses in the grouped analyses rows
        analysis_sequencing_groups = await self.get_analysis_sequencing_groups(
            [
                latest_annotate_dataset_by_project_id_and_seq_type,
                latest_es_indices_by_project_id_and_seq_type_and_stage,
            ]
        )

        sequencing_technologies = await SequencingTechnologyTable(
            self._connection
        ).get()

        # Get all possible permutations of the projects, sequencing types, and sequencing technologies
        permutations = itertools.product(
            projects, sequencing_types, sequencing_technologies
        )

        response = []
        for project, sequencing_type, sequencing_technology in permutations:
            total_sequencing_groups = (
                total_sequencing_groups_by_project_id_and_seq_fields.get(
                    (project.id, sequencing_type, sequencing_technology), 0
                )
            )
            if total_sequencing_groups == 0:
                continue

            crams_in_project_with_sequencing_fields = (
                crams_by_project_id_and_seq_fields[
                    (project.id, sequencing_type, sequencing_technology)
                ]
            )

            if sequencing_technology == "short-read":
                latest_annotate_dataset = AnalysisStatsInternal(
                    id=latest_annotate_dataset_by_project_id_and_seq_type[
                        (project.id, sequencing_type)
                    ].get("id"),
                    sg_count=len(
                        analysis_sequencing_groups.get(
                            latest_annotate_dataset_by_project_id_and_seq_type[
                                (project.id, sequencing_type)
                            ].get("id"),
                            [],
                        )
                    ),
                    timestamp=latest_annotate_dataset_by_project_id_and_seq_type[
                        (project.id, sequencing_type)
                    ].get("timestamp_completed"),
                )

                latest_snv_es_index = AnalysisStatsInternal(
                    id=latest_es_indices_by_project_id_and_seq_type_and_stage[
                        (project.id, sequencing_type, "MtToEs")
                    ].get("id"),
                    name=latest_es_indices_by_project_id_and_seq_type_and_stage[
                        (project.id, sequencing_type, "MtToEs")
                    ].get("output"),
                    sg_count=len(
                        analysis_sequencing_groups.get(
                            latest_es_indices_by_project_id_and_seq_type_and_stage[
                                (project.id, sequencing_type, "MtToEs")
                            ].get("id"),
                            [],
                        )
                    ),
                    timestamp=latest_es_indices_by_project_id_and_seq_type_and_stage[
                        (project.id, sequencing_type, "MtToEs")
                    ].get("timestamp_completed"),
                )

                # SV index is only available for genome, treated as SV_WGS by seqr
                # GCNV is only available for exome, treated as SV_WES by seqr
                if sequencing_type not in ["genome", "exome"]:
                    latest_sv_es_index = None
                else:
                    if sequencing_type == "genome":
                        sv_index_stage = "MtToEsSv"
                    elif sequencing_type == "exome":
                        sv_index_stage = "MtToEsCNV"
                    latest_sv_es_index = AnalysisStatsInternal(
                        id=latest_es_indices_by_project_id_and_seq_type_and_stage[
                            (project.id, sequencing_type, sv_index_stage)
                        ].get("id"),
                        name=latest_es_indices_by_project_id_and_seq_type_and_stage[
                            (project.id, sequencing_type, sv_index_stage)
                        ].get("output"),
                        sg_count=len(
                            analysis_sequencing_groups.get(
                                latest_es_indices_by_project_id_and_seq_type_and_stage[
                                    (project.id, sequencing_type, sv_index_stage)
                                ].get("id"),
                                [],
                            )
                        ),
                        timestamp=latest_es_indices_by_project_id_and_seq_type_and_stage[
                            (project.id, sequencing_type, sv_index_stage)
                        ].get("timestamp_completed"),
                    )

            else:
                # If the sequencing technology is not short-read, set the latest analysis ids to None
                latest_annotate_dataset = None
                latest_snv_es_index = None
                latest_sv_es_index = None

            response.append(
                ProjectInsightsSummaryInternal(
                    project=project.id,
                    dataset=project.name,
                    sequencing_type=sequencing_type,
                    sequencing_technology=sequencing_technology,
                    total_families=total_families_by_project_id_and_seq_fields[
                        (project.id, sequencing_type, sequencing_technology)
                    ],
                    total_participants=total_participants_by_project_id_and_seq_fields[
                        (project.id, sequencing_type, sequencing_technology)
                    ],
                    total_samples=total_samples_by_project_id_and_seq_fields[
                        (project.id, sequencing_type, sequencing_technology)
                    ],
                    total_sequencing_groups=total_sequencing_groups,
                    total_crams=len(set(crams_in_project_with_sequencing_fields)),
                    latest_annotate_dataset=latest_annotate_dataset,
                    latest_snv_es_index=latest_snv_es_index,
                    latest_sv_es_index=latest_sv_es_index,
                )
            )

        return response

    async def get_project_insights_details(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ):
        """Combines the results of the queries above into a response"""
        ptable = ProjectPermissionsTable(self._connection)
        projects = await ptable.get_and_check_access_to_projects_for_ids(
            user=self.author, project_ids=project_ids, readonly=True
        )

        (
            families_by_project_id_and_seq_fields,
            crams_by_project_id_and_seq_fields,
            latest_annotate_dataset_by_project_id_and_seq_type,
            latest_es_indices_by_project_id_and_seq_type_and_stage,
            sequencing_group_stripy_reports,
            sequencing_group_mito_reports,
        ) = await asyncio.gather(
            self._families_by_project_and_seq_fields(project_ids, sequencing_types),
            self._crams_by_project_id_and_seq_fields(project_ids, sequencing_types),
            self._latest_annotate_dataset_by_project_id_and_seq_type(
                project_ids, sequencing_types
            ),
            self._latest_es_indices_by_project_id_and_seq_type_and_stage(
                project_ids, sequencing_types
            ),
            self._details_stripy_reports(project_ids),
            self._details_mito_reports(project_ids),
        )
        # Get the sequencing groups for each of the analyses in the grouped analyses rows
        analysis_sequencing_groups = await self.get_analysis_sequencing_groups(
            [
                latest_annotate_dataset_by_project_id_and_seq_type,
                latest_es_indices_by_project_id_and_seq_type_and_stage,
            ]
        )

        sequencing_platforms = await SequencingPlatformTable(self._connection).get()
        sequencing_technologies = await SequencingTechnologyTable(
            self._connection
        ).get()

        # Get all possible permutations of the projects, sequencing types, sequencing platforms, and sequencing technologies
        permutations = itertools.product(
            projects, sequencing_types, sequencing_platforms, sequencing_technologies
        )

        response = []
        for (
            project,
            sequencing_type,
            sequencing_platform,
            sequencing_technology,
        ) in permutations:
            sequencing_groups_with_crams = crams_by_project_id_and_seq_fields[
                (project.id, sequencing_type, sequencing_technology)
            ]
            family_rows = families_by_project_id_and_seq_fields[
                (
                    project.id,
                    sequencing_type,
                    sequencing_platform,
                    sequencing_technology,
                )
            ]
            if not family_rows:
                continue

            if sequencing_technology == "short-read":
                latest_annotate_dataset = AnalysisStatsInternal(
                    id=latest_annotate_dataset_by_project_id_and_seq_type[
                        (project.id, sequencing_type)
                    ].get("id"),
                    sg_count=len(
                        analysis_sequencing_groups.get(
                            latest_annotate_dataset_by_project_id_and_seq_type[
                                (project.id, sequencing_type)
                            ].get("id"),
                            [],
                        )
                    ),
                    timestamp=latest_annotate_dataset_by_project_id_and_seq_type[
                        (project.id, sequencing_type)
                    ].get("timestamp_completed"),
                )
                latest_snv_es_index = AnalysisStatsInternal(
                    id=latest_es_indices_by_project_id_and_seq_type_and_stage[
                        (project.id, sequencing_type, "MtToEs")
                    ].get("id"),
                    name=latest_es_indices_by_project_id_and_seq_type_and_stage[
                        (project.id, sequencing_type, "MtToEs")
                    ].get("output"),
                    sg_count=len(
                        analysis_sequencing_groups.get(
                            latest_es_indices_by_project_id_and_seq_type_and_stage[
                                (project.id, sequencing_type, "MtToEs")
                            ].get("id"),
                            [],
                        )
                    ),
                    timestamp=latest_es_indices_by_project_id_and_seq_type_and_stage[
                        (project.id, sequencing_type, "MtToEs")
                    ].get("timestamp_completed"),
                )
                if sequencing_type not in ["genome", "exome"]:
                    latest_sv_es_index = AnalysisStatsInternal(
                        id=None,
                        name=None,
                        sg_count=0,
                        timestamp=None,
                    )
                else:
                    if sequencing_type == "genome":
                        sv_index_stage = "MtToEsSv"
                    elif sequencing_type == "exome":
                        sv_index_stage = "MtToEsCNV"
                    latest_sv_es_index = AnalysisStatsInternal(
                        id=latest_es_indices_by_project_id_and_seq_type_and_stage[
                            (project.id, sequencing_type, sv_index_stage)
                        ].get("id"),
                        name=latest_es_indices_by_project_id_and_seq_type_and_stage[
                            (project.id, sequencing_type, sv_index_stage)
                        ].get("output"),
                        sg_count=len(
                            analysis_sequencing_groups.get(
                                latest_es_indices_by_project_id_and_seq_type_and_stage[
                                    (project.id, sequencing_type, sv_index_stage)
                                ].get("id"),
                                [],
                            )
                        ),
                        timestamp=latest_es_indices_by_project_id_and_seq_type_and_stage[
                            (project.id, sequencing_type, sv_index_stage)
                        ].get("timestamp_completed"),
                    )

                sequencing_groups_in_latest_annotate_dataset = (
                    analysis_sequencing_groups.get(latest_annotate_dataset.id, [])
                )
                sequencing_groups_in_latest_snv_es_index = (
                    analysis_sequencing_groups.get(latest_snv_es_index.id, [])
                )
                sequencing_groups_in_latest_sv_es_index = (
                    analysis_sequencing_groups.get(latest_sv_es_index.id, [])
                )

            else:
                # If the sequencing technology is not short-read, don't fetch the latest analyses
                latest_annotate_dataset = None
                latest_snv_es_index = None
                latest_sv_es_index = None

            for family_row in family_rows:
                if not family_row:
                    continue
                sequencing_group_id = family_row["sequencing_group_id"]
                web_reports = self.get_sg_web_report_links(
                    sequencing_group_stripy_reports,
                    sequencing_group_mito_reports,
                    project,
                    sequencing_group_id,
                )
                response.append(
                    ProjectInsightsDetailsInternal(
                        project=project.id,
                        dataset=project.name,
                        sequencing_type=sequencing_type,
                        sequencing_platform=sequencing_platform,
                        sequencing_technology=sequencing_technology,
                        sample_type=family_row["sample_type"],
                        family_id=family_row["family_id"],
                        family_ext_id=family_row["family_external_id"],
                        participant_id=family_row["participant_id"],
                        participant_ext_id=family_row["participant_external_id"],
                        sample_id=family_row["sample_id"],
                        sample_ext_ids=[family_row["sample_external_ids"]],
                        sequencing_group_id=sequencing_group_id,
                        completed_cram=family_row["sequencing_group_id"]
                        in sequencing_groups_with_crams,
                        in_latest_annotate_dataset=family_row["sequencing_group_id"]
                        in sequencing_groups_in_latest_annotate_dataset,
                        in_latest_snv_es_index=family_row["sequencing_group_id"]
                        in sequencing_groups_in_latest_snv_es_index,
                        in_latest_sv_es_index=family_row["sequencing_group_id"]
                        in sequencing_groups_in_latest_sv_es_index,
                        web_reports=web_reports,
                    )
                )

        return response
