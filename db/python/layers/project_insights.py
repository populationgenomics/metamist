# mypy: disable-error-code="attr-defined,arg-type,index,call-overload"
# pylint: disable=too-many-arguments,too-many-locals,missing-class-docstring,too-many-lines
import asyncio
import itertools
import json
from datetime import datetime
from typing import Any, NamedTuple

from databases.interfaces import Record

from db.python.enum_tables import SequencingPlatformTable as SeqPlatformTable
from db.python.enum_tables import SequencingTechnologyTable as SeqTechTable
from db.python.layers.base import BaseLayer
from db.python.tables.base import DbBase
from models.models import (
    AnalysisStatsInternal,
    ProjectInsightsDetailsInternal,
    ProjectInsightsSummaryInternal,
)
from models.models.project import Project, ProjectId, ReadAccessRoles
from models.models.sequencing_group import SequencingGroupInternalId
from models.utils.sequencing_group_id_format import sequencing_group_id_format

AnalysisId = int
SequencingType = str
SequencingTechnology = str
SequencingPlatform = str


# This layer has a lot of different queries, so we'll define some namedtuples to help us keep track of the keys
class ProjectSeqTypeKey(NamedTuple):
    project: ProjectId
    sequencing_type: SequencingType


class ProjectSeqTypeTechnologyKey(NamedTuple):
    project: ProjectId
    sequencing_type: SequencingType
    sequencing_technology: SequencingTechnology


class ProjectSeqTypeTechnologyPlatformKey(NamedTuple):
    project: ProjectId
    sequencing_type: SequencingType
    sequencing_technology: SequencingTechnology
    sequencing_platform: SequencingPlatform


class ProjectSeqTypeStageKey(NamedTuple):
    project: ProjectId
    sequencing_type: SequencingType
    stage: str


class ProjectSeqGroupKey(NamedTuple):
    project: ProjectId
    sequencing_group_id: SequencingGroupInternalId


# Namedtuples for the rows returned by the queries
class AnalysisRow(NamedTuple):
    id: AnalysisId
    output: str
    timestamp_completed: datetime


class SequencingGroupDetailRow(NamedTuple):
    family_id: int
    family_external_id: str
    participant_id: int
    participant_external_id: str
    sample_id: int
    sample_external_ids: list[str]
    sample_type: str
    sequencing_group_id: SequencingGroupInternalId


class StripyReportRow(NamedTuple):
    id: AnalysisId
    output: str
    outliers_detected: bool
    outlier_loci: str
    timestamp_completed: datetime


SV_INDEX_SEQ_TYPE_STAGE_MAP = {
    'genome': 'MtToEsSv',
    'exome': 'MtToEsCNV',
}


class ProjectInsightsLayer(BaseLayer):
    """Project Insights layer - business logic for the project insights dashboards"""

    async def get_project_insights_summary(
        self,
        project_names: list[str],
        sequencing_types: list[SequencingType],
    ) -> list[ProjectInsightsSummaryInternal]:
        """
        Get summary and analysis stats for a list of projects
        """
        pidb = ProjectInsightsDb(self.connection)
        return await pidb.get_project_insights_summary(
            project_names=project_names, sequencing_types=sequencing_types
        )

    async def get_project_insights_details(
        self,
        project_names: list[str],
        sequencing_types: list[SequencingType],
    ) -> list[ProjectInsightsDetailsInternal]:
        """
        Get extensive sequencing group details for a list of projects
        """
        pidb = ProjectInsightsDb(self.connection)
        return await pidb.get_project_insights_details(
            project_names=project_names, sequencing_types=sequencing_types
        )


class ProjectInsightsDb(DbBase):
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

    # Helper functions
    def get_analysis_row(self, row: Record) -> AnalysisRow:
        """Parse a table row returned by fetch_all into an AnalysisRow object"""
        return AnalysisRow(
            id=row['id'],
            output=row['output'],
            timestamp_completed=row['timestamp_completed'],
        )

    def convert_to_external_ids(self, external_ids_value: str | list[str]) -> list[str]:
        """Converts a string or list of strings to a list of strings"""
        if isinstance(external_ids_value, str):
            return [external_ids_value]
        return external_ids_value

    def parse_project_seqtype_technology_keyed_rows(
        self, rows: list[Record], value_field: str
    ) -> dict[ProjectSeqTypeTechnologyKey, Any]:
        """
        Parse rows that are keyed by project, sequencing type, and sequencing technology
        """
        parsed_rows: dict[
            ProjectSeqTypeTechnologyKey,
            dict[str, Any] | list[SequencingGroupInternalId],
        ] = {}
        for row in rows:
            key = ProjectSeqTypeTechnologyKey(
                row['project'],
                row['sequencing_type'],
                row['sequencing_technology'],
            )
            if value_field == 'sequencing_group_ids':
                parsed_rows[key] = [int(sgid) for sgid in row[value_field].split(',')]
            else:
                try:
                    parsed_rows[key] = row[value_field]
                except KeyError:
                    parsed_rows[key] = None
        return parsed_rows

    async def _get_sequencing_groups_by_analysis_ids(
        self, analysis_ids: list[AnalysisId]
    ) -> dict[AnalysisId, list[SequencingGroupInternalId]]:
        """Get sequencing groups for a list of analysis ids"""
        if not analysis_ids:
            return {}
        _query = """
SELECT
    analysis_id,
    GROUP_CONCAT(sequencing_group_id) as sequencing_group_ids
FROM analysis_sequencing_group
WHERE analysis_id IN :analysis_ids
GROUP BY analysis_id;
        """
        _query_results = await self.connection.fetch_all(
            _query,
            {
                'analysis_ids': analysis_ids,
            },
        )
        sequencing_groups_by_analysis_id: dict[
            AnalysisId, list[SequencingGroupInternalId]
        ] = {}
        for row in _query_results:
            sequencing_groups_by_analysis_id[row['analysis_id']] = [
                int(sgid) for sgid in row['sequencing_group_ids'].split(',')
            ]

        return sequencing_groups_by_analysis_id

    async def get_analysis_sequencing_groups(
        self, grouped_analysis_rows: list[AnalysisRow]
    ):
        """
        Get the analysis IDs from the group analysis rows, which is a list of analysis record dicts
        """
        analyses_to_query_sequencing_groups: list[AnalysisId] = []
        for row in grouped_analysis_rows:
            analyses_to_query_sequencing_groups.append(row.id)

        return await self._get_sequencing_groups_by_analysis_ids(
            analyses_to_query_sequencing_groups
        )

    def get_report_url(
        self,
        project_name: str,
        sequencing_group_id: SequencingGroupInternalId,
        output: str,
        stage: str,
    ):
        """Converts an analysis output gs path to a web report link"""
        sg_id = sequencing_group_id_format(sequencing_group_id)
        if 'main-web' in output:
            url_base = 'https://main-web.populationgenomics.org.au'
        else:
            url_base = 'https://test-web.populationgenomics.org.au'

        if stage == 'Stripy':
            return f'{url_base}/{project_name}/stripy/{sg_id}.stripy.html'
        if stage == 'MitoReport':
            return f'{url_base}/{project_name}/mito/mitoreport-{sg_id}/index.html'
        return None

    def get_sg_web_report_links(
        self,
        sequencing_group_stripy_reports: dict[ProjectSeqGroupKey, StripyReportRow],
        sequencing_group_mito_reports: dict[ProjectSeqGroupKey, AnalysisRow],
        project: Project,
        sequencing_group_id: SequencingGroupInternalId,
    ):
        """
        Get the web report links for a sequencing group
        """
        report_links: dict[str, dict[str, Any]] = {}
        report_key = ProjectSeqGroupKey(project.id, sequencing_group_id)

        if stripy_report := sequencing_group_stripy_reports.get(report_key):
            report_links['stripy'] = {
                'url': self.get_report_url(
                    project.name, sequencing_group_id, stripy_report.output, 'Stripy'
                ),
                'outliers_detected': stripy_report.outliers_detected,
                'outlier_loci': (
                    json.loads(stripy_report.outlier_loci)
                    if stripy_report.outlier_loci
                    else None
                ),
                'timestamp_completed': stripy_report.timestamp_completed.isoformat()
                if stripy_report.timestamp_completed
                else None,
            }

        if mito_report := sequencing_group_mito_reports.get(report_key):
            report_links['mito'] = {
                'url': self.get_report_url(
                    project.name, sequencing_group_id, mito_report.output, 'MitoReport'
                ),
                'timestamp_completed': mito_report.timestamp_completed.isoformat()
                if mito_report.timestamp_completed
                else None,
            }

        return report_links

    def get_cram_record(self, cram_row: AnalysisRow | None):
        """Get the CRAM record for a sequencing group"""
        return {
            'id': cram_row.id if cram_row else None,
            'output': cram_row.output if cram_row else None,
            'timestamp_completed': cram_row.timestamp_completed.strftime('%d-%m-%y')
            if cram_row
            else None,
        }

    def get_analysis_stats_internal_from_record(
        self,
        analysis_row: AnalysisRow | None,
        analysis_sequencing_groups: dict[AnalysisId, list[SequencingGroupInternalId]],
    ) -> AnalysisStatsInternal | None:
        """Transforms an analysis row record into an AnalysisStatsInternal object"""
        if not analysis_row:
            return None
        return AnalysisStatsInternal(
            id=analysis_row.id,
            name=analysis_row.output,
            sg_count=len(analysis_sequencing_groups.get(analysis_row.id, [])),
            timestamp=analysis_row.timestamp_completed,
        )

    def get_insights_summary_internal_row(
        self,
        summary_row_key: ProjectSeqTypeTechnologyKey,
        project: Project,
        total_families: int,
        total_participants: int,
        total_samples: int,
        total_sequencing_groups: int,
        crams: list[SequencingGroupInternalId],
        analysis_sequencing_groups: dict[AnalysisId, list[SequencingGroupInternalId]],
        latest_annotate_dataset_analysis: AnalysisRow | None,
        latest_snv_es_index_analysis: AnalysisRow | None,
        latest_sv_es_index_analysis: AnalysisRow | None,
    ) -> ProjectInsightsSummaryInternal:
        """Returns a ProjectInsightsSummaryInternal object from the given data"""
        latest_annotate_dataset = self.get_analysis_stats_internal_from_record(
            latest_annotate_dataset_analysis, analysis_sequencing_groups
        )
        latest_snv_es_index = self.get_analysis_stats_internal_from_record(
            latest_snv_es_index_analysis, analysis_sequencing_groups
        )
        latest_sv_es_index = self.get_analysis_stats_internal_from_record(
            latest_sv_es_index_analysis, analysis_sequencing_groups
        )

        return ProjectInsightsSummaryInternal(
            project=summary_row_key.project,
            dataset=project.name,
            sequencing_type=summary_row_key.sequencing_type,
            sequencing_technology=summary_row_key.sequencing_technology,
            total_families=total_families,
            total_participants=total_participants,
            total_samples=total_samples,
            total_sequencing_groups=total_sequencing_groups,
            total_crams=len(set(crams)),
            latest_annotate_dataset=latest_annotate_dataset,
            latest_snv_es_index=latest_snv_es_index,
            latest_sv_es_index=latest_sv_es_index,
        )

    def get_insights_details_internal_row(
        self,
        project: Project,
        sequencing_type: SequencingType,
        sequencing_platform: SequencingPlatform,
        sequencing_technology: SequencingTechnology,
        sequencing_group_details: SequencingGroupDetailRow,
        sequencing_group_cram: AnalysisRow,
        analysis_sequencing_groups: dict[AnalysisId, list[SequencingGroupInternalId]],
        latest_annotate_dataset_id: AnalysisId | None,
        latest_snv_es_index_id: AnalysisId | None,
        latest_sv_es_index_id: AnalysisId | None,
        stripy_reports: dict[ProjectSeqGroupKey, StripyReportRow],
        mito_reports: dict[ProjectSeqGroupKey, AnalysisRow],
    ) -> ProjectInsightsDetailsInternal:
        """Returns a ProjectInsightsDetailsInternal object from the given data"""
        web_reports = self.get_sg_web_report_links(
            stripy_reports,
            mito_reports,
            project,
            sequencing_group_details.sequencing_group_id,
        )
        sgs_in_latest_annotate_dataset = analysis_sequencing_groups.get(
            latest_annotate_dataset_id, []
        )
        sgs_in_latest_snv_es_index = analysis_sequencing_groups.get(
            latest_snv_es_index_id, []
        )
        sgs_in_latest_sv_es_index = analysis_sequencing_groups.get(
            latest_sv_es_index_id, []
        )
        sg_cram = self.get_cram_record(sequencing_group_cram)

        sample_ext_ids = self.convert_to_external_ids(
            sequencing_group_details.sample_external_ids
        )
        return ProjectInsightsDetailsInternal(
            project=project.id,
            dataset=project.name,
            sequencing_type=sequencing_type,
            sequencing_platform=sequencing_technology,
            sequencing_technology=sequencing_platform,
            sample_type=sequencing_group_details.sample_type,
            family_id=sequencing_group_details.family_id,
            family_ext_id=sequencing_group_details.family_external_id,
            participant_id=sequencing_group_details.participant_id,
            participant_ext_id=sequencing_group_details.participant_external_id,
            sample_id=sequencing_group_details.sample_id,
            sample_ext_ids=sample_ext_ids,
            sequencing_group_id=sequencing_group_details.sequencing_group_id,
            cram=sg_cram,
            in_latest_annotate_dataset=sequencing_group_details.sequencing_group_id
            in sgs_in_latest_annotate_dataset,
            in_latest_snv_es_index=sequencing_group_details.sequencing_group_id
            in sgs_in_latest_snv_es_index,
            in_latest_sv_es_index=sequencing_group_details.sequencing_group_id
            in sgs_in_latest_sv_es_index,
            web_reports=web_reports,
        )

    # Project Insights Summary queries
    async def _total_families_by_project_id_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[SequencingType]
    ) -> dict[ProjectSeqTypeTechnologyKey, int]:
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
                'projects': project_ids,
                'sequencing_types': sequencing_types,
            },
        )
        return self.parse_project_seqtype_technology_keyed_rows(
            _query_results, 'num_families'
        )

    async def _total_participants_by_project_id_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[SequencingType]
    ) -> dict[ProjectSeqTypeTechnologyKey, int]:
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
                'projects': project_ids,
                'sequencing_types': sequencing_types,
            },
        )
        return self.parse_project_seqtype_technology_keyed_rows(
            _query_results, 'num_participants'
        )

    async def _total_samples_by_project_id_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[SequencingType]
    ) -> dict[ProjectSeqTypeTechnologyKey, int]:
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
                'projects': project_ids,
                'sequencing_types': sequencing_types,
            },
        )
        return self.parse_project_seqtype_technology_keyed_rows(
            _query_results, 'num_samples'
        )

    async def _total_sequencing_groups_by_project_id_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[SequencingType]
    ) -> dict[ProjectSeqTypeTechnologyKey, int]:
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
                'projects': project_ids,
                'sequencing_types': sequencing_types,
            },
        )
        return self.parse_project_seqtype_technology_keyed_rows(
            _query_results, 'num_sgs'
        )

    async def _crams_by_project_id_and_seq_fields(
        self,
        project_ids: list[ProjectId],
        sequencing_types: list[SequencingType],
    ) -> dict[ProjectSeqTypeTechnologyKey, list[SequencingGroupInternalId]]:
        _query = """
SELECT
    a.project,
    sg.type as sequencing_type,
    sg.technology as sequencing_technology,
    GROUP_CONCAT(DISTINCT asg.sequencing_group_id) as sequencing_group_ids
FROM
    analysis a
    LEFT JOIN analysis_sequencing_group asg ON a.id = asg.analysis_id
    LEFT JOIN sequencing_group sg ON sg.id = asg.sequencing_group_id
WHERE
    a.project IN :projects
    AND sg.type IN :sequencing_types
    AND a.type = 'CRAM'
    AND a.status = 'COMPLETED'
GROUP BY
    a.project,
    sg.type,
    sg.technology;
        """

        _query_results = await self.connection.fetch_all(
            _query,
            {
                'projects': project_ids,
                'sequencing_types': sequencing_types,
            },
        )
        return self.parse_project_seqtype_technology_keyed_rows(
            _query_results, 'sequencing_group_ids'
        )

    async def _sg_crams_by_project_id_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[
        ProjectSeqTypeTechnologyKey, dict[SequencingGroupInternalId, AnalysisRow]
    ]:
        _query = """
SELECT
    a.project,
    a.id as analysis_id,
    sg.id as sequencing_group_id,
    sg.type as sequencing_type,
    sg.technology as sequencing_technology,
    COALESCE(a.output, ao.output, of.path) as output,
    a.timestamp_completed
FROM
    analysis a
    LEFT JOIN analysis_sequencing_group asg ON a.id = asg.analysis_id
    LEFT JOIN analysis_outputs ao ON a.id = ao.analysis_id
    LEFT JOIN output_file of ON ao.file_id = of.id
    LEFT JOIN sequencing_group sg ON sg.id = asg.sequencing_group_id
    INNER JOIN (
        SELECT
            asg.sequencing_group_id,
            MAX(a.timestamp_completed) as max_timestamp
        FROM analysis a
        INNER JOIN analysis_sequencing_group asg ON a.id = asg.analysis_id
        WHERE a.type='CRAM'
        AND a.status='COMPLETED'
        AND a.project IN :projects
        GROUP BY asg.sequencing_group_id
    ) max_timestamps ON asg.sequencing_group_id = max_timestamps.sequencing_group_id
    AND a.timestamp_completed = max_timestamps.max_timestamp
WHERE
    a.project IN :projects
    AND sg.type IN :sequencing_types
    AND a.type = 'CRAM'
    AND a.status = 'COMPLETED';
        """
        _query_results = await self.connection.fetch_all(
            _query,
            {
                'projects': project_ids,
                'sequencing_types': sequencing_types,
            },
        )

        cram_timestamps_by_project_id_and_seq_fields: dict[
            ProjectSeqTypeTechnologyKey, dict[SequencingGroupInternalId, AnalysisRow]
        ] = {}
        for row in _query_results:
            key = ProjectSeqTypeTechnologyKey(
                row['project'],
                row['sequencing_type'],
                row['sequencing_technology'],
            )
            sg_id = row['sequencing_group_id']
            cram_row = AnalysisRow(
                id=row['analysis_id'],
                output=row['output'],
                timestamp_completed=row['timestamp_completed'],
            )
            if key not in cram_timestamps_by_project_id_and_seq_fields:
                cram_timestamps_by_project_id_and_seq_fields[key] = {}
            cram_timestamps_by_project_id_and_seq_fields[key][sg_id] = cram_row
        return cram_timestamps_by_project_id_and_seq_fields

    async def _latest_annotate_dataset_by_project_id_and_seq_type(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[ProjectSeqTypeKey, AnalysisRow]:
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
WHERE
    a.type = 'CUSTOM'
    AND a.status = 'COMPLETED'
    AND a.project IN :projects
    AND JSON_UNQUOTE(JSON_EXTRACT(a.meta, '$.sequencing_type')) IN :sequencing_types
    AND JSON_EXTRACT(a.meta, '$.stage') = 'AnnotateDataset';
    -- JSON_UNQUOTE is necessary to compare JSON values with IN operator
        """
        _query_results = await self.connection.fetch_all(
            _query,
            {
                'projects': project_ids,
                'sequencing_types': sequencing_types,
            },
        )
        latest_annotate_dataset_by_project_id_and_seq_type: dict[
            ProjectSeqTypeKey, AnalysisRow
        ] = {}
        for row in _query_results:
            key = ProjectSeqTypeKey(row['project'], row['sequencing_type'])
            latest_annotate_dataset_by_project_id_and_seq_type[key] = (
                self.get_analysis_row(row)
            )
        return latest_annotate_dataset_by_project_id_and_seq_type

    async def _latest_es_indices_by_project_id_and_seq_type_and_stage(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[ProjectSeqTypeStageKey, AnalysisRow]:
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
    AND status='COMPLETED'
    GROUP BY project, JSON_EXTRACT(meta, '$.sequencing_type'), JSON_EXTRACT(meta, '$.stage')
) max_timestamps ON a.project = max_timestamps.project
AND a.timestamp_completed = max_timestamps.max_timestamp
AND JSON_UNQUOTE(JSON_EXTRACT(a.meta, '$.sequencing_type')) = max_timestamps.sequencing_type
AND JSON_EXTRACT(a.meta, '$.stage') = max_timestamps.stage
WHERE
    a.project IN :projects
    AND JSON_UNQUOTE(JSON_EXTRACT(a.meta, '$.sequencing_type')) in :sequencing_types;
        """
        _query_results = await self.connection.fetch_all(
            _query,
            {
                'projects': project_ids,
                'sequencing_types': sequencing_types,
            },
        )
        latest_es_indices_by_project_id_and_seq_type_and_stage: dict[
            ProjectSeqTypeStageKey, AnalysisRow
        ] = {}
        for row in _query_results:
            key = ProjectSeqTypeStageKey(
                row['project'], row['sequencing_type'], row['stage']
            )
            latest_es_indices_by_project_id_and_seq_type_and_stage[key] = (
                self.get_analysis_row(row)
            )
        return latest_es_indices_by_project_id_and_seq_type_and_stage

    # Project Insights details queries
    async def _sequencing_group_details_by_project_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[ProjectSeqTypeTechnologyPlatformKey, list[SequencingGroupDetailRow]]:
        _query = """
SELECT
    f.project,
    sg.type as sequencing_type,
    sg.platform as sequencing_platform,
    sg.technology as sequencing_technology,
    s.type as sample_type,
    f.id as family_id,
    fext.external_id as family_external_id,
    fp.participant_id as participant_id,
    pext.external_id as participant_external_id,
    s.id as sample_id,
    sext.external_id as sample_external_ids,
    sg.id as sequencing_group_id
FROM
    family f
    LEFT JOIN family_participant fp ON f.id = fp.family_id
    LEFT JOIN family_external_id fext ON f.id = fext.family_id
    LEFT JOIN participant_external_id pext ON fp.participant_id = pext.participant_id
    LEFT JOIN sample s ON fp.participant_id = s.participant_id
    LEFT JOIN sample_external_id sext ON s.id = sext.sample_id
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
        _query_results = await self.connection.fetch_all(
            _query,
            {
                'projects': project_ids,
                'sequencing_types': sequencing_types,
            },
        )
        sequencing_group_details_by_project_id_and_seq_fields: dict[
            ProjectSeqTypeTechnologyPlatformKey, list[SequencingGroupDetailRow]
        ] = {}
        for row in _query_results:
            key = ProjectSeqTypeTechnologyPlatformKey(
                row['project'],
                row['sequencing_type'],
                row['sequencing_platform'],
                row['sequencing_technology'],
            )
            if key not in sequencing_group_details_by_project_id_and_seq_fields:
                sequencing_group_details_by_project_id_and_seq_fields[key] = []
            sequencing_group_details_by_project_id_and_seq_fields[key].append(
                SequencingGroupDetailRow(
                    family_id=row['family_id'],
                    family_external_id=row['family_external_id'],
                    participant_id=row['participant_id'],
                    participant_external_id=row['participant_external_id'],
                    sample_id=row['sample_id'],
                    sample_external_ids=row['sample_external_ids'],
                    sample_type=row['sample_type'],
                    sequencing_group_id=row['sequencing_group_id'],
                )
            )

        return sequencing_group_details_by_project_id_and_seq_fields

    async def _details_stripy_reports(
        self, project_ids: list[ProjectId]
    ) -> dict[ProjectSeqGroupKey, StripyReportRow]:
        """Get stripy web report links"""
        _query = """
SELECT
    a.project,
    a.id,
    coalesce(a.output, ao.output, of.path) as output,
    a.timestamp_completed,
    asg.sequencing_group_id,
    JSON_EXTRACT(a.meta, '$.outliers_detected') as outliers_detected,
    JSON_QUERY(a.meta, '$.outlier_loci') as outlier_loci
FROM analysis a
LEFT JOIN analysis_outputs ao on a.id=ao.analysis_id
LEFT JOIN output_file of on of.id = ao.file_id
LEFT JOIN analysis_sequencing_group asg on asg.analysis_id=a.id
INNER JOIN (
    SELECT
        asg.sequencing_group_id,
        MAX(a.id) as max_analysis_id
    FROM analysis a
    LEFT JOIN analysis_sequencing_group asg on asg.analysis_id=a.id
    WHERE type='web'
    AND status='COMPLETED'
    AND project IN :projects
    AND JSON_EXTRACT(meta, '$.stage') = 'Stripy'
    GROUP BY asg.sequencing_group_id
) latest_analysis ON a.id = latest_analysis.max_analysis_id;
        """
        _query_results = await self.connection.fetch_all(
            _query,
            {
                'projects': project_ids,
            },
        )
        stripy_reports: dict[ProjectSeqGroupKey, StripyReportRow] = {}
        for row in _query_results:
            key = ProjectSeqGroupKey(row['project'], row['sequencing_group_id'])
            stripy_reports[key] = StripyReportRow(
                id=row['id'],
                output=row['output'],
                outliers_detected=row['outliers_detected'],
                outlier_loci=row['outlier_loci'],
                timestamp_completed=row['timestamp_completed'],
            )

        return stripy_reports

    async def _details_mito_reports(
        self, project_ids: list[ProjectId]
    ) -> dict[ProjectSeqGroupKey, AnalysisRow]:
        """Get mito web report links"""
        _query = """
SELECT
    a.project,
    a.id,
    coalesce(a.output, ao.output, of.path) as output,
    a.timestamp_completed,
    asg.sequencing_group_id
FROM analysis a
LEFT JOIN analysis_outputs ao on a.id=ao.analysis_id
LEFT JOIN output_file of on of.id = ao.file_id
LEFT JOIN analysis_sequencing_group asg on asg.analysis_id=a.id
INNER JOIN (
    SELECT
        asg.sequencing_group_id,
        MAX(a.id) as max_analysis_id
    FROM analysis a
    LEFT JOIN analysis_sequencing_group asg on asg.analysis_id=a.id
    WHERE type='web'
    AND status='COMPLETED'
    AND project IN :projects
    AND JSON_EXTRACT(meta, '$.stage') = 'MitoReport'
    GROUP BY asg.sequencing_group_id
) latest_analysis ON a.id = latest_analysis.max_analysis_id;
        """
        _query_results = await self.connection.fetch_all(
            _query,
            {
                'projects': project_ids,
            },
        )
        mito_reports: dict[ProjectSeqGroupKey, AnalysisRow] = {}
        for row in _query_results:
            key = ProjectSeqGroupKey(row['project'], row['sequencing_group_id'])
            mito_reports[key] = self.get_analysis_row(row)

        return mito_reports

    def get_latest_grouped_analyses(
        self,
        project: Project,
        sequencing_type: SequencingType,
        sequencing_technology: SequencingTechnology,
        latest_annotate_dataset_by_project_id_and_seq_type: dict[
            ProjectSeqTypeKey, AnalysisRow
        ],
        latest_es_indices_by_project_id_and_seq_type_and_stage: dict[
            ProjectSeqTypeStageKey, AnalysisRow
        ],
    ):
        """Returns the latest grouped analyses for a project, sequencing type, and technology"""
        if sequencing_technology == 'short-read':
            latest_annotate_dataset_row = (
                latest_annotate_dataset_by_project_id_and_seq_type.get(
                    ProjectSeqTypeKey(project.id, sequencing_type)
                )
            )
            latest_snv_es_index_row = (
                latest_es_indices_by_project_id_and_seq_type_and_stage.get(
                    ProjectSeqTypeStageKey(project.id, sequencing_type, 'MtToEs')
                )
            )
            latest_sv_es_index_row = (
                latest_es_indices_by_project_id_and_seq_type_and_stage.get(
                    ProjectSeqTypeStageKey(
                        project.id,
                        sequencing_type,
                        SV_INDEX_SEQ_TYPE_STAGE_MAP.get(sequencing_type),
                    )
                )
            )
        else:
            latest_annotate_dataset_row = None
            latest_snv_es_index_row = None
            latest_sv_es_index_row = None
        return (
            latest_annotate_dataset_row,
            latest_snv_es_index_row,
            latest_sv_es_index_row,
        )

    # Main functions
    async def get_project_insights_summary(
        self, project_names: list[str], sequencing_types: list[str]
    ):
        """Combines the results of the above queries into a response"""
        projects = self._connection.get_and_check_access_to_projects_for_names(
            project_names=project_names, allowed_roles=ReadAccessRoles
        )
        project_ids: list[ProjectId] = [project.id for project in projects]

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
            (
                list(latest_annotate_dataset_by_project_id_and_seq_type.values())
                + list(latest_es_indices_by_project_id_and_seq_type_and_stage.values())
            )
        )

        sequencing_technologies = await SeqTechTable(self._connection).get()
        # Get all possible combinations of the projects, sequencing types, and sequencing technologies
        combinations = itertools.product(
            projects, sequencing_types, sequencing_technologies
        )

        response = []
        for project, seq_type, seq_tech in combinations:
            rowkey = ProjectSeqTypeTechnologyKey(project.id, seq_type, seq_tech)

            total_sequencing_groups = (
                total_sequencing_groups_by_project_id_and_seq_fields.get(rowkey, 0)
            )
            if total_sequencing_groups == 0:
                continue

            crams_in_project_with_sequencing_fields = (
                crams_by_project_id_and_seq_fields.get(rowkey, [])
            )
            (
                latest_annotate_dataset_row,
                latest_snv_es_index_row,
                latest_sv_es_index_row,
            ) = self.get_latest_grouped_analyses(
                project,
                seq_type,
                seq_tech,
                latest_annotate_dataset_by_project_id_and_seq_type,
                latest_es_indices_by_project_id_and_seq_type_and_stage,
            )

            total_families_by_project_id_and_seq_fields.setdefault(rowkey, 0)
            total_participants_by_project_id_and_seq_fields.setdefault(rowkey, 0)
            total_samples_by_project_id_and_seq_fields.setdefault(rowkey, 0)

            response.append(
                self.get_insights_summary_internal_row(
                    summary_row_key=rowkey,
                    project=project,
                    total_families=total_families_by_project_id_and_seq_fields[rowkey],
                    total_participants=total_participants_by_project_id_and_seq_fields[
                        rowkey
                    ],
                    total_samples=total_samples_by_project_id_and_seq_fields[rowkey],
                    total_sequencing_groups=total_sequencing_groups,
                    crams=crams_in_project_with_sequencing_fields,
                    analysis_sequencing_groups=analysis_sequencing_groups,
                    latest_annotate_dataset_analysis=latest_annotate_dataset_row,
                    latest_snv_es_index_analysis=latest_snv_es_index_row,
                    latest_sv_es_index_analysis=latest_sv_es_index_row,
                )
            )

        return response

    async def get_project_insights_details(
        self, project_names: list[str], sequencing_types: list[str]
    ):
        """Combines the results of the queries above into a response"""
        projects = self._connection.get_and_check_access_to_projects_for_names(
            project_names=project_names, allowed_roles=ReadAccessRoles
        )
        project_ids: list[ProjectId] = [project.id for project in projects]

        (
            sequencing_group_details_by_project_id_and_seq_fields,
            crams_by_project_id_and_seq_fields,
            latest_annotate_dataset_by_project_id_and_seq_type,
            latest_es_indices_by_project_id_and_seq_type_and_stage,
            sequencing_group_stripy_reports,
            sequencing_group_mito_reports,
        ) = await asyncio.gather(
            self._sequencing_group_details_by_project_and_seq_fields(
                project_ids, sequencing_types
            ),
            self._sg_crams_by_project_id_and_seq_fields(project_ids, sequencing_types),
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
            (
                list(latest_annotate_dataset_by_project_id_and_seq_type.values())
                + list(latest_es_indices_by_project_id_and_seq_type_and_stage.values())
            )
        )

        sequencing_platforms = await SeqPlatformTable(self._connection).get()
        sequencing_technologies = await SeqTechTable(self._connection).get()

        # Get all possible combinations of the projects, sequencing types, platforms, and technologies
        combinations = itertools.product(
            projects, sequencing_types, sequencing_platforms, sequencing_technologies
        )

        response = []
        for (
            project,
            seq_type,
            seq_platform,
            seq_tech,
        ) in combinations:
            details_rows: list[SequencingGroupDetailRow]
            if not (
                details_rows
                := sequencing_group_details_by_project_id_and_seq_fields.get(
                    (project.id, seq_type, seq_platform, seq_tech)
                )
            ):
                continue

            sequencing_groups_crams: dict[SequencingGroupInternalId, AnalysisRow] = (
                crams_by_project_id_and_seq_fields.get(
                    (project.id, seq_type, seq_tech), {}
                )
            )
            (
                latest_annotate_dataset_row,
                latest_snv_es_index_row,
                latest_sv_es_index_row,
            ) = self.get_latest_grouped_analyses(
                project,
                seq_type,
                seq_tech,
                latest_annotate_dataset_by_project_id_and_seq_type,
                latest_es_indices_by_project_id_and_seq_type_and_stage,
            )

            for details_row in details_rows:
                if not details_row:
                    continue
                sg_id = details_row.sequencing_group_id
                response.append(
                    self.get_insights_details_internal_row(
                        project=project,
                        sequencing_type=seq_type,
                        sequencing_platform=seq_platform,
                        sequencing_technology=seq_tech,
                        sequencing_group_details=details_row,
                        sequencing_group_cram=sequencing_groups_crams.get(sg_id),
                        analysis_sequencing_groups=analysis_sequencing_groups,
                        latest_annotate_dataset_id=(
                            latest_annotate_dataset_row.id
                            if latest_annotate_dataset_row
                            else None
                        ),
                        latest_snv_es_index_id=(
                            latest_snv_es_index_row.id
                            if latest_snv_es_index_row
                            else None
                        ),
                        latest_sv_es_index_id=(
                            latest_sv_es_index_row.id
                            if latest_sv_es_index_row
                            else None
                        ),
                        stripy_reports=sequencing_group_stripy_reports,
                        mito_reports=sequencing_group_mito_reports,
                    )
                )

        return response
