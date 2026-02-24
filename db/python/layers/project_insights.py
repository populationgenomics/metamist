# mypy: disable-error-code="attr-defined,arg-type,index,call-overload"
import asyncio
import itertools
import json
from typing import Any

from db.python.connect import Connection
from db.python.enum_tables import SequencingPlatformTable as SeqPlatformTable
from db.python.enum_tables import SequencingTechnologyTable as SeqTechTable
from db.python.layers.base import BaseLayer
from db.python.tables.project_insights import ProjectInsightsDb
from models.models import (
    AnalysisStatsInternal,
    ProjectInsightsDetailsInternal,
    ProjectInsightsSummaryInternal,
)
from models.models.project import Project, ProjectId, ReadAccessRoles
from models.models.sequencing_group import SequencingGroupInternalId
from models.utils.project_insights import (
    AnalysisId,
    AnalysisRow,
    ProjectSeqGroupKey,
    ProjectSeqTypeKey,
    ProjectSeqTypeStageKey,
    ProjectSeqTypeTechnologyKey,
    SequencingGroupDetailRow,
    SequencingPlatform,
    SequencingTechnology,
    SequencingType,
    StripyReportRow,
)
from models.utils.sequencing_group_id_format import sequencing_group_id_format


SV_INDEX_SEQ_TYPE_STAGE_MAP = {
    'genome': 'MtToEsSv',
    'exome': 'MtToEsCNV',
}


class ProjectInsightsLayer(BaseLayer):
    """Project Insights layer - business logic for the project insights dashboards"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.pidb = ProjectInsightsDb(connection=connection)

    async def get_project_insights_summary(
        self,
        project_names: list[str],
        sequencing_types: list[SequencingType],
    ) -> list[ProjectInsightsSummaryInternal]:
        """
        Get summary and analysis stats for a list of projects
        """
        return await self.collect_project_insights_summary(
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
        return await self.collect_project_insights_details(
            project_names=project_names, sequencing_types=sequencing_types
        )

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
    async def collect_project_insights_summary(
        self, project_names: list[str], sequencing_types: list[str]
    ):
        """Combines the results of the above queries into a response"""
        projects = self.connection.get_and_check_access_to_projects_for_names(
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
            self.pidb.total_families_by_project_id_and_seq_fields(
                project_ids, sequencing_types
            ),
            self.pidb.total_participants_by_project_id_and_seq_fields(
                project_ids, sequencing_types
            ),
            self.pidb.total_samples_by_project_id_and_seq_fields(
                project_ids, sequencing_types
            ),
            self.pidb.total_sequencing_groups_by_project_id_and_seq_fields(
                project_ids, sequencing_types
            ),
            self.pidb.crams_by_project_id_and_seq_fields(project_ids, sequencing_types),
            self.pidb.latest_annotate_dataset_by_project_id_and_seq_type(
                project_ids, sequencing_types
            ),
            self.pidb.latest_es_indices_by_project_id_and_seq_type_and_stage(
                project_ids,
                sequencing_types,
            ),
        )

        # Get the sequencing groups for each of the analyses in the grouped analyses rows
        analysis_sequencing_groups = await self.get_analysis_sequencing_groups(
            list(latest_annotate_dataset_by_project_id_and_seq_type.values())
            + list(latest_es_indices_by_project_id_and_seq_type_and_stage.values())
        )

        sequencing_technologies = await SeqTechTable(self.connection).get()
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

    async def collect_project_insights_details(
        self, project_names: list[str], sequencing_types: list[str]
    ):
        """Combines the results of the queries above into a response"""
        projects = self.connection.get_and_check_access_to_projects_for_names(
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
            self.pidb.sequencing_group_details_by_project_and_seq_fields(
                project_ids, sequencing_types
            ),
            self.pidb.sg_crams_by_project_id_and_seq_fields(
                project_ids, sequencing_types
            ),
            self.pidb.latest_annotate_dataset_by_project_id_and_seq_type(
                project_ids, sequencing_types
            ),
            self.pidb.latest_es_indices_by_project_id_and_seq_type_and_stage(
                project_ids, sequencing_types
            ),
            self.pidb.details_stripy_reports(project_ids),
            self.pidb.details_mito_reports(project_ids),
        )
        # Get the sequencing groups for each of the analyses in the grouped analyses rows
        analysis_sequencing_groups = await self.get_analysis_sequencing_groups(
            list(latest_annotate_dataset_by_project_id_and_seq_type.values())
            + list(latest_es_indices_by_project_id_and_seq_type_and_stage.values())
        )

        sequencing_platforms = await SeqPlatformTable(self.connection).get()
        sequencing_technologies = await SeqTechTable(self.connection).get()

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

    async def get_analysis_sequencing_groups(
        self, grouped_analysis_rows: list[AnalysisRow]
    ):
        """
        Get the analysis IDs from the group analysis rows, which is a list of analysis record dicts
        """
        analyses_to_query_sequencing_groups: list[AnalysisId] = []
        for row in grouped_analysis_rows:
            analyses_to_query_sequencing_groups.append(row.id)
        analyses_to_query_sequencing_groups = [4, 5]
        return await self.pidb.get_sequencing_groups_by_analysis_ids(
            analyses_to_query_sequencing_groups
        )

    def get_insights_summary_internal_row(  # noqa: PLR0913
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

    def get_insights_details_internal_row(  # noqa: PLR0913
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

    def get_cram_record(self, cram_row: AnalysisRow | None):
        """Get the CRAM record for a sequencing group"""
        return {
            'id': cram_row.id if cram_row else None,
            'output': cram_row.output if cram_row else None,
            'timestamp_completed': cram_row.timestamp_completed.strftime('%d-%m-%y')
            if cram_row
            else None,
        }

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

    def convert_to_external_ids(self, external_ids_value: str | list[str]) -> list[str]:
        """Converts a string or list of strings to a list of strings"""
        if isinstance(external_ids_value, str):
            return [external_ids_value]
        return external_ids_value
