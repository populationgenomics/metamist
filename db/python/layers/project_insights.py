# pylint: disable=too-many-locals, too-many-instance-attributes, too-many-lines
import asyncio

from db.python.layers.base import BaseLayer
from db.python.tables.base import DbBase
from db.python.tables.project import ProjectPermissionsTable
from models.models import (
    AnalysisStats,
    ProjectInsightsDetailsInternal,
    ProjectInsightsStatsInternal,
)
from models.utils.sample_id_format import sample_id_format
from models.utils.sequencing_group_id_format import sequencing_group_id_format


class ProjectInsightsLayer(BaseLayer):
    """Project Insights layer - business logic for the project insights dashboard"""

    async def get_project_insights_stats(
        self,
        projects: list[int],
        sequencing_types: list[str],
    ) -> list[ProjectInsightsStatsInternal]:
        """
        Get summary and analysis stats for a list of projects
        """
        pidb = ProjectsInsightsDb(self.connection)
        return await pidb.get_project_insights_stats(
            projects=projects, sequencing_types=sequencing_types
        )

    async def get_project_insights_details(
        self,
        projects: list[int],
        sequencing_types: list[str],
    ) -> list[ProjectInsightsDetailsInternal]:
        """
        Get details for a list of projects
        """
        pidb = ProjectsInsightsDb(self.connection)
        return await pidb.get_project_insights_details(
            projects=projects, sequencing_types=sequencing_types
        )


class ProjectsInsightsDb(DbBase):
    """Db layer for project insights stats and details routes"""

    async def _details_families_query(
        self, projects: list[int], sequencing_types: list[str]
    ):
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
    s.external_id as sample_external_id,
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
                'projects': projects,
                'sequencing_types': sequencing_types,
            },
        )

        return families

    async def _stats_families_query(
        self, projects: list[int], sequencing_types: list[str]
    ):
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
        total_families_by_project_id_and_seq_type = await self.connection.fetch_all(
            _query,
            {
                'projects': projects,
                'sequencing_types': sequencing_types,
            },
        )
        return total_families_by_project_id_and_seq_type

    async def _stats_participants_query(
        self, projects: list[int], sequencing_types: list[str]
    ):
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
        total_participants_by_project_id_and_seq_type = await self.connection.fetch_all(
            _query,
            {
                'projects': projects,
                'sequencing_types': sequencing_types,
            },
        )
        return total_participants_by_project_id_and_seq_type

    async def _stats_samples_query(
        self, projects: list[int], sequencing_types: list[str]
    ):
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
        total_samples_by_project_id_and_seq_type = await self.connection.fetch_all(
            _query,
            {
                'projects': projects,
                'sequencing_types': sequencing_types,
            },
        )
        return total_samples_by_project_id_and_seq_type

    async def _stats_sequencing_groups_query(
        self, projects: list[int], sequencing_types: list[str]
    ):
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
        total_sequencing_groups_by_project_id_and_seq_type = (
            await self.connection.fetch_all(
                _query,
                {
                    'projects': projects,
                    'sequencing_types': sequencing_types,
                },
            )
        )
        return total_sequencing_groups_by_project_id_and_seq_type

    async def _stats_crams_query(
        self, projects: list[int], sequencing_types: list[str]
    ):
        _query = """
SELECT
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
        total_crams_by_project_id_and_seq_type = await self.connection.fetch_all(
            _query,
            {
                'projects': projects,
                'sequencing_types': sequencing_types,
            },
        )
        return total_crams_by_project_id_and_seq_type

    async def _stats_es_indexes_query(
        self, projects: list[int], sequencing_types: list[str]
    ):
        _query = """
SELECT
    a.project,
    a.id,
    JSON_EXTRACT(a.meta, '$.sequencing_type') as sequencing_type,
    JSON_EXTRACT(a.meta, '$.stage') as stage,
    a.output,
    a.timestamp_completed,
FROM analysis a
INNER JOIN (
    SELECT
        project,
        MAX(a.timestamp_completed) as max_timestamp,
        JSON_EXTRACT(meta, '$.sequencing_type') as sequencing_type,
        JSON_EXTRACT(meta, '$.stage') as stage,
    FROM analysis
    GROUP BY project, JSON_EXTRACT(meta, '$.sequencing_type'), JSON_EXTRACT(meta, '$.stage')
) max_timestamps ON a.project = max_timestamps.project
AND a.timestamp_completed = max_timestamps.max_timestamp
AND JSON_EXTRACT(a.meta, '$.sequencing_type') = max_timestamps.sequencing_type
AND JSON_EXTRACT(a.meta, '$.stage') = max_timestamps.stage
AND a.project in :projects
        """
        latest_es_indexes_by_project_id_and_seq_type_and_stage = await self.connection.fetch_all(
            _query,
            {
                'projects': projects,
                'sequencing_types': sequencing_types,
            },
        )
        return latest_es_indexes_by_project_id_and_seq_type_and_stage

    async def _stats_annotate_dataset_query(
        self, projects: list[int], sequencing_types: list[str]
    ):
        _query = """
SELECT
    a.project,
    a.sequencing_type,
    a.id,
    a.output,
    a.timestamp_completed
FROM
    (
        SELECT
            a.project,
            a.id,
            a.output,
            a.timestamp_completed,
            sg.type as sequencing_type,
            ROW_NUMBER() OVER (
                PARTITION BY a.project,
                sg.type
                ORDER BY
                    a.timestamp_completed DESC
            ) AS rn
        FROM
            analysis a
            LEFT JOIN analysis_sequencing_group asg ON a.id = asg.analysis_id
            LEFT JOIN sequencing_group sg ON sg.id = asg.sequencing_group_id
        WHERE
            a.status = 'COMPLETED'
            AND a.type = 'CUSTOM'
            AND a.meta LIKE '%AnnotateDataset%'
            AND sg.type IN :sequencing_types
    ) a
WHERE
    a.rn = 1
AND
    a.project IN :projects
        """
        latest_annotate_dataset_by_project_id_and_seq_type = (
            await self.connection.fetch_all(
                _query,
                {
                    'projects': projects,
                    'sequencing_types': sequencing_types,
                },
            )
        )
        return latest_annotate_dataset_by_project_id_and_seq_type

    async def _details_sequencing_groups_report_links(
        self, projects: list[int], sequencing_types: list[str]
    ):
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

        sequencing_group_reports = await self.connection.fetch_all(
            _query,
            {
                'projects': projects,
                'sequencing_types': sequencing_types,
            },
        )
        return sequencing_group_reports

    async def _get_sequencing_groups_from_analysis_id(self, analysis_id: int):
        """Get sequencing groups from an analysis id"""
        _query = """
SELECT
    asg.sequencing_group_id
FROM
    analysis_sequencing_group asg
WHERE
    asg.analysis_id = :analysis_id
        """
        analysis_sequencing_groups = await self.connection.fetch_all(
            _query,
            {
                'analysis_id': analysis_id,
            },
        )
        return analysis_sequencing_groups

    @staticmethod
    def get_val_for_project_and_sequencing_type(project, sequencing_type, field, rows):
        """
        Filter returned records for a specific project and sequencing type and
        return the value of a specific field
        """
        if not rows:
            return 0
        for row in rows:
            if row['project'] == project and row['sequencing_type'] == sequencing_type:
                return row[field]
        return 0

    @staticmethod
    def get_val_for_project_and_sequencing_type_and_sg_id(
        project, sequencing_type, sg_id, field, rows
    ):
        """
        Filter returned records for a specific project, sequencing type and
        sequencing group id and return the value of a specific field
        """
        if not rows:
            return 0
        for row in rows:
            if (
                row['project'] == project
                and row['sequencing_type'] == sequencing_type
                and row['sequencing_group_id'] == sg_id
            ):
                return row[field]
        return 0

    @staticmethod
    def get_val_for_project_and_sequencing_type_and_stage(
        project, sequencing_type, stage, field, rows
    ):
        """
        Filter returned records for a specific project, sequencing type and
        stage and return the value of a specific field
        """
        if not rows:
            return 0
        for row in rows:
            if (
                row['project'] == project
                and row['sequencing_type'] == sequencing_type
                and row['stage'] == stage
            ):
                return row[field]
        return 0

    async def get_project_insights_stats(
        self, projects: list[int], sequencing_types: list[str]
    ):
        """Combines the results of the above insights stats queries into a response"""

        ptable = ProjectPermissionsTable(self._connection)
        await ptable.check_access_to_project_ids(
            user=self.author, project_ids=projects, readonly=True
        )

        (
            families,
            participants,
            samples,
            sequencing_groups,
            crams,
            latest_annotate_dataset,
            latest_es_indexes,
        ) = await asyncio.gather(
            self._stats_families_query(projects, sequencing_types),
            self._stats_participants_query(projects, sequencing_types),
            self._stats_samples_query(projects, sequencing_types),
            self._stats_sequencing_groups_query(
                projects, sequencing_types
            ),
            self._stats_crams_query(projects, sequencing_types),
            self._stats_annotate_dataset_query(
                projects, sequencing_types
            ),
            self._stats_es_indexes_query(projects, sequencing_types,),
        )

        response = []
        for pid in projects:
            project = await ptable.get_and_check_access_to_project_for_id(self.author, pid, readonly=True)
            for sequencing_type in sequencing_types:
                crams_in_project_and_sequencing_type = [
                    cram['sequencing_group_id']
                    for cram in crams
                    if cram['project'] == project.id
                    and cram['sequencing_type'] == sequencing_type
                ]
                latest_annotate_dataset_id = (
                    self.get_val_for_project_and_sequencing_type(
                        project.id, sequencing_type, 'id', latest_annotate_dataset
                    )
                )
                latest_snv_index_id = self.get_val_for_project_and_sequencing_type_and_stage(
                    project.id, sequencing_type, 'MtToEs', 'id', latest_es_indexes
                )
                latest_sv_index_id = self.get_val_for_project_and_sequencing_type_and_stage(
                    project.id, sequencing_type, 'MtToEsSv', 'id', latest_es_indexes
                )
                latest_gcnv_index_id = self.get_val_for_project_and_sequencing_type_and_stage(
                    project.id, sequencing_type, 'MtToEsCNV', 'id', latest_es_indexes
                )
                response.append(
                    ProjectInsightsStatsInternal(
                        project=project.id,
                        dataset=project.name,
                        sequencing_type=sequencing_type,
                        total_families=self.get_val_for_project_and_sequencing_type(
                            project.id, sequencing_type, 'num_families', families
                        ),
                        total_participants=self.get_val_for_project_and_sequencing_type(
                            project.id,
                            sequencing_type,
                            'num_participants',
                            participants,
                        ),
                        total_samples=self.get_val_for_project_and_sequencing_type(
                            project.id, sequencing_type, 'num_samples', samples
                        ),
                        total_sequencing_groups=self.get_val_for_project_and_sequencing_type(
                            project.id, sequencing_type, 'num_sgs', sequencing_groups
                        ),
                        total_crams=len(set(crams_in_project_and_sequencing_type)),
                        latest_annotate_dataset=AnalysisStats(
                            id=latest_annotate_dataset_id,
                            sg_count=len(
                                await self._get_sequencing_groups_from_analysis_id(
                                    latest_annotate_dataset_id
                                )
                            )
                        ),
                        latest_snv_es_index=AnalysisStats(
                            id=latest_snv_index_id,
                            sg_count=len(
                                await self._get_sequencing_groups_from_analysis_id(
                                    latest_snv_index_id
                                )
                            ),
                        ),
                        latest_sv_es_index=AnalysisStats(
                            id=latest_sv_index_id,
                            sg_count=len(
                                await self._get_sequencing_groups_from_analysis_id(
                                    latest_sv_index_id
                                )
                            ),
                        ),
                        latest_gcnv_es_index=AnalysisStats(
                            id=latest_gcnv_index_id,
                            sg_count=len(
                                await self._get_sequencing_groups_from_analysis_id(
                                    latest_gcnv_index_id
                                )
                            ),
                        ),
                    )
                )

        return response

    async def get_project_insights_details(
        self, projects: list[int], sequencing_types: list[str]
    ):
        """Combines the results of the above insights details queries into a response"""
        ptable = ProjectPermissionsTable(self._connection)
        await ptable.check_access_to_project_ids(
            user=self.author, project_ids=projects, readonly=True
        )

        (
            families,
            crams,
            latest_annotate_dataset,
            latest_es_indexes,
            sequencing_group_report_links,
        ) = await asyncio.gather(
            self._details_families_query(projects, sequencing_types),
            self._stats_crams_query(projects, sequencing_types),
            self._stats_annotate_dataset_query(
                projects, sequencing_types
            ),
            self._stats_es_indexes_query(projects, sequencing_types),
            self._details_sequencing_groups_report_links(
                projects, sequencing_types
            ),
        )

        response = []
        for pid in projects:
            project = await ptable.get_and_check_access_to_project_for_id(self.author, pid, readonly=True)
            for sequencing_type in sequencing_types:
                sequencing_groups_with_crams = [
                    cram['sequencing_group_id'] for cram in crams
                ]
                latest_annotate_dataset_id = (
                    self.get_val_for_project_and_sequencing_type(
                        project.id, sequencing_type, 'id', latest_annotate_dataset
                    )
                )
                sequencing_groups_in_latest_annotate_dataset = [
                    row['sequencing_group_id']
                    for row in await self._get_sequencing_groups_from_analysis_id(
                        latest_annotate_dataset_id
                    )
                ]
                latest_snv_es_index_id = self.get_val_for_project_and_sequencing_type_and_stage(
                    project.id, sequencing_type, 'MtToEs', 'id', latest_es_indexes
                )
                sequencing_groups_in_latest_snv_es_index = [
                    row['sequencing_group_id']
                    for row in await self._get_sequencing_groups_from_analysis_id(
                        latest_snv_es_index_id
                    )
                ]
                latest_sv_es_index_id = self.get_val_for_project_and_sequencing_type_and_stage(
                    project.id, sequencing_type, 'MtToEsSv', 'id', latest_es_indexes
                )
                sequencing_groups_in_latest_sv_es_index = [
                    row['sequencing_group_id']
                    for row in await self._get_sequencing_groups_from_analysis_id(
                        latest_sv_es_index_id
                    )
                ]
                latest_gcnv_es_index_id = self.get_val_for_project_and_sequencing_type_and_stage(
                    project.id, sequencing_type, 'MtToEsCNV', 'id', latest_es_indexes
                )
                sequencing_groups_in_latest_gcnv_es_index = [
                    row['sequencing_group_id']
                    for row in await self._get_sequencing_groups_from_analysis_id(
                        latest_gcnv_es_index_id
                    )
                ]
                for family_row in families:
                    sequencing_group_id = sequencing_group_id_format(
                        family_row['sequencing_group_id']
                    )
                    report_links = {}
                    stripy = self.get_val_for_project_and_sequencing_type_and_sg_id(
                        project.id,
                        sequencing_type,
                        family_row['sequencing_group_id'],
                        'stripy',
                        sequencing_group_report_links,
                    )
                    if stripy:
                        report_links['stripy'] = (
                            f'https://main-web.populationgenomics.org.au/'
                            f'{project.name}/stripy/'
                            f'{sequencing_group_id}.stripy.html'
                        )

                    mito = self.get_val_for_project_and_sequencing_type_and_sg_id(
                        project.id,
                        sequencing_type,
                        family_row['sequencing_group_id'],
                        'mito',
                        sequencing_group_report_links,
                    )
                    if mito:
                        report_links['mito'] = (
                            f'https://main-web.populationgenomics.org.au/'
                            f'{project.name}/mito/'
                            f'mitoreport-{sequencing_group_id}/index.html'
                        )

                    response.append(
                        ProjectInsightsDetailsInternal(
                            project=project.id,
                            dataset=project.name,
                            sequencing_type=sequencing_type,
                            sample_type=family_row['sample_type'],
                            family_id=family_row['family_id'],
                            family_ext_id=family_row['family_external_id'],
                            participant_id=family_row['participant_id'],
                            participant_ext_id=family_row['participant_external_id'],
                            sample_id=sample_id_format(family_row['sample_id']),
                            sample_ext_id=family_row['sample_external_id'],
                            sequencing_group_id=sequencing_group_id,
                            completed_cram=family_row['sequencing_group_id']
                            in sequencing_groups_with_crams,
                            in_latest_annotate_dataset=family_row['sequencing_group_id']
                            in sequencing_groups_in_latest_annotate_dataset,
                            in_latest_snv_es_index=family_row['sequencing_group_id']
                            in sequencing_groups_in_latest_snv_es_index,
                            in_latest_sv_es_index=family_row['sequencing_group_id']
                            in sequencing_groups_in_latest_sv_es_index,
                            in_latest_gcnv_es_index=family_row['sequencing_group_id']
                            in sequencing_groups_in_latest_gcnv_es_index,
                            sequencing_group_report_links=report_links,
                        )
                    )

        return response
