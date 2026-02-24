# mypy: disable-error-code="attr-defined,arg-type,index,call-overload"
from typing import Any

from db.python.tables.base import DbBase
from models.models.project import ProjectId
from models.models.sequencing_group import SequencingGroupInternalId
from models.utils.project_insights import (
    AnalysisId,
    AnalysisRow,
    ProjectSeqGroupKey,
    ProjectSeqTypeKey,
    ProjectSeqTypeStageKey,
    ProjectSeqTypeTechnologyKey,
    ProjectSeqTypeTechnologyPlatformKey,
    SequencingGroupDetailRow,
    SequencingType,
    StripyReportRow,
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

    async def get_sequencing_groups_by_analysis_ids(
        self, analysis_ids: list[AnalysisId]
    ) -> dict[AnalysisId, list[SequencingGroupInternalId]]:
        """Get sequencing groups for a list of analysis ids"""
        if not analysis_ids:
            return {}
        _query = t"""
        SELECT
            analysis_id,
            ARRAY_AGG(sequencing_group_id) as sequencing_group_ids
        FROM analysis_sequencing_group
        WHERE analysis_id = ANY({analysis_ids})
        GROUP BY analysis_id;
        """

        _query_results = await (
            await self.connection.pg_connection.execute(_query)
        ).fetchall()

        sequencing_groups_by_analysis_id: dict[
            AnalysisId, list[SequencingGroupInternalId]
        ] = {}
        for row in _query_results:
            sequencing_groups_by_analysis_id[row['analysis_id']] = row[
                'sequencing_group_ids'
            ]

        return sequencing_groups_by_analysis_id

    # Project Insights Summary queries
    async def total_families_by_project_id_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[SequencingType]
    ) -> dict[ProjectSeqTypeTechnologyKey, int]:

        sequencing_types_param = [st.lower() for st in sequencing_types]
        _query = t"""
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
            f.project = ANY({project_ids})
            AND sg.type = ANY({sequencing_types_param})
        GROUP BY
            f.project,
            sg.type,
            sg.technology;
        """

        _query_results = await (
            await self.connection.pg_connection.execute(_query)
        ).fetchall()
        return self.parse_project_seqtype_technology_keyed_rows(
            _query_results, 'num_families'
        )

    async def total_participants_by_project_id_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[SequencingType]
    ) -> dict[ProjectSeqTypeTechnologyKey, int]:

        sequencing_types_param = [st.lower() for st in sequencing_types]
        _query = t"""
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
            p.project = ANY({project_ids})
            AND sg.type = ANY({sequencing_types_param})
        GROUP BY
            p.project,
            sg.type,
            sg.technology;
        """

        _query_results = await (
            await self.connection.pg_connection.execute(_query)
        ).fetchall()
        return self.parse_project_seqtype_technology_keyed_rows(
            _query_results, 'num_participants'
        )

    async def total_samples_by_project_id_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[SequencingType]
    ) -> dict[ProjectSeqTypeTechnologyKey, int]:

        sequencing_types_param = [st.lower() for st in sequencing_types]
        _query = t"""
        SELECT
            s.project,
            sg.type as sequencing_type,
            sg.technology as sequencing_technology,
            COUNT(DISTINCT s.id) as num_samples
        FROM
            sample s
            LEFT JOIN sequencing_group sg on sg.sample_id = s.id
        WHERE
            s.project = ANY({project_ids})
            AND sg.type = ANY({sequencing_types_param})
        GROUP BY
            s.project,
            sg.type,
            sg.technology;
        """

        _query_results = await (
            await self.connection.pg_connection.execute(_query)
        ).fetchall()

        return self.parse_project_seqtype_technology_keyed_rows(
            _query_results, 'num_samples'
        )

    async def total_sequencing_groups_by_project_id_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[SequencingType]
    ) -> dict[ProjectSeqTypeTechnologyKey, int]:

        sequencing_types_param = [st.lower() for st in sequencing_types]
        _query = t"""
        SELECT
            s.project,
            sg.type as sequencing_type,
            sg.technology as sequencing_technology,
            COUNT(DISTINCT sg.id) as num_sgs
        FROM
            sequencing_group sg
            LEFT JOIN sample s on s.id = sg.sample_id
        WHERE
            s.project = ANY({project_ids})
            AND sg.type = ANY({sequencing_types_param})
        GROUP BY
            s.project,
            sg.type,
            sg.technology;
        """

        _query_results = await (
            await self.connection.pg_connection.execute(_query)
        ).fetchall()
        return self.parse_project_seqtype_technology_keyed_rows(
            _query_results, 'num_sgs'
        )

    async def crams_by_project_id_and_seq_fields(
        self,
        project_ids: list[ProjectId],
        sequencing_types: list[SequencingType],
    ) -> dict[ProjectSeqTypeTechnologyKey, list[SequencingGroupInternalId]]:

        sequencing_types_param = [st.lower() for st in sequencing_types]
        _query = t"""
        SELECT
            a.project,
            sg.type as sequencing_type,
            sg.technology as sequencing_technology,
            ARRAY_AGG(DISTINCT asg.sequencing_group_id) as sequencing_group_ids
        FROM
            analysis a
            LEFT JOIN analysis_sequencing_group asg ON a.id = asg.analysis_id
            LEFT JOIN sequencing_group sg ON sg.id = asg.sequencing_group_id
        WHERE
            a.project = ANY({project_ids})
            AND sg.type = ANY({sequencing_types_param})
            AND a.type = 'cram'
            AND a.status = 'completed'
        GROUP BY
            a.project,
            sg.type,
            sg.technology;
        """

        _query_results = await (
            await self.connection.pg_connection.execute(_query)
        ).fetchall()

        return self.parse_project_seqtype_technology_keyed_rows(
            _query_results, 'sequencing_group_ids'
        )

    async def sg_crams_by_project_id_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[
        ProjectSeqTypeTechnologyKey, dict[SequencingGroupInternalId, AnalysisRow]
    ]:

        sequencing_types_param = [st.lower() for st in sequencing_types]
        _query = t"""
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
                WHERE a.type = 'cram'
                AND a.status='completed'
                AND a.project = ANY({project_ids})
                GROUP BY asg.sequencing_group_id
            ) max_timestamps ON asg.sequencing_group_id = max_timestamps.sequencing_group_id
            AND a.timestamp_completed = max_timestamps.max_timestamp
        WHERE
            a.project = ANY({project_ids})
            AND sg.type = ANY({sequencing_types_param})
            AND a.type = 'cram'
            AND a.status = 'completed';
        """

        _query_results = await (
            await self.connection.pg_connection.execute(_query)
        ).fetchall()

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

    async def latest_annotate_dataset_by_project_id_and_seq_type(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[ProjectSeqTypeKey, AnalysisRow]:

        sequencing_types_param = [st.lower() for st in sequencing_types]
        _query = t"""
        SELECT
            a.project,
            a.meta ->> 'sequencing_type' as sequencing_type,
            a.id,
            a.output,
            a.timestamp_completed
        FROM analysis a
        INNER JOIN (
            SELECT
                project,
                MAX(timestamp_completed) as max_timestamp,
                LOWER(meta ->> 'sequencing_type') as sequencing_type
            FROM analysis
            WHERE
                status = 'completed'
                AND type = 'custom'
                AND LOWER(meta ->> 'stage') = 'annotatedataset'
                AND LOWER(meta ->> 'sequencing_type') = ANY({sequencing_types_param})
            GROUP BY project, LOWER(meta ->> 'sequencing_type')
        ) max_timestamps ON a.project = max_timestamps.project
        AND a.timestamp_completed = max_timestamps.max_timestamp
        AND LOWER(a.meta ->> 'sequencing_type') = max_timestamps.sequencing_type
        WHERE
            a.type = 'custom'
            AND a.status = 'completed'
            AND a.project = ANY({project_ids})
            AND LOWER(a.meta ->> 'sequencing_type') = ANY({sequencing_types_param})
            AND LOWER(a.meta ->> 'stage') = 'annotatedataset';
        """

        _query_results = await (
            await self.connection.pg_connection.execute(_query)
        ).fetchall()
        latest_annotate_dataset_by_project_id_and_seq_type: dict[
            ProjectSeqTypeKey, AnalysisRow
        ] = {}
        for row in _query_results:
            key = ProjectSeqTypeKey(row['project'], row['sequencing_type'])
            latest_annotate_dataset_by_project_id_and_seq_type[key] = (
                self.get_analysis_row(row)
            )
        return latest_annotate_dataset_by_project_id_and_seq_type

    async def latest_es_indices_by_project_id_and_seq_type_and_stage(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[ProjectSeqTypeStageKey, AnalysisRow]:

        sequencing_types_param = [st.lower() for st in sequencing_types]
        _query = t"""
        SELECT
            a.project,
            a.meta ->> 'sequencing_type' as sequencing_type,
            a.id,
            a.meta ->> 'stage' as stage,
            a.output,
            a.timestamp_completed
        FROM analysis a
        INNER JOIN (
            SELECT
                project,
                MAX(timestamp_completed) as max_timestamp,
                LOWER(meta ->> 'sequencing_type') as sequencing_type,
                LOWER(meta ->> 'stage') as stage
            FROM analysis
            WHERE type = 'es-index'
            AND status = 'completed'
            GROUP BY project, LOWER(meta ->> 'sequencing_type'), LOWER(meta ->> 'stage')
        ) max_timestamps ON a.project = max_timestamps.project
        AND a.timestamp_completed = max_timestamps.max_timestamp
        AND LOWER(a.meta ->> 'sequencing_type') = max_timestamps.sequencing_type
        AND LOWER(a.meta ->> 'stage') = max_timestamps.stage
        WHERE
            a.project = ANY({project_ids})
            AND LOWER(a.meta ->> 'sequencing_type') = ANY({sequencing_types_param});
        """

        _query_results = await (
            await self.connection.pg_connection.execute(_query)
        ).fetchall()
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
    async def sequencing_group_details_by_project_and_seq_fields(
        self, project_ids: list[ProjectId], sequencing_types: list[str]
    ) -> dict[ProjectSeqTypeTechnologyPlatformKey, list[SequencingGroupDetailRow]]:

        sequencing_types_param = [st.lower() for st in sequencing_types]
        _query = t"""
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
            f.project = ANY({project_ids})
            AND sg.type = ANY({sequencing_types_param})
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

        _query_results = await (
            await self.connection.pg_connection.execute(_query)
        ).fetchall()
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

    async def details_stripy_reports(
        self, project_ids: list[ProjectId]
    ) -> dict[ProjectSeqGroupKey, StripyReportRow]:
        """Get stripy web report links"""
        _query = t"""
        SELECT
            a.project,
            a.id,
            coalesce(a.output, ao.output, of.path) as output,
            a.timestamp_completed,
            asg.sequencing_group_id,
            a.meta -> 'outliers_detected' as outliers_detected,
            a.meta -> 'outlier_loci' as outlier_loci
        FROM analysis a
        LEFT JOIN analysis_outputs ao on a.id = ao.analysis_id
        LEFT JOIN output_file of on of.id = ao.file_id
        LEFT JOIN analysis_sequencing_group asg on asg.analysis_id = a.id
        INNER JOIN (
            SELECT
                asg.sequencing_group_id,
                MAX(a.id) as max_analysis_id
            FROM analysis a
            LEFT JOIN analysis_sequencing_group asg on asg.analysis_id = a.id
            WHERE type = 'web'
            AND status = 'completed'
            AND project = ANY({project_ids})
            AND LOWER(meta ->> 'stage') = 'stripy'
            GROUP BY asg.sequencing_group_id
        ) latest_analysis ON a.id = latest_analysis.max_analysis_id;
        """

        _query_results = await (
            await self.connection.pg_connection.execute(_query)
        ).fetchall()
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

    async def details_mito_reports(
        self, project_ids: list[ProjectId]
    ) -> dict[ProjectSeqGroupKey, AnalysisRow]:
        """Get mito web report links"""

        _query = t"""
        SELECT
            a.project,
            a.id,
            coalesce(a.output, ao.output, out_file.path) as output,
            a.timestamp_completed,
            asg.sequencing_group_id
        FROM analysis a
        LEFT JOIN analysis_outputs ao on a.id = ao.analysis_id
        LEFT JOIN output_file out_file on out_file.id = ao.file_id
        LEFT JOIN analysis_sequencing_group asg on asg.analysis_id = a.id
        INNER JOIN (
            SELECT
                asg.sequencing_group_id,
                MAX(a.id) as max_analysis_id
            FROM analysis a
            LEFT JOIN analysis_sequencing_group asg on asg.analysis_id = a.id
            WHERE type = 'web'
            AND status = 'completed'
            AND project = ANY({project_ids})
            AND LOWER(meta ->> 'stage') = 'mitoreport'
            GROUP BY asg.sequencing_group_id
        ) latest_analysis ON a.id = latest_analysis.max_analysis_id;
        """

        _query_results = await (
            await self.connection.pg_connection.execute(_query)
        ).fetchall()
        mito_reports: dict[ProjectSeqGroupKey, AnalysisRow] = {}
        for row in _query_results:
            key = ProjectSeqGroupKey(row['project'], row['sequencing_group_id'])
            mito_reports[key] = self.get_analysis_row(row)

        return mito_reports

    # Helper functions
    def get_analysis_row(self, row: dict[Any, Any]) -> AnalysisRow:
        """Parse a table row returned by fetch_all into an AnalysisRow object"""
        return AnalysisRow(
            id=row['id'],
            output=row['output'],
            timestamp_completed=row['timestamp_completed'],
        )

    def parse_project_seqtype_technology_keyed_rows(
        self, rows: list[dict[Any, Any]], value_field: str
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
            parsed_rows[key] = row.get(value_field)
        return parsed_rows
