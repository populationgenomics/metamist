"""Core audit analysis business logic."""

from .file_matcher import FileMatchingService
from metamist.audit.models import (
    SequencingGroup,
    Analysis,
    AuditReportEntry,
    AuditResult,
    FileMetadata,
    MovedFile,
)


class AuditAnalyzer:
    """Core audit analysis service with pure business logic."""

    def __init__(self):
        """
        Initialize the audit analyzer.
        """
        self.file_matcher = FileMatchingService()

    def analyze_sequencing_groups(
        self,
        sequencing_groups: list[SequencingGroup],
        bucket_files: list[FileMetadata],
        analyses: list[Analysis],
        excluded_sg_ids: list[str] | None = None,
    ) -> AuditResult:
        """
        Main analysis entry point for audit.

        Args:
            sequencing_groups: List of sequencing groups from Metamist
            bucket_files: List of files found in the bucket
            analyses: List of completed analyses
            excluded_sg_ids: Optional list of SG IDs to exclude

        Returns:
            AuditResult containing all findings
        """
        excluded_sg_ids = excluded_sg_ids or []

        # Update SGs with CRAM analyses
        self._update_sgs_with_cram_analyses(sequencing_groups, analyses)

        # Identify unaligned SGs
        unaligned_sgs = [sg for sg in sequencing_groups if not sg.is_complete]

        # Get all read files from Metamist
        metamist_read_files = self._get_all_read_files(sequencing_groups)

        # Find moved files
        moved_files = self.file_matcher.find_moved_files(
            metamist_read_files, bucket_files
        )

        # Find original analysis files
        self.file_matcher.find_original_analysis_files(
            analyses,
            bucket_files,
        )

        # Find uningested files
        analysis_metadata = [a.output_file for a in analyses if a.output_file]
        uningested_files = self.file_matcher.find_uningested_files(
            metamist_read_files, bucket_files, moved_files, analysis_metadata
        )

        # Generate report entries
        files_to_delete = self._generate_delete_entries(
            sequencing_groups, bucket_files, moved_files, analyses, excluded_sg_ids
        )

        files_to_review = self._generate_review_entries(
            sequencing_groups, uningested_files
        )

        moved_entries = self._generate_moved_entries(
            sequencing_groups, moved_files, excluded_sg_ids
        )

        return AuditResult(
            files_to_delete=files_to_delete,
            files_to_review=files_to_review,
            moved_files=moved_entries,
            unaligned_sequencing_groups=unaligned_sgs,
        )

    def _update_sgs_with_cram_analyses(
        self, sequencing_groups: list[SequencingGroup], analyses: list[Analysis]
    ):
        """Update sequencing groups with their CRAM analyses."""
        # Create lookup of analyses by SG ID
        cram_by_sg: dict[str, Analysis] = {}
        for analysis in analyses:
            if analysis.is_cram and analysis.sequencing_group_id:
                sg_id = analysis.sequencing_group_id
                # Keep the latest if multiple exist
                if sg_id not in cram_by_sg:
                    cram_by_sg[sg_id] = analysis

        # Update SGs
        for sg in sequencing_groups:
            if sg.id in cram_by_sg:
                sg.set_cram_analysis(cram_by_sg[sg.id])

    def _get_all_read_files(
        self, sequencing_groups: list[SequencingGroup]
    ) -> list[FileMetadata]:
        """Get all read files from all sequencing groups."""
        read_files = []
        for sg in sequencing_groups:
            read_files.extend(sg.get_all_read_files())
        return read_files

    def _generate_delete_entries(
        self,
        sequencing_groups: list[SequencingGroup],
        bucket_files: list[FileMetadata],
        moved_files: dict[str, MovedFile],
        analyses: list[Analysis],
        excluded_sg_ids: list[str],
    ) -> list[AuditReportEntry]:
        """Generate report entries for files to delete."""
        entries = []

        bucket_paths = set()
        for path in bucket_files:
            bucket_paths.add(str(path.filepath))
        for path in moved_files.values():
            bucket_paths.add(str(path.new_path))

        for sg in sequencing_groups:
            # Assay files from completed SGs can be deleted
            if sg.id in excluded_sg_ids or not sg.is_complete:
                continue
            for assay in sg.assays:
                for read_file in assay.read_files:
                    if str(read_file.filepath) not in bucket_paths:
                        continue
                    entry = self._create_report_entry(
                        sg=sg, assay_id=assay.id, file_metadata=read_file
                    )
                    entries.append(entry)

            # Original files from ingested analyses can be deleted
            for analysis in analyses:
                if not analysis.original_file or analysis.sequencing_group_id != sg.id:
                    continue
                entry = self._create_report_entry(
                    sg=sg, file_metadata=analysis.original_file
                )
                entries.append(entry)

        return entries

    def _generate_review_entries(
        self,
        sequencing_groups: list[SequencingGroup],
        uningested_files: list[FileMetadata],
    ) -> list[AuditReportEntry]:
        """Generate report entries for files to review."""
        entries = []

        # Build lookup of external IDs to completed SGs
        completed_sgs_by_sample_id: dict[str, SequencingGroup] = {}
        completed_sgs_by_participant_id: dict[str, SequencingGroup] = {}

        for sg in sequencing_groups:
            if not sg.is_complete:
                continue

            for ext_id in sg.sample.external_ids.values():
                completed_sgs_by_sample_id[ext_id] = sg

            for ext_id in sg.sample.participant.external_ids.values():
                completed_sgs_by_participant_id[ext_id] = sg

        # Check each uningested file
        for file_metadata in uningested_files:
            filename = file_metadata.filepath.name
            matching_sg = None

            # Check if filename contains a known sample ID
            for sample_id, sg in completed_sgs_by_sample_id.items():
                if sample_id in filename:
                    matching_sg = sg
                    break

            # Check if filename contains a known participant ID
            if not matching_sg:
                for participant_id, sg in completed_sgs_by_participant_id.items():
                    if participant_id in filename:
                        matching_sg = sg
                        break

            if matching_sg:
                # File matches a completed SG, should be deleted not ingested
                entry = self._create_report_entry(
                    sg=matching_sg, file_metadata=file_metadata
                )
            else:
                # File doesn't match any SG, should be ingested
                entry = AuditReportEntry(
                    filepath=str(file_metadata.filepath),
                    filesize=file_metadata.filesize,
                )

            entries.append(entry)

        return entries

    def _generate_moved_entries(
        self,
        sequencing_groups: list[SequencingGroup],
        moved_files: dict[str, MovedFile],
        excluded_sg_ids: list[str],
    ) -> list[AuditReportEntry]:
        """Generate report entries for moved files."""
        entries = []

        # Build lookup of file paths to assays and SGs
        path_to_sg_assay: dict[str, tuple[SequencingGroup, int]] = {}

        for sg in sequencing_groups:
            for assay in sg.assays:
                for read_file in assay.read_files:
                    path = str(read_file.filepath)
                    path_to_sg_assay[path] = (sg, assay.id)

        # Create entries for moved files
        for original_path, moved_file in moved_files.items():
            if original_path not in path_to_sg_assay:
                continue

            sg, assay_id = path_to_sg_assay[original_path]

            if sg.id in excluded_sg_ids or not sg.is_complete:
                continue

            entry = self._create_report_entry(
                sg=sg, assay_id=assay_id, file_metadata=moved_file.metadata
            )
            entries.append(entry)

        return entries

    def _create_report_entry(
        self,
        sg: SequencingGroup,
        file_metadata: FileMetadata,
        assay_id: int | None = None,
    ) -> AuditReportEntry:
        """Create a report entry from sequencing group and file data."""
        return AuditReportEntry(
            filepath=str(file_metadata.filepath),
            filesize=file_metadata.filesize,
            sg_id=sg.id,
            sg_type=sg.type,
            sg_tech=sg.technology,
            sg_platform=sg.platform,
            assay_id=assay_id,
            cram_analysis_id=sg.cram_analysis.id if sg.cram_analysis else None,
            cram_file_path=sg.cram_path,
            sample_id=sg.sample.id,
            sample_external_ids=sg.sample.external_ids.ids,
            participant_id=sg.sample.participant.id,
            participant_external_ids=sg.sample.participant.external_ids.ids,
        )
