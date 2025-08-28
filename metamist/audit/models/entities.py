"""Core business entities for the audit module."""

from dataclasses import dataclass, field
from typing import Optional

from .value_objects import FileMetadata, ExternalIds


@dataclass
class Participant:
    """Participant entity."""

    id: int
    external_ids: ExternalIds

    @property
    def external_id(self) -> Optional[str]:
        """Get the primary external ID."""
        return self.external_ids.get_primary()


@dataclass
class Sample:
    """Sample entity."""

    id: str
    external_ids: ExternalIds
    participant: Participant

    @property
    def external_id(self) -> Optional[str]:
        """Get the primary external ID."""
        return self.external_ids.get_primary()


@dataclass
class ReadFile:
    """Read file entity."""

    metadata: FileMetadata

    @property
    def filepath(self):
        """Get the file path."""
        return self.metadata.filepath

    @property
    def filesize(self):
        """Get the file size."""
        return self.metadata.filesize

    @property
    def checksum(self):
        """Get the file checksum."""
        return self.metadata.checksum

    def update_metadata(self, metadata: FileMetadata):
        """Update the file metadata."""
        self.metadata = metadata


@dataclass
class Assay:
    """Assay entity."""

    id: int
    read_files: list[ReadFile] = field(default_factory=list)

    def add_read_file(self, read_file: ReadFile):
        """Add a read file to the assay."""
        self.read_files.append(read_file)

    def get_total_size(self) -> int:
        """Calculate total size of all read files."""
        return sum(f.filesize or 0 for f in self.read_files)


@dataclass
class Analysis:
    """Analysis entity."""

    id: int
    type: str
    output_file: Optional[FileMetadata] = None
    original_file: Optional[FileMetadata] = None
    sequencing_group_id: Optional[str] = None
    timestamp_completed: Optional[str] = None

    @property
    def is_cram(self) -> bool:
        """Check if this is a CRAM analysis."""
        return self.type.upper() == 'CRAM'

    @property
    def output_path(self) -> Optional[str]:
        """Get the output file path if it exists."""
        return str(self.output_file.filepath) if self.output_file else None


@dataclass
class SequencingGroup:
    """Sequencing group entity."""

    id: str
    type: str
    technology: str
    platform: str
    sample: Sample
    assays: list[Assay] = field(default_factory=list)
    cram_analysis: Optional[Analysis] = None

    @property
    def is_complete(self) -> bool:
        """Check if the sequencing group has a completed CRAM."""
        return self.cram_analysis is not None

    @property
    def cram_path(self) -> Optional[str]:
        """Get the CRAM file path if it exists."""
        return self.cram_analysis.output_path if self.cram_analysis else None

    def set_cram_analysis(self, analysis: Analysis):
        """Set the CRAM analysis for this sequencing group."""
        if analysis.is_cram:
            self.cram_analysis = analysis

    def get_all_read_files(self) -> list[ReadFile]:
        """Get all read files from all assays."""
        return [read_file for assay in self.assays for read_file in assay.read_files]

    def get_total_read_size(self) -> int:
        """Calculate total size of all read files."""
        return sum(assay.get_total_size() for assay in self.assays)


@dataclass
class AuditReportEntry:  # pylint: disable=too-many-instance-attributes
    """Entry for audit reports."""

    filepath: Optional[str] = None
    filesize: Optional[int] = None
    sg_id: Optional[str] = None
    sg_type: Optional[str] = None
    sg_tech: Optional[str] = None
    sg_platform: Optional[str] = None
    assay_id: Optional[int] = None
    cram_analysis_id: Optional[int] = None
    cram_file_path: Optional[str] = None
    sample_id: Optional[str] = None
    sample_external_ids: Optional[str] = None
    participant_id: Optional[int] = None
    participant_external_ids: Optional[str] = None
    action: Optional[str] = None
    review_comment: Optional[str] = None

    def to_report_dict(self) -> dict:
        """Convert to dictionary for report generation."""
        # Simplify external IDs if only one exists
        sample_ext = self.sample_external_ids
        if isinstance(sample_ext, dict) and len(sample_ext) == 1:
            sample_ext = next(iter(sample_ext.values()))

        participant_ext = self.participant_external_ids
        if isinstance(participant_ext, dict) and len(participant_ext) == 1:
            participant_ext = next(iter(participant_ext.values()))

        return {
            'File Path': self.filepath,
            'File Size': self.filesize,
            'SG ID': self.sg_id,
            'SG Type': self.sg_type,
            'SG Technology': self.sg_tech,
            'SG Platform': self.sg_platform,
            'Assay ID': self.assay_id,
            'Sample ID': self.sample_id,
            'Sample External ID': sample_ext,
            'Participant ID': self.participant_id,
            'Participant External ID': participant_ext,
            'CRAM Analysis ID': self.cram_analysis_id,
            'CRAM Path': self.cram_file_path,
            'Action': self.action,
            'Review Comment': self.review_comment,
        }

    def update_action(self, action: str):
        """Update the action for this audit report entry."""
        self.action = action

    def update_review_comment(self, comment: str):
        """Update the review comment for this audit report entry."""
        self.review_comment = comment


@dataclass
class AuditResult:
    """Result of an audit analysis."""

    files_to_delete: list[AuditReportEntry] = field(default_factory=list)
    files_to_review: list[AuditReportEntry] = field(default_factory=list)
    moved_files: list[AuditReportEntry] = field(default_factory=list)
    unaligned_sequencing_groups: list[SequencingGroup] = field(default_factory=list)


@dataclass
class ReviewResult:
    """Result of a review of audit results."""

    reviewed_files: list[AuditReportEntry] = field(default_factory=list)


@dataclass
class DeletionResult:
    """Result of a deletion of audit results."""

    deleted_files: list[AuditReportEntry] = field(default_factory=list)
