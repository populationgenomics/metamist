"""Core business entities for the audit module."""

from dataclasses import dataclass, field

from .value_objects import FileMetadata, ExternalIds


@dataclass
class Participant:
    """Participant entity."""

    id: int
    external_ids: ExternalIds

    def to_gql_dict(self) -> dict:
        """Convert the participant to a GQL dictionary representation."""
        return {
            'id': self.id,
            'externalIds': self.external_ids.ids,
        }

    @property
    def external_id(self) -> str | None:
        """Get the primary external ID."""
        return self.external_ids.get_primary()


@dataclass
class Sample:
    """Sample entity."""

    id: str
    external_ids: ExternalIds
    participant: Participant

    def to_gql_dict(self) -> dict:
        """Convert the sample to a GQL dictionary representation."""
        return {
            'id': self.id,
            'externalIds': self.external_ids.ids,
            'participant': self.participant.to_gql_dict(),
        }

    @property
    def external_id(self) -> str | None:
        """Get the primary external ID."""
        return self.external_ids.get_primary()


@dataclass
class Assay:
    """Assay entity."""

    id: int
    read_files: list[FileMetadata] = field(default_factory=list)

    def to_gql_dict(self) -> dict:
        """Convert the assay to a GQL dictionary representation."""
        return {
            'id': self.id,
            'meta': {
                'reads': [read_file.to_gql_dict() for read_file in self.read_files],
            },
        }

    def add_read_file(self, read_file: FileMetadata):
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
    output_file: FileMetadata | None = None
    original_file: FileMetadata | None = None
    sequencing_group_id: str | None = None
    timestamp_completed: str | None = None

    def to_gql_dict(self) -> dict:
        """Convert the analysis to a GQL dictionary representation."""
        return {
            'id': self.id,
            'type': self.type.lower(),
            'status': 'completed',
            'meta': {},
            'output': str(self.output_file.filepath) if self.output_file else None,
            'timestampCompleted': self.timestamp_completed,
        }

    @property
    def is_cram(self) -> bool:
        """Check if this is a CRAM analysis."""
        return self.type.upper() == 'CRAM'

    @property
    def output_path(self) -> str | None:
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
    cram_analysis: Analysis | None = None

    def to_gql_dict(self) -> dict:
        """Convert the sequencing group to the dict representation for GraphQL."""
        return {
            'id': self.id,
            'type': self.type,
            'technology': self.technology,
            'platform': self.platform,
            'sample': self.sample.to_gql_dict(),
            'assays': [assay.to_gql_dict() for assay in self.assays],
        }

    @property
    def is_complete(self) -> bool:
        """Check if the sequencing group has a completed CRAM."""
        return self.cram_analysis is not None

    @property
    def cram_path(self) -> str | None:
        """Get the CRAM file path if it exists."""
        return self.cram_analysis.output_path if self.cram_analysis else None

    def set_cram_analysis(self, analysis: Analysis):
        """Set the CRAM analysis for this sequencing group."""
        if analysis.is_cram:
            self.cram_analysis = analysis

    def get_all_read_files(self) -> list[FileMetadata]:
        """Get all read files from all assays."""
        return [read_file for assay in self.assays for read_file in assay.read_files]

    def get_total_read_size(self) -> int:
        """Calculate total size of all read files."""
        return sum(assay.get_total_size() for assay in self.assays)


@dataclass
class AuditReportEntry:  # pylint: disable=too-many-instance-attributes
    """Entry for audit reports."""

    filepath: str | None = None
    filesize: int | None = None
    sg_id: str | None = None
    sg_type: str | None = None
    sg_tech: str | None = None
    sg_platform: str | None = None
    assay_id: int | None = None
    cram_analysis_id: int | None = None
    cram_file_path: str | None = None
    sample_id: str | None = None
    sample_external_ids: str | None = None
    participant_id: int | None = None
    participant_external_ids: str | None = None
    action: str | None = None
    review_comment: str | None = None

    # Mapping from human-readable headers to field names
    HEADER_TO_FIELD_MAP = {
        'File Path': 'filepath',
        'File Size': 'filesize',
        'SG ID': 'sg_id',
        'SG Type': 'sg_type',
        'SG Technology': 'sg_tech',
        'SG Platform': 'sg_platform',
        'Assay ID': 'assay_id',
        'Sample ID': 'sample_id',
        'Sample External ID': 'sample_external_ids',
        'Participant ID': 'participant_id',
        'Participant External ID': 'participant_external_ids',
        'CRAM Analysis ID': 'cram_analysis_id',
        'CRAM Path': 'cram_file_path',
        'Action': 'action',
        'Review Comment': 'review_comment',
    }

    def __init__(self, **kwargs):
        """Initialize an AuditReportEntry from keyword arguments."""
        for key, value in kwargs.items():
            key_name = self.HEADER_TO_FIELD_MAP.get(key, key.lower().replace(' ', '_'))
            setattr(self, key_name, value)

    @classmethod
    def get_field_to_header_map(cls) -> dict[str, str]:
        """Get the reverse mapping from field names to human-readable headers."""
        return {field: header for header, field in cls.HEADER_TO_FIELD_MAP.items()}

    @classmethod
    def from_report_dict(cls, report_dict: dict) -> 'AuditReportEntry':
        """Create an AuditReportEntry from a report dictionary with human-readable headers."""
        kwargs = {}
        for header, value in report_dict.items():
            field_name = cls.HEADER_TO_FIELD_MAP.get(
                header, header.lower().replace(' ', '_')
            )
            kwargs[field_name] = value
        return cls(**kwargs)

    def to_report_dict(self, use_external_headers: bool = True) -> dict:
        """Convert to dictionary for report generation."""
        # Simplify external IDs if only one exists
        sample_ext = self.sample_external_ids
        if isinstance(sample_ext, dict) and len(sample_ext) == 1:
            sample_ext = next(iter(sample_ext.values()))

        participant_ext = self.participant_external_ids
        if isinstance(participant_ext, dict) and len(participant_ext) == 1:
            participant_ext = next(iter(participant_ext.values()))

        # Use the mapping to ensure consistency
        field_to_header = self.get_field_to_header_map()
        result = {}

        # Get all field values
        field_values = {
            'filepath': self.filepath,
            'filesize': self.filesize,
            'sg_id': self.sg_id,
            'sg_type': self.sg_type,
            'sg_tech': self.sg_tech,
            'sg_platform': self.sg_platform,
            'assay_id': self.assay_id,
            'sample_id': self.sample_id,
            'sample_external_ids': sample_ext,
            'participant_id': self.participant_id,
            'participant_external_ids': participant_ext,
            'cram_analysis_id': self.cram_analysis_id,
            'cram_file_path': self.cram_file_path,
            'action': self.action,
            'review_comment': self.review_comment,
        }

        # Map to human-readable headers
        for field_name, value in field_values.items():
            header = field_to_header.get(
                field_name, field_name.replace('_', ' ').title()
            )
            if use_external_headers:
                result[header] = value
            else:
                # Or use field names directly
                if not value:
                    continue  # Skip fields with no values if not using headers
                result[field_name] = value

        return result

    def fieldnames(self) -> list[str]:
        """Get the field names for the audit report entry."""
        return list(self.to_report_dict().keys())

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
