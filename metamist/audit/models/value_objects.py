"""Value objects for the audit module - immutable data structures."""

from dataclasses import dataclass
from enum import Enum

from cpg_utils import Path


class FileType(Enum):
    """File type enumeration with extensions."""

    FASTQ = ('.fq.gz', '.fastq.gz', '.fq', '.fastq')
    BAM = ('.bam',)
    CRAM = ('.cram',)
    GVCF = ('.g.vcf.gz',)
    VCF = ('.vcf', '.vcf.gz', '.vcf.bgz')
    ARCHIVE = ('.tar', '.tar.gz', '.zip')

    @property
    def extensions(self) -> tuple[str, ...]:
        """Get all file extensions for this file type."""
        return self.value

    @classmethod
    def all_read_filetypes(cls) -> tuple['FileType', ...]:
        """Get all read file types (FASTQ, BAM, CRAM)."""
        return (cls.FASTQ, cls.BAM, cls.CRAM)

    @classmethod
    def all_read_extensions(cls) -> tuple[str, ...]:
        """Get all read file extensions (FASTQ, BAM, CRAM)."""
        return cls.FASTQ.value + cls.BAM.value + cls.CRAM.value

    @classmethod
    def all_filetypes(cls) -> tuple['FileType', ...]:
        """Get all file types."""
        return tuple(cls)

    @classmethod
    def all_extensions(cls) -> tuple[str, ...]:
        """Get all file extensions."""
        return tuple(ext for file_type in cls for ext in file_type.value)


@dataclass(frozen=True)
class FileMetadata:
    """Immutable file metadata."""

    filepath: Path
    filesize: int | None = None
    checksum: str | None = None

    def to_gql_dict(self) -> dict:
        """Convert the file metadata to a GQL dictionary representation."""
        return {
            'location': str(self.filepath),
            'size': self.filesize,
            'checksum': self.checksum,
        }


@dataclass(frozen=True)
class ExternalIds:
    """External ID collection."""

    ids: dict[str, str]

    def get_primary(self) -> str | None:
        """Get the primary external ID if only one exists."""
        return self.ids.get('')

    def values(self) -> list[str]:
        """Get all external ID values."""
        return list(self.ids.values())


@dataclass(frozen=True)
class AuditConfig:  # pylint: disable=too-many-instance-attributes
    """Immutable configuration for audit runs."""

    dataset: str
    sequencing_types: tuple[str, ...]
    sequencing_technologies: tuple[str, ...]
    sequencing_platforms: tuple[str, ...]
    analysis_types: tuple[str, ...]
    file_types: tuple[FileType, ...]
    excluded_prefixes: tuple[str, ...] = ()
    results_folder: str | None = None

    @classmethod
    def from_cli_args(cls, args) -> 'AuditConfig':
        """Factory method from CLI arguments."""
        file_types = []
        for ft in args.file_types:
            if ft == 'all':
                file_types = list(FileType.all_filetypes())
                break
            if ft == 'all_reads':
                file_types = list(FileType.all_read_filetypes())
                break
            # Map string to FileType
            ft_map = {
                'fastq': FileType.FASTQ,
                'bam': FileType.BAM,
                'cram': FileType.CRAM,
                'gvcf': FileType.GVCF,
                'vcf': FileType.VCF,
                'archive': FileType.ARCHIVE,
            }
            if ft in ft_map:
                file_types.append(ft_map[ft])

        return cls(
            dataset=args.dataset,
            sequencing_types=args.sequencing_types,
            sequencing_technologies=args.sequencing_technologies,
            sequencing_platforms=args.sequencing_platforms or ('all',),
            analysis_types=args.analysis_types or ('CRAM',),
            file_types=tuple(file_types),
            excluded_prefixes=args.excluded_prefixes or (),
            results_folder=args.results_folder,
        )


@dataclass(frozen=True)
class MovedFile:
    """Represents a file that has been moved."""

    old_path: Path
    new_path: Path
    metadata: FileMetadata
