"""Value objects for the audit module - immutable data structures."""

from dataclasses import dataclass
from enum import Enum

from cpg_utils import Path

from ..data_access import MetamistDataAccess


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
    def all_read_extensions(cls) -> tuple[str, ...]:
        """Get all read file extensions (FASTQ, BAM, CRAM)."""
        return cls.FASTQ.value + cls.BAM.value + cls.CRAM.value

    @classmethod
    def all_extensions(cls) -> tuple[str, ...]:
        """Get all file extensions."""
        return tuple(ext for file_type in cls for ext in file_type.value)


@dataclass(frozen=True)
class FilePath:
    """Immutable file path value object."""

    path: Path

    @property
    def name(self) -> str:
        """Get the file name."""
        return self.path.name

    @property
    def bucket(self) -> str:
        """Get the bucket name."""
        return self.path.bucket

    @property
    def blob(self) -> str:
        """Get the blob name."""
        return self.path.blob

    @property
    def uri(self) -> str:
        """Get the URI."""
        return self.path.as_uri()

    def __str__(self) -> str:
        return self.uri


@dataclass(frozen=True)
class FileMetadata:
    """Immutable file metadata."""

    filepath: FilePath
    filesize: int | None = None
    checksum: str | None = None

    def with_size(self, size: int) -> 'FileMetadata':
        """Create a new instance with updated size."""
        return FileMetadata(self.filepath, size, self.checksum)

    def with_checksum(self, checksum: str) -> 'FileMetadata':
        """Create a new instance with updated checksum."""
        return FileMetadata(self.filepath, self.filesize, checksum)


@dataclass(frozen=True)
class ExternalIds:
    """External ID collection."""

    ids: dict[str, str]

    def get_primary(self) -> str | None:
        """Get the primary external ID if only one exists."""
        return self.ids.get('')

    def __getitem__(self, key: str) -> str | None:
        return self.ids.get(key)

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
    async def from_cli_args_validated(
        cls, args, metamist: MetamistDataAccess
    ) -> 'AuditConfig':
        """Factory method that creates and validates config from CLI args."""
        config = cls.from_cli_args(args)
        return await config.validate_metamist_enums(metamist)

    @classmethod
    def from_cli_args(cls, args) -> 'AuditConfig':
        """Factory method from CLI arguments."""
        file_types = []
        for ft in args.file_types:
            if ft == 'all':
                file_types = list(FileType)
                break
            if ft == 'all_reads':
                file_types = [FileType.FASTQ, FileType.BAM, FileType.CRAM]
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

    async def validate_metamist_enums(
        self,
        metamist: MetamistDataAccess,
    ) -> 'AuditConfig':
        """
        Validate enum values against Metamist API.

        Args:
            metamist: Metamist data access object

        Returns:
            Validated configuration
        """

        async def validate_enum_value(enum_type: str, config_values: tuple[str]) -> str:
            valid_values = await metamist.graphql_client.get_enum_values(enum_type)
            if 'all' in config_values:
                return valid_values
            if any(value.lower() not in valid_values for value in config_values):
                raise ValueError(
                    f"Invalid {enum_type} values: {', '.join(config_values)}. "
                    f"Valid values are: {', '.join(valid_values)}."
                )
            return tuple(config_values)

        sequencing_types = await validate_enum_value(
            'sequencing_type', self.sequencing_types
        )
        sequencing_techs = await validate_enum_value(
            'sequencing_technology', self.sequencing_technologies
        )
        sequencing_platforms = await validate_enum_value(
            'sequencing_platform', self.sequencing_platforms
        )
        analysis_types = await validate_enum_value('analysis_type', self.analysis_types)

        return AuditConfig(
            dataset=self.dataset,
            sequencing_types=sequencing_types,
            sequencing_technologies=sequencing_techs,
            sequencing_platforms=sequencing_platforms,
            analysis_types=analysis_types,
            file_types=self.file_types,
            excluded_prefixes=self.excluded_prefixes,
        )


@dataclass(frozen=True)
class MovedFile:
    """Represents a file that has been moved."""

    old_path: FilePath
    new_path: FilePath
    metadata: FileMetadata
