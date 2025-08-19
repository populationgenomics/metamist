"""Value objects for the audit module - immutable data structures."""

from dataclasses import dataclass
from enum import Enum
from pathlib import Path as PathLib
from typing import Optional

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
        return self.value
    
    @classmethod
    def all_read_extensions(cls) -> tuple[str, ...]:
        """Get all read file extensions (FASTQ, BAM, CRAM)."""
        return cls.FASTQ.value + cls.BAM.value + cls.CRAM.value
    
    @classmethod
    def all_extensions(cls) -> tuple[str, ...]:
        """Get all file extensions."""
        return tuple(ext for file_type in cls for ext in file_type.value)


class SequencingType(Enum):
    """Sequencing type enumeration."""
    GENOME = 'genome'
    EXOME = 'exome'
    TOTAL_RNA = 'totalrna'
    POLY_RNA = 'polyarna'
    SINGLE_CELL_RNA = 'singlecellrna'


class AnalysisType(Enum):
    """Analysis type enumeration."""
    CRAM = 'cram'
    GVCF = 'gvcf'
    VCF = 'vcf'
    QC = 'qc'
    FASTQC = 'fastqc'
    MULTIQC = 'multiqc'


@dataclass(frozen=True)
class FilePath:
    """Immutable file path value object."""
    path: Path
    
    @property
    def name(self) -> str:
        return self.path.name
    
    @property
    def bucket(self) -> str:
        return self.path.bucket
    
    @property
    def blob(self) -> str:
        return self.path.blob
    
    @property
    def uri(self) -> str:
        return self.path.as_uri()
    
    def __str__(self) -> str:
        return self.uri


@dataclass(frozen=True)
class FileMetadata:
    """Immutable file metadata."""
    filepath: FilePath
    filesize: Optional[int] = None
    checksum: Optional[str] = None
    
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
    
    def get_primary(self) -> Optional[str]:
        """Get the primary external ID if only one exists."""
        if len(self.ids) == 1:
            return next(iter(self.ids.values()))
        return None
    
    def __getitem__(self, key: str) -> Optional[str]:
        return self.ids.get(key)
    
    def values(self) -> list[str]:
        return list(self.ids.values())


@dataclass(frozen=True)
class AuditConfig:
    """Immutable configuration for audit runs."""
    dataset: str
    sequencing_types: tuple[str, ...]
    sequencing_technologies: tuple[str, ...]
    sequencing_platforms: tuple[str, ...]
    analysis_types: tuple[str, ...]
    file_types: tuple[FileType, ...]
    excluded_prefixes: tuple[str, ...] = ()
    excluded_sequencing_groups: tuple[str, ...] = ()
    
    @classmethod
    def from_cli_args(cls, args) -> 'AuditConfig':
        """Factory method from CLI arguments."""
        file_types = []
        for ft in args.file_types:
            if ft == 'all':
                file_types = list(FileType)
                break
            elif ft == 'all_reads':
                file_types = [FileType.FASTQ, FileType.BAM, FileType.CRAM]
                break
            else:
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
        )


@dataclass(frozen=True)
class MovedFile:
    """Represents a file that has been moved."""
    old_path: FilePath
    new_path: FilePath
    metadata: FileMetadata
