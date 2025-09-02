"""
Metamist Audit Module

This module provides tools for auditing Metamist datasets and their associated
cloud storage buckets. The primary use case is identifying:
- Files that can be safely deleted (e.g., original reads where CRAMs exist)
- Files that need ingestion into Metamist
- Sequencing groups requiring processing

Architecture:
- models/: Data models and value objects
- adapters/: External interface adapters (GraphQL, GCS)
- data_access/: Data access layer
- services/: Business logic
- cli/: Command-line interfaces

Usage:
    from metamist.audit import audit_upload_bucket, AuditConfig

    config = AuditConfig(
        dataset='my-dataset',
        sequencing_types=('genome',),
        sequencing_technologies=('short-read',),
        sequencing_platforms=('illumina',),
        analysis_types=('CRAM',),
        file_types=(FileType.FASTQ, FileType.BAM),
    )

    audit_upload_bucket(config)
"""

from .models import (
    # Core entities
    SequencingGroup,
    Analysis,
    Assay,
    Sample,
    Participant,
    ReadFile,
    AuditReportEntry,
    AuditResult,
    # Value objects
    AuditConfig,
    FileType,
    FileMetadata,
    FilePath,
    ExternalIds,
    MovedFile,
)

from .services import (
    AuditLogs,
    AuditAnalyzer,
    ReportGenerator,
    FileMatchingService,
)

from .cli import (
    audit_upload_bucket,
    audit_upload_bucket_async,
    AuditOrchestrator,
)

__version__ = '2.0.0'

__all__ = [
    # Core functions
    'audit_upload_bucket',
    'audit_upload_bucket_async',
    'AuditOrchestrator',
    # Configuration
    'AuditConfig',
    # Enums
    'FileType',
    # Core entities
    'SequencingGroup',
    'Analysis',
    'Assay',
    'Sample',
    'Participant',
    'ReadFile',
    # Results
    'AuditResult',
    'AuditReportEntry',
    # Services
    'AuditAnalyzer',
    'ReportGenerator',
    'FileMatchingService',
    # Value objects
    'FileMetadata',
    'FilePath',
    'ExternalIds',
    'MovedFile',
]
