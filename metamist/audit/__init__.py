"""
Metamist Audit Module

This module provides tools for auditing Metamist datasets and their associated
cloud storage buckets. The primary use case is identifying:
- Files that can be safely deleted (e.g., original reads where CRAMs exist)
- Files that need review (could require ingestion into Metamist, could require deletion)
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
    AuditReportEntry,
    AuditResult,
    ReviewResult,
    DeletionResult,
    # Value objects
    AuditConfig,
    FileType,
    FileMetadata,
    ExternalIds,
    MovedFile,
)

from .services import (
    BucketAuditLogger,
    AuditAnalyzer,
    Reporter,
    FileMatchingService,
)

from .cli.upload_bucket_audit import (
    audit_upload_bucket,
    audit_upload_bucket_async,
    AuditOrchestrator,
)
from .cli.review_audit_results import review_audit_report, review_rows
from .cli.delete_from_audit_results import (
    delete_from_audit_results,
    delete_files_from_report,
    upsert_deleted_files_analysis,
)

__version__ = '2.0.0'

__all__ = [
    # Core functions
    'audit_upload_bucket',
    'audit_upload_bucket_async',
    'AuditOrchestrator',
    'review_audit_report',
    'review_rows',
    'delete_from_audit_results',
    'delete_files_from_report',
    'upsert_deleted_files_analysis',
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
    # Results
    'AuditResult',
    'AuditReportEntry',
    'ReviewResult',
    'DeletionResult',
    # Services
    'AuditAnalyzer',
    'BucketAuditLogger',
    'Reporter',
    'FileMatchingService',
    # Value objects
    'FileMetadata',
    'ExternalIds',
    'MovedFile',
]
