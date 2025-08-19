# Metamist Audit Module (Refactored)

A clean, maintainable implementation of the Metamist audit system for managing cloud storage buckets and metadata integrity.

## Overview

This refactored audit module provides tools for:
- Identifying files that can be safely deleted (e.g., original reads where CRAMs exist)
- Finding files that need ingestion into Metamist
- Detecting sequencing groups requiring processing
- Tracking file movements within buckets

## Architecture

The module follows clean architecture principles with clear separation of concerns:

```
audit/
├── models/               # Pure data models (no external dependencies)
│   ├── entities.py      # Core business entities
│   └── value_objects.py # Immutable value objects
├── adapters/            # External interface adapters
│   ├── graphql_client.py # GraphQL API adapter
│   └── storage_client.py # Google Cloud Storage adapter
├── repositories/        # Data access layer
│   ├── metamist_repository.py # Metamist data access
│   └── gcs_repository.py      # GCS data access
├── services/           # Business logic (pure functions)
│   ├── file_matcher.py    # File matching strategies
│   ├── audit_analyzer.py  # Core audit analysis
│   └── report_generator.py # Report generation
└── cli/                # Command-line interfaces
    └── upload_bucket_audit.py # Main CLI entry point
```

### Key Design Improvements

1. **Separation of Concerns**: Each layer has a single, well-defined responsibility
2. **Dependency Injection**: All dependencies are injected, making testing easy
3. **Immutable Data Models**: Using dataclasses and frozen configurations
4. **Pure Business Logic**: Services contain no I/O or external dependencies
5. **Strategy Pattern**: Flexible file matching with composable strategies
6. **Repository Pattern**: Clean abstraction over data sources

## Usage

### Command Line

```bash
# Basic usage
python -m metamist.audit.cli.upload_bucket_audit \
    --dataset my-dataset \
    --sequencing-types genome \
    --sequencing-technologies short-read \
    --file-types fastq bam

# With all options
python -m metamist.audit.cli.upload_bucket_audit \
    --dataset my-dataset \
    --sequencing-types genome exome \
    --sequencing-technologies short-read long-read \
    --sequencing-platforms illumina pacbio \
    --analysis-types CRAM \
    --file-types all_reads \
    --excluded-prefixes tmp/ test/
```

### Programmatic Usage

```python
from metamist.audit import audit_upload_bucket, AuditConfig, FileType

# Create configuration
config = AuditConfig(
    dataset='my-dataset',
    sequencing_types=('genome', 'exome'),
    sequencing_technologies=('short-read',),
    sequencing_platforms=('illumina',),
    analysis_types=('CRAM',),
    file_types=(FileType.FASTQ, FileType.BAM),
    excluded_prefixes=('tmp/', 'test/')
)

# Run audit
audit_upload_bucket(config)
```

### Advanced Usage with Custom Components

```python
from metamist.audit.adapters import GraphQLClient, StorageClient
from metamist.audit.repositories import MetamistRepository, GCSRepository
from metamist.audit.services import AuditAnalyzer, ReportGenerator
from metamist.audit.cli import AuditOrchestrator

# Create custom components
graphql_client = GraphQLClient()
storage_client = StorageClient(project='my-gcp-project')

metamist_repo = MetamistRepository(graphql_client)
gcs_repo = GCSRepository(storage_client)

analyzer = AuditAnalyzer(logger=custom_logger)
report_writer = ReportGenerator(storage_client.client, logger=custom_logger)

# Create orchestrator
orchestrator = AuditOrchestrator(
    metamist_repo,
    gcs_repo,
    analyzer,
    report_writer,
    custom_logger
)

# Run audit
await orchestrator.run_audit(config)
```

## Reports Generated

The audit generates several reports in the upload bucket:

1. **audit_metadata.tsv**: Configuration used for the audit
2. **files_to_delete.csv**: Files that can be safely deleted
3. **files_to_ingest.csv**: Files that need to be ingested
4. **moved_files.csv**: Files that have been moved within the bucket
5. **unaligned_sgs.tsv**: Sequencing groups without completed CRAMs

## Testing

The refactored architecture makes testing straightforward:

```python
# Test file matching logic
def test_checksum_matcher():
    matcher = ChecksumMatcher()
    file1 = FileMetadata(filepath=FilePath(...), checksum='abc123')
    file2 = FileMetadata(filepath=FilePath(...), checksum='abc123')
    
    result = matcher.match(file1, [file2])
    assert result == file2

# Test audit analyzer with mock data
def test_audit_analyzer():
    analyzer = AuditAnalyzer()
    
    # Create test data
    sgs = [create_test_sg()]
    bucket_files = [create_test_file()]
    analyses = [create_test_analysis()]
    
    # Run analysis
    result = analyzer.analyze_sequencing_groups(
        sgs, bucket_files, analyses
    )
    
    assert len(result.files_to_delete) == expected_count
```

## Configuration

### File Types
- `fastq`: FASTQ files (.fq.gz, .fastq.gz, .fq, .fastq)
- `bam`: BAM files (.bam)
- `cram`: CRAM files (.cram)
- `gvcf`: gVCF files (.g.vcf.gz)
- `vcf`: VCF files (.vcf, .vcf.gz, .vcf.bgz)
- `all_reads`: All read file types (FASTQ + BAM + CRAM)
- `all`: All supported file types

### Sequencing Types
Use `all` to include all types, or specify:
- genome
- exome
- totalrna
- polyarna
- singlecellrna

### Sequencing Technologies
Use `all` to include all technologies, or specify from available enum values

### Analysis Types
Default is `CRAM`, but can specify others like:
- GVCF
- VCF
- QC
- FASTQC

## Migration from Original Module

The refactored module maintains functional compatibility while improving the architecture:

### Key Changes
1. **UploadBucketAuditor removed**: Functionality moved to AuditOrchestrator
2. **AuditHelper split**: Functionality distributed across repositories and services
3. **GenericAuditor decomposed**: Logic separated into analyzer and repositories
4. **Data classes modernized**: Using @dataclass instead of manual __init__
5. **File matching extracted**: Now uses strategy pattern for flexibility

### Migration Path
```python
# Old way
from metamist.audit.audit_upload_bucket import UploadBucketAuditor
auditor = UploadBucketAuditor(dataset='my-dataset', ...)
# Complex initialization and method calls

# New way
from metamist.audit import audit_upload_bucket, AuditConfig
config = AuditConfig(dataset='my-dataset', ...)
audit_upload_bucket(config)
```

## Benefits of Refactoring

1. **Testability**: Pure functions and dependency injection enable comprehensive unit testing
2. **Maintainability**: Clear separation of concerns makes changes easier
3. **Readability**: Smaller, focused functions with clear purposes
4. **Flexibility**: Easy to add new file types, matching strategies, or data sources
5. **Reliability**: Better error handling and validation
6. **Performance**: Potential for async optimizations and caching

## Future Enhancements

- Add caching layer for frequently accessed data
- Implement parallel processing for large datasets
- Add dry-run mode for testing
- Support for additional cloud storage providers
- Real-time monitoring and alerting
- Web UI for audit results
