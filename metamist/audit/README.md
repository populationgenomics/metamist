# Metamist Audit

Audit system for managing cloud storage buckets and the integrity of genomic data.

## Overview

This audit module provides tools for:

- Identifying files that can be safely deleted (e.g., original reads where CRAMs exist)
- Finding files that need ingestion into Metamist
- Detecting sequencing groups requiring processing
- Tracking file movements and duplications within buckets

## Architecture

```text
audit/
├── models/                     # Pure data models (no external dependencies)
│   ├── entities.py                 # Core business entities
│   └── value_objects.py            # Immutable value objects
├── adapters/                   # External interface adapters
│   ├── graphql_client.py           # GraphQL API adapter
│   └── storage_client.py           # Google Cloud Storage adapter
├── data_access/                # Data access layer
│   ├── metamist_data_access.py     # Metamist data access
│   └── gcs_data_access.py          # GCS data access
├── services/                   # Business logic (pure functions)
│   ├── file_matcher.py             # File matching strategies
│   ├── audit_analyzer.py           # Core audit analysis
│   └── report_generator.py         # Report generation
└── cli/                        # Command-line interfaces
    └── upload_bucket_audit.py      # Main CLI entry point
    └── delete_audit_results.py     # CLI for deleting audit results
```

## Usage

### Command Line

```bash
# Basic usage
python -m metamist.audit.cli.upload_bucket_audit \
    --dataset my-dataset \
    --sequencing-types genome \
    --sequencing-technologies short-read \
    --file-types fastq

# With more complex options
python -m metamist.audit.cli.upload_bucket_audit \
    -d my-dataset \
    -s all \
    -t short-read -t long-read \
    -p illumina -p pacbio -p oxford-nanopore \
    -a CRAM -a pacbio_cram -a vcf -a pacbio_vcf \
    -f all \
    -e test/ -e tmp/
```

### With analysis-runner

```bash
analysis-runner \
  --access-level full \
  --dataset my-dataset \
  --description "Audit the dataset upload bucket" \
  --output-dir "NA" \
  --config /path/to/custom_config.toml \
  python -m metamist.audit.cli.upload_bucket_audit \
    --dataset my-dataset \
    --sequencing-types genome \
    --sequencing-technologies short-read \
    --file-types fastq
```


## Reports Generated

The audit generates several reports in the upload bucket:

1. **audit_metadata.tsv**: Configuration used for the audit
2. **files_to_delete.csv**: Files that can be safely deleted
3. **files_to_ingest.csv**: Files that need to be ingested
4. **moved_files.csv**: Files that have been moved within the bucket
5. **unaligned_sgs.tsv**: Sequencing groups without completed CRAMs


## Programmatic Usage

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

## Advanced Usage with Custom Components

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

## Testing

The architecture makes testing straightforward:

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

Use `all` to include all types, or specify sequencing types from the enum values.

- genome
- exome
- totalrna
- polyarna
- singlecellrna

### Sequencing Technologies

Use `all` to include all technologies, or specify from available enum values

### Analysis Types

Default is `CRAM`, but you can specify others like `VCF`, `GVCF`, etc.

### Excluded prefixes

Specify any prefixes to exclude from the audit (e.g., `tmp/`, `test/`).

Any files matching these prefixes will be ignored during the audit process.

E.g. `gs://cpg-my-dataset-main-upload/tmp/my_file.fastq` will be excluded.

## Future Enhancements

- Add caching layer for frequently accessed data
- Implement parallel processing for large datasets
- Add dry-run mode for testing
- Support for additional cloud storage providers
- Real-time monitoring and alerting
- Web UI for audit results
