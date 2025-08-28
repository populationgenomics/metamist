# Metamist Audit

Audit system for managing cloud storage buckets and the integrity of genomic data.

## Overview

This audit module provides tools for:

- Identifying files that can be safely deleted
- Identifying files that require manual review
- Detecting sequencing groups requiring processing
- Tracking file movements and duplications within buckets

### Implementation Details

#### Deletions

Deleting files is a critical aspect of the audit process. The system identifies files that can be safely deleted, such as original reads where CRAMs exist. This helps in managing storage costs and maintaining an organized file structure.

Other conditions which could be met to identify files for deletion:

- If the file has been copied to the main bucket and recorded as an Analysis record
- If the file is associated with a sequencing group that has been completed, e.g. additional VCF or BAM files provided on top of the original fastqs
- If the file is a duplicate of a file recorded in Metamist or found elsewhere in the bucket

In general, files are considered for deletion if they are no longer needed for analysis or if they have been superseded by newer versions. Any file which cannot be definitively considered as safe to delete should not be deleted without further review. Some manual review of the results may be necessary to ensure that no important data is lost.

Deletions are handled via the `delete_audit_results.py` CLI entrypoint. This script allows deletion from the `files_to_delete` report, which is generated during the audit process. It will also enable deletion of files from the `files_to_review` report, if the user has reviewed and annotated the report marking the rows intended for deletion.

Using the deletion script will create or update a report `deleted_files` and a corresponding analysis record in Metamist, of analysis type `audit_deletion`. The analysis meta tracks the deletion stats for the audit, separating the deletion actions depending on their source report.

An example analysis record might look like this:

```json
{
  "id": 12345,
  "type": "audit_deletion",
  "output": "gs://cpg-dataset-main-analysis/audit_results/2025-01-01_123456/deleted_files.csv",
  "meta": {
    "report_stats": {
      "files_to_delete": {
        "deleted_files": 200,
        "deleted_bytes": 4011744806301
      },
      "files_to_review": {
        "deleted_files": 100,
        "deleted_bytes": 2005872403150
      }
    }
  }
}
```

#### Reviews

If a file is found and not identified as a 'safe' deletion candidate, it must be manually reviewed. These review candidates are saved to the 'files_to_review' report.

Naturally, some of the files in this report will be candidates for deletion. To mark these files for deletion, the user must review and annotate the report, indicating which files are safe to delete. Reviews are conducted through the CLI entrypoint `review_audit_results.py`. Users must provide their annotations and justifications for each file marked for deletion via input filters and input comments.

Using this CLI tool, a new report will be created or updated, called `reviewed_files.csv`. This report will contain the user's annotations and justifications for each file marked for deletion, and is a valid input for the deletion process.

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
│   ├── audit_analyzer.py           # Core audit analysis
│   ├── audit_logging.py            # Logging utilities
│   ├── file_matcher.py             # File matching strategies
│   └── report_generator.py         # Report generation
└── cli/                        # Command-line interfaces
    └── upload_bucket_audit.py      # Main CLI entry point
    └── review_audit_results.py     # CLI for reviewing audit results
    └── delete_audit_results.py     # CLI for deleting audit results
```

## Usage

### Command Line

#### Audit Upload Bucket

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

#### Review Audit Results

Users can review the audit results using the CLI entrypoint `review_audit_results.py`. This tool allows users to provide annotated justifications for each file marked for deletion.

```bash
python -m metamist.audit.cli.review_audit_results \
    --dataset my-dataset \
    --results-folder "2025-01-01_123456" \
    --comment "Unnecessary BAMs and VCFs for aligned exome sequencing groups" \
    --filter "SG Type==exome" \
    --filter "File Type==BAM" \
    --filter "File Type==VCF"
```

#### Delete Audit Results

Users can delete the audit results using the CLI entrypoint `delete_audit_results.py`. This tool allows users to specify which files should be deleted based on the audit results.

```bash
# To delete from the 'files_to_delete' report
python -m metamist.audit.cli.delete_audit_results \
    --dataset my-dataset \
    --results-folder "2025-01-01_123456"

# To delete files marked for deletion from the annotated 'reviewed_files.csv' report
python -m metamist.audit.cli.delete_audit_results \
    --dataset my-dataset \
    --results-folder "2025-01-01_123456" \
    --report-name "reviewed_files"
```

## Reports Generated

The audit generates several reports in the analysis bucket:

### From the `upload_bucket_audit.py` script

1. **audit_metadata.tsv**: Configuration used for the audit
2. **files_to_delete.csv**: Files that can be safely deleted
3. **files_to_review.csv**: Files that need to be reviewed
4. **unaligned_sgs.tsv**: Sequencing groups without completed CRAMs

### From the `review_audit_results.py` script

1. **reviewed_files.csv**: User annotations and justifications for files marked for deletion

### From the `delete_audit_results.py` script

1. **deleted_files.csv**: List of files that were deleted
2. An analysis record in Metamist of type `audit_deletion`

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
