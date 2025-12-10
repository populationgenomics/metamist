# OutputFile Model Documentation

The OutputFile model in Metamist represents and tracks file outputs from bioinformatics analyses. This system automatically validates files against Google Cloud Storage (GCS), extracts metadata, and manages complex file relationships like VCF files with their index files.

---

## Table of Contents

- [Quick Start](#quick-start)
  - [Upload a Single File](#upload-a-single-file)
  - [Upload a VCF with Index](#upload-a-vcf-with-index)
  - [Understanding the Response](#understanding-the-response)
- [Understanding the Model](#understanding-the-model)
  - [What is an OutputFile?](#what-is-an-outputfile)
  - [How OutputFiles Work](#how-outputfiles-work)
  - [File Validation](#file-validation)
  - [Parent-Child Relationships](#parent-child-relationships)
- [API Usage Guide](#api-usage-guide)
  - [GraphQL API](#graphql-api)
  - [Output Structures Comparison](#output-structures-comparison)
  - [Response Format Examples](#response-format-examples)
- [Advanced Patterns](#advanced-patterns)
  - [Complex Nested Structures](#complex-nested-structures)
  - [Handling Invalid Files](#handling-invalid-files)
  - [Secondary Files](#secondary-files)
  - [Special File Types](#special-file-types)

---

## Quick Start

This section gets you started with the most common use cases for OutputFiles in Metamist.

### Upload a Single File

To add an output file to an analysis, include it in the `outputs` field when creating an analysis via GraphQL.

**Example variables:**

```json
{
  "project": "my-project",
  "sequencingGroupIds": ["CPGABC123"],
  "type": "alignment",
  "status": "COMPLETED",
  "outputs": {
    "bam": {
      "basename": "gs://my-bucket/results/sample.bam"
    }
  }
}
```

The system will automatically:

- Extract file metadata (size, checksum) from GCS
- Parse the file path into components (directory, filename, extension)
- Validate that the file exists
- Create an OutputFile record linked to your analysis

See [GraphQL API](#graphql-api) for the full mutation details.

### Upload a VCF with Index

Use the `secondary_files` field to link companion files:

**Variables:**

```json
{
  "project": "my-project",
  "sequencingGroupIds": ["CPGABC123"],
  "type": "joint-calling",
  "status": "COMPLETED",
  "outputs": {
    "vcf": {
      "basename": "gs://my-bucket/results/cohort.vcf.gz",
      "secondary_files": {
        "index": {
          "basename": "gs://my-bucket/results/cohort.vcf.gz.tbi"
        }
      }
    }
  }
}
```

The index file is tracked as a child via the `parent_id` foreign key.

### Understanding the Response

When you query an analysis, the OutputFile data is returned with full metadata extracted from GCS.

**What you send:**

```json
{
  "outputs": {
    "vcf": {
      "basename": "gs://my-bucket/results/cohort.vcf.gz"
    }
  }
}
```

**What you get back:**

```json
{
  "outputs": {
    "vcf": {
      "id": 456,
      "path": "gs://my-bucket/results/cohort.vcf.gz",
      "basename": "cohort.vcf.gz",
      "dirname": "gs://my-bucket/results",
      "nameroot": "cohort",
      "nameext": ".vcf.gz",
      "file_checksum": "abc123def456",
      "size": 1024000,
      "meta": null,
      "valid": true,
      "secondary_files": {}
    }
  }
}
```

> [!TIP]
> Files that don't exist in GCS won't have OutputFile records created - they'll be returned as simple string values instead.

---

## Understanding the Model

### What is an OutputFile?

An OutputFile represents a file in Google Cloud Storage produced by a bioinformatics analysis. Each OutputFile record contains:

- **File location**: Full GCS path (`gs://bucket/path/file.ext`)
- **File metadata**: Size, CRC32C checksum, validation status
- **File structure**: Path components (basename, directory, extension)
- **Relationships**: Links to secondary files (indices, checksums, etc.)
- **Custom metadata**: Optional JSON field for additional information

### How OutputFiles Work

When you create an analysis with outputs, the system validates files against GCS, extracts metadata, and builds parent-child relationships for secondary files. The following diagram illustrates this flow:

```mermaid
graph TD
    A[User submits analysis with outputs] --> B{Parse output structure}
    B --> C[Extract GCS paths]
    C --> D[Validate files in GCS]
    D --> E{File exists?}
    E -->|Yes| F[Extract metadata<br/>checksum, size]
    E -->|No| G[Mark as invalid<br/>store as string]
    F --> H[Create OutputFile record]
    G --> I[Store raw output string]
    H --> J[Link to Analysis via<br/>analysis_outputs table]
    I --> J
    J --> K[Return enriched response<br/>to user]

    style F fill:#90EE90,color:black
    style G fill:#FFB6C1,color:black
    style K fill:#87CEEB,color:black
```

### File Validation

Every OutputFile is automatically validated against Google Cloud Storage when created or updated.

**Validation Process:**

1. **Existence Check**: Metamist attempts to access the file at the specified GCS path
2. **Metadata Extraction**: If the file exists, GCS metadata is retrieved:
   - **CRC32C Checksum**: For data integrity verification
   - **File Size**: In bytes
3. **Validity Flag**: If the file is accessible, an OutputFile record is created with `valid: true`. If the file is not accessible, no OutputFile record is created and the path falls back to a simple string

**Validation Results:**

| Validation Status | Response Format | Use Case |
|------------------|-----------------|----------|
| ✅ File exists and accessible | Full OutputFile object with `valid: true` | Normal operation |
| ❌ File not found or inaccessible | Simple string (no OutputFile created) | Planned outputs, missing files, permission issues |

<!-- markdownlint-disable MD028 -->
> [!NOTE]
> See [Handling Invalid Files](#handling-invalid-files) for details on how the system processes files that fail validation.

> [!CAUTION]
> **Matrix Table (.mt) files** are currently not supported. Because they are directories in GCS, they cannot be validated by the system. Consequently, they are treated as invalid files: no `OutputFile` record is created, and the path is stored as a simple string.
<!-- markdownlint-enable MD028 -->

### Parent-Child Relationships

Many bioinformatics file formats require companion files (index files, checksum files, etc.). OutputFile models these as parent-child relationships.

```mermaid
graph LR
    A[Primary File<br/>cohort.vcf.gz<br/>parent_id: null] --> B[Secondary File<br/>cohort.vcf.gz.tbi<br/>parent_id: 456]
    A --> C[Secondary File<br/>cohort.vcf.gz.md5<br/>parent_id: 456]

    style A fill:#87CEEB,color:black
    style B fill:#90EE90,color:black
    style C fill:#90EE90,color:black
```

**How it works:**

- **Primary files** have `parent_id = null` (internal database field)
- **Secondary files** reference their primary file via `parent_id` (internal database field)
- In API responses, secondary files are nested under `secondary_files` in their parent's object

**Example structure:**

```json
{
  "vcf": {
    "id": 456,
    "parent_id": null,
    "path": "gs://bucket/cohort.vcf.gz",
    "basename": "cohort.vcf.gz",
    "dirname": "gs://bucket",
    "nameroot": "cohort",
    "nameext": ".vcf.gz",
    "file_checksum": "abc123def456",
    "size": 1024000,
    "meta": null,
    "valid": true,
    "secondary_files": {
      "index": {
        "id": 457,
        "path": "gs://bucket/cohort.vcf.gz.tbi",
        "basename": "cohort.vcf.gz.tbi",
        "dirname": "gs://bucket",
        "nameroot": "cohort.vcf.gz",
        "nameext": ".tbi",
        "file_checksum": "def456ghi789",
        "size": 50000,
        "meta": null,
        "valid": true,
        "secondary_files": {}
      }
    }
  }
}
```

---

## API Usage Guide

This section covers how to interact with OutputFiles via the GraphQL API.

### GraphQL API

OutputFiles can also be created and queried through GraphQL. The `outputs` field uses a JSON scalar type, accepting a JSON structure as input.

**Creating an Analysis:**

```graphql
mutation CreateAnalysis(
  $project: String!
  $sequencingGroupIds: [String!]
  $type: String!
  $status: AnalysisStatus!
  $outputs: JSON
) {
  analysis {
    createAnalysis(
      project: $project
      analysis: {
        type: $type
        status: $status
        sequencingGroupIds: $sequencingGroupIds
        outputs: $outputs
      }
    ) {
      id
      status
      outputs
    }
  }
}
```

**Variables:**

```json
{
  "project": "my-project",
  "sequencingGroupIds": ["CPGABC123"],
  "type": "joint-calling",
  "status": "COMPLETED",
  "outputs": {
    "vcf": {
      "basename": "gs://bucket/results/joint.vcf.gz",
      "secondary_files": {
        "index": {"basename": "gs://bucket/results/joint.vcf.gz.tbi"}
      }
    }
  }
}
```

**Querying OutputFiles:**

```graphql
query GetAnalysis($analysisId: Int!) {
  analysis(id: $analysisId) {
    id
    type
    status
    outputs  # JSON scalar - returns entire outputs structure
  }
}
```

> [!NOTE]
> **GraphQL `outputs` field**: The `outputs` field is a `JSON` scalar type, which means you receive the entire JSON structure as a blob. You cannot select individual OutputFile fields within the GraphQL query - you get the complete nested structure with all OutputFile objects and their metadata.

### Output Structures Comparison

You can structure your outputs in different ways depending on your needs. Here's a comparison of the three main patterns:

| Pattern | Structure | Best For | Example |
|---------|-----------|----------|---------|
| **Simple String** (Deprecated) | `"output": "gs://..."` | Legacy compatibility | Single primary file |
| **Single File** | `{"file": {"basename": "gs://..."}}` | One file per analysis | BAM alignment output |
| **Multi-File** | `{"vcf": {...}, "bam": {...}}` | Multiple unrelated files | Pipeline with diverse outputs |
| **Nested Structure** | `{"results": {"filtered": {...}, "raw": {...}}}` | Organized file groups | Multi-stage analysis outputs |
| **With Secondary Files** | `{"vcf": {"basename": "...", "secondary_files": {...}}}` | Files with companions | VCF + index, BAM + BAI |

### Response Format Examples

The format of the response depends on whether the files were successfully validated and created as OutputFile objects.

#### Successful Validation

When files exist in GCS and are successfully validated:

```json
{
  "id": 123,
  "type": "joint-calling",
  "status": "completed",
  "outputs": {
    "vcf": {
      "id": 456,
      "path": "gs://bucket/results/joint.vcf.gz",
      "basename": "joint.vcf.gz",
      "dirname": "gs://bucket/results",
      "nameroot": "joint",
      "nameext": ".vcf.gz",
      "file_checksum": "abc123def456",
      "size": 1024000,
      "meta": null,
      "valid": true,
      "secondary_files": {
        "index": {
          "id": 457,
          "path": "gs://bucket/results/joint.vcf.gz.tbi",
          "basename": "joint.vcf.gz.tbi",
          "dirname": "gs://bucket/results",
          "nameroot": "joint.vcf.gz",
          "nameext": ".tbi",
          "file_checksum": "def456ghi789",
          "size": 50000,
          "meta": null,
          "valid": true,
          "secondary_files": {}
        }
      }
    }
  }
}
```

#### Invalid Files Fallback

When files don't exist in GCS or can't be accessed, they fall back to simple string values:

```json
{
  "id": 123,
  "type": "test-analysis",
  "outputs": {
    "missing_file": "gs://bucket/nonexistent.txt",
    "valid_file": {
      "id": 458,
      "path": "gs://bucket/exists.txt",
      "valid": true,
      ...
    }
  }
}
```

> [!NOTE]
> The fallback behavior ensures that analyses can be created even when files don't exist yet (e.g., for planned outputs or in-progress analyses).

---

## Advanced Patterns

This section covers more complex usage patterns and edge cases.

### Complex Nested Structures

For pipelines with multiple stages or organized outputs, you can use deeply nested structures. The JSON hierarchy is preserved in the response.

**Input:**

```json
{
  "outputs": {
    "variants": {
      "filtered": {
        "basename": "gs://bucket/filtered.vcf.gz",
        "secondary_files": {
          "index": {"basename": "gs://bucket/filtered.vcf.gz.tbi"}
        }
      },
      "raw": {
        "basename": "gs://bucket/raw.vcf.gz"
      }
    },
    "metrics": {
      "quality": {
        "basename": "gs://bucket/quality.txt"
      },
      "coverage": {
        "basename": "gs://bucket/coverage.txt"
      }
    }
  }
}
```

**Response:**

The full OutputFile objects are nested at the same locations in the JSON structure, preserving your organizational hierarchy.

```json
{
  "outputs": {
    "variants": {
      "filtered": {
        "id": 459,
        "path": "gs://bucket/filtered.vcf.gz",
        "valid": true,
        "secondary_files": {
          "index": {
            "id": 460,
            "path": "gs://bucket/filtered.vcf.gz.tbi",
            "valid": true,
            "secondary_files": {}
          }
        }
      },
      "raw": {
        "id": 461,
        "path": "gs://bucket/raw.vcf.gz",
        "valid": true,
        "secondary_files": {}
      }
    },
    "metrics": {
      "quality": {
        "id": 462,
        "path": "gs://bucket/quality.txt",
        "valid": true,
        "secondary_files": {}
      },
      "coverage": {
        "id": 463,
        "path": "gs://bucket/coverage.txt",
        "valid": true,
        "secondary_files": {}
      }
    }
  }
}
```

### Handling Invalid Files

Files may fail validation for several reasons:

- File doesn't exist yet (planned output, in-progress analysis)
- File was deleted from GCS after analysis completion
- Incorrect GCS path
- File is a Matrix Table (.mt) which is treated as a directory

**Actual Behavior** (based on code in `create_or_update_output_file()`):

- If a file **fails validation** (doesn't exist or can't be accessed), **NO OutputFile record is created**
- Instead, the path is stored as a simple string in `outputs`
- The system does **NOT** create OutputFile objects with `valid: false`

> [!IMPORTANT]
> **Why no `valid: false` OutputFiles?** The code explicitly checks `if not file_obj or not file_obj.valid: return None` in [output_file.py:L80](file:///Users/yaspan/Development/metamist/db/python/tables/output_file.py#L80). This means only files that successfully validate (exist in GCS and are accessible) get OutputFile records.

**Example with mixed valid/invalid files:**

```json
{
  "outputs": {
    "missing_file": "gs://bucket/nonexistent.txt",
    "valid_file": {
      "id": 464,
      "path": "gs://bucket/actual-file.txt",
      "valid": true,
      "secondary_files": {},
      ...
    }
  }
}
```

- `missing_file`: Simple string (file doesn't exist, no OutputFile created)
- `valid_file`: Full OutputFile object (file exists and validated)

### Secondary Files

Secondary files can be nested multiple levels deep and can have their own secondary files.

**Common Secondary File Patterns:**

| Primary File | Secondary Files | Use Case |
|--------------|----------------|----------|
| `.vcf.gz` | `.tbi` or `.csi` | VCF index for fast queries |
| `.bam` | `.bai` or `.csi` | BAM index for IGV/samtools |
| `.cram` | `.crai` | CRAM index |
| `.fasta` | `.fai`, `.dict` | Reference genome indices |
| Any file | `.md5` | Checksum validation |

**Example with multiple secondary files:**

```json
{
  "reference": {
    "basename": "gs://bucket/genome.fasta",
    "secondary_files": {
      "fai": {"basename": "gs://bucket/genome.fasta.fai"},
      "dict": {"basename": "gs://bucket/genome.dict"}
    }
  }
}
```

### Special File Types

Some file types require special handling or have limitations.

#### Matrix Table (.mt) Files

> [!WARNING]
> **Not Currently Supported**: Matrix Table (`.mt`) files used by Hail are stored as directories in GCS, not individual files. The current validation system cannot handle directory-based file formats and will fail validation for `.mt` files. Consequently, they are treated as invalid files: no `OutputFile` record is created, and the path is stored as a simple string.

**Expected Behavior**: When you include a `.mt` path in `outputs`, it will automatically be stored as a simple string (since validation fails):

```json
{
  "type": "hail-analysis",
  "outputs": {
    "matrix_table": "gs://bucket/results/data.mt",
    "summary": {"basename": "gs://bucket/summary.txt"}
  }
}
```

The response will contain:

```json
{
  "outputs": {
    "matrix_table": "gs://bucket/results/data.mt",
    "summary": {
      "id": 465,
      "path": "gs://bucket/summary.txt",
      "valid": true,
      ...
    }
  }
}
```
