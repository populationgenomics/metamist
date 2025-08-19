# OutputFile Model Documentation

## Overview

The OutputFile model in Metamist is used to represent and track file outputs from analyses, typically stored in Google Cloud Storage (GCS). The system provides two model variants:

- `OutputFileInternal`: For internal database operations and business logic
- `OutputFile`: For external API interactions (Pydantic model)

## Database Schema

### `output_file` Table

```sql
CREATE TABLE output_file (
    id INT AUTO_INCREMENT PRIMARY KEY,
    path VARCHAR(255) NOT NULL UNIQUE,           -- Full GCS path (gs://bucket/path/file.ext)
    basename VARCHAR(255) NOT NULL,              -- Filename with extension
    dirname VARCHAR(255) NOT NULL,               -- Directory path
    nameroot VARCHAR(255) NOT NULL,              -- Filename without extension
    nameext VARCHAR(25),                         -- File extension (.vcf, .bam, etc.)
    file_checksum VARCHAR(255),                  -- CRC32C checksum from GCS
    size BIGINT NOT NULL,                        -- File size in bytes
    meta VARCHAR(255),                           -- Additional metadata (JSON)
    valid BOOLEAN,                               -- Whether file exists and is valid
    parent_id INT,                               -- References output_file(id) for secondary files
    FOREIGN KEY (parent_id) REFERENCES output_file(id)
);
```

### `analysis_outputs` Table (Join Table)

```sql
CREATE TABLE analysis_outputs (
    analysis_id INT NOT NULL,                    -- References analysis(id)
    file_id INT,                                 -- References output_file(id)
    output VARCHAR(255),                         -- Raw output string (fallback)
    json_structure VARCHAR(255),                 -- JSON path structure (e.g., "results.vcf")
    FOREIGN KEY (analysis_id) REFERENCES analysis(id),
    FOREIGN KEY (file_id) REFERENCES output_file(id),
    CONSTRAINT chk_file_id_output CHECK (
        (file_id IS NOT NULL AND output IS NULL) OR
        (file_id IS NULL AND output IS NOT NULL)
    )
);
```

## Model Structure

### OutputFileInternal Fields

```python
class OutputFileInternal(SMBase):
    id: int | None = None                        # Database primary key
    parent_id: int | None = None                 # Parent file for secondary files
    path: str                                    # Full GCS path
    basename: str                                # Filename with extension
    dirname: str                                 # Directory path
    nameroot: str                                # Filename without extension
    nameext: str | None                          # File extension
    file_checksum: str | None                    # CRC32C checksum
    size: int                                    # File size in bytes
    meta: dict | None = None                     # Additional metadata
    valid: bool = False                          # File validity status
    secondary_files: dict | None = {}            # Secondary files mapping
```

## File Processing Workflow

### 1. Analysis Output Processing

When an analysis is created or updated with outputs, the system processes them through:

```python
# Via REST API
POST /analysis/{project}/
{
    "sequencing_group_ids": ["ABC123"],
    "type": "joint-calling",
    "status": "completed",
    "outputs": {
        "vcf": {
            "basename": "gs://bucket/results/joint.vcf.gz",
            "secondary_files": {
                "index": {
                    "basename": "gs://bucket/results/joint.vcf.gz.tbi"
                }
            }
        }
    }
}
```

### 2. File Information Extraction

The system automatically extracts file information from GCS paths:

```python
# Input: "gs://my-bucket/results/sample.vcf.gz"
# Extracted:
{
    "bucket": "my-bucket",
    "basename": "sample.vcf.gz",
    "dirname": "gs://my-bucket/results",
    "nameroot": "sample",
    "nameext": ".vcf.gz",
    "file_checksum": "abc123...",  # From GCS blob
    "size": 1024000,               # From GCS blob
    "valid": True                  # If file exists in GCS
}
```

### 3. Database Operations

The OutputFileTable provides methods for:

- **Creating/Updating Files**: `create_or_update_output_file()`
- **Linking to Analysis**: `add_output_file_to_analysis()`
- **Processing Complex Outputs**: `create_or_update_analysis_output_files_from_output()`

## Usage Patterns

### Creating Output Files

#### 1. Simple String Output (Deprecated)

```python
analysis = {
    "type": "alignment",
    "output": "gs://bucket/sample.bam"  # Will be deprecated
}
```

#### 2. Structured Dictionary Output (Recommended)

```python
analysis = {
    "type": "joint-calling",
    "outputs": {
        "vcf": {
            "basename": "gs://bucket/joint.vcf.gz",
            "secondary_files": {
                "index": {"basename": "gs://bucket/joint.vcf.gz.tbi"}
            }
        },
        "stats": {
            "basename": "gs://bucket/stats.txt"
        }
    }
}
```

#### 3. Complex Nested Structure

```python
analysis = {
    "type": "annotation",
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
        }
    }
}
```

### File Validation

Files are automatically validated against GCS:

- **Existence Check**: File must exist in the specified GCS bucket
- **Checksum Extraction**: CRC32C checksum retrieved from GCS metadata
- **Size Extraction**: File size retrieved from GCS metadata
- **Special Handling**: `.mt` files (Matrix Table) are treated as directories and currently not supported due to their complex structure.

### Secondary Files

The system supports hierarchical file relationships:

- **Primary Files**: Main analysis outputs
- **Secondary Files**: Index files, companion files linked to primary files
- **Parent-Child Relationship**: Tracked via `parent_id` foreign key

## API Integration

### REST API

Output files are managed indirectly through analysis endpoints:

- `PUT /analysis/{project}/` - Create analysis with outputs
- `PATCH /analysis/{analysis_id}/` - Update analysis outputs

### GraphQL

OutputFile objects can be created and updated through GraphQL analysis mutations, and are accessible through analysis queries as part of the analysis outputs.

#### GraphQL Mutations for OutputFile Creation/Updates

**Create Analysis with Outputs:**

```graphql
mutation createAnalysis($project: String!, $sequencingGroupIds: [String!], $type: String!, $status: AnalysisStatus!, $outputs: JSON) {
    analysis {
        createAnalysis(project: $project, analysis: {
            type: $type,
            status: $status,
            sequencingGroupIds: $sequencingGroupIds,
            outputs: $outputs,
            meta: {}
        }) {
            id
            status
            outputs {
                # OutputFile fields accessible here
                id
                path
                basename
                dirname
                nameroot
                nameext
                fileChecksum
                size
                valid
                secondaryFiles
                meta
            }
        }
    }
}
```

**Update Analysis Outputs:**

```graphql
mutation updateAnalysis($analysisId: Int!, $status: AnalysisStatus!, $outputs: JSON) {
    analysis {
        updateAnalysis(analysisId: $analysisId, analysis: {
            status: $status,
            outputs: $outputs
        }) {
            id
            status
            outputs {
                # Updated OutputFile fields accessible here
                id
                path
                basename
                dirname
                nameroot
                nameext
                fileChecksum
                size
                valid
                secondaryFiles
                meta
            }
        }
    }
}
```

The `outputs` parameter accepts the same JSON structure as the REST API, allowing creation and updates of OutputFile objects through GraphQL mutations.

**Example GraphQL Variables for Creating Analysis with Outputs:**

```json
{
    "project": "my-project",
    "sequencingGroupIds": ["ABC123", "ABC124"],
    "type": "joint-calling",
    "status": "COMPLETED",
    "outputs": {
        "vcf": {
            "basename": "gs://bucket/results/joint.vcf.gz",
            "secondary_files": {
                "index": {
                    "basename": "gs://bucket/results/joint.vcf.gz.tbi"
                }
            }
        },
        "stats": {
            "basename": "gs://bucket/results/stats.json"
        }
    }
}
```

## API Response Formats

When querying analysis objects that contain OutputFile data, the APIs return different formats depending on how the outputs were structured.

### REST API Response Format

#### GET /analysis/{analysis_id}

```json
{
    "id": 123,
    "type": "joint-calling",
    "status": "completed",
    "sequencing_group_ids": ["ABC123", "ABC124"],
    "timestamp_completed": "2024-06-27T10:30:00",
    "project": 1,
    "active": true,
    "meta": {"param1": "value1"},
    "output": "gs://bucket/primary-output.vcf.gz",
    "outputs": {
        "vcf": {
            "id": 456,
            "parent_id": null,
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
                    "parent_id": 456,
                    "path": "gs://bucket/results/joint.vcf.gz.tbi",
                    "basename": "joint.vcf.gz.tbi",
                    "dirname": "gs://bucket/results",
                    "nameroot": "joint.vcf.gz",
                    "nameext": ".tbi",
                    "file_checksum": "def456ghi789",
                    "size": 50000,
                    "meta": null,
                    "valid": true
                }
            }
        },
        "stats": {
            "id": 458,
            "parent_id": null,
            "path": "gs://bucket/results/stats.txt",
            "basename": "stats.txt",
            "dirname": "gs://bucket/results",
            "nameroot": "stats",
            "nameext": ".txt",
            "file_checksum": "ghi789jkl012",
            "size": 2048,
            "meta": null,
            "valid": true
        }
    }
}
```

#### Complex Nested Structure Response

For complex nested outputs, the JSON structure is preserved:

```json
{
    "outputs": {
        "variants": {
            "filtered": {
                "id": 459,
                "parent_id": null,
                "path": "gs://bucket/filtered.vcf.gz",
                "basename": "filtered.vcf.gz",
                "dirname": "gs://bucket",
                "nameroot": "filtered",
                "nameext": ".vcf.gz",
                "file_checksum": "xyz789abc123",
                "size": 2048000,
                "meta": null,
                "valid": true,
                "secondary_files": {
                    "index": {
                        "id": 460,
                        "parent_id": 459,
                        "path": "gs://bucket/filtered.vcf.gz.tbi",
                        "basename": "filtered.vcf.gz.tbi",
                        "dirname": "gs://bucket",
                        "nameroot": "filtered.vcf.gz",
                        "nameext": ".tbi",
                        "file_checksum": "mno345pqr678",
                        "size": 75000,
                        "meta": null,
                        "valid": true
                    }
                }
            },
            "raw": {
                "id": 461,
                "parent_id": null,
                "path": "gs://bucket/raw.vcf.gz",
                "basename": "raw.vcf.gz",
                "dirname": "gs://bucket",
                "nameroot": "raw",
                "nameext": ".vcf.gz",
                "file_checksum": "stu901vwx234",
                "size": 1536000,
                "meta": null,
                "valid": true
            }
        },
        "metrics": {
            "quality": {
                "id": 462,
                "parent_id": null,
                "path": "gs://bucket/quality.txt",
                "basename": "quality.txt",
                "dirname": "gs://bucket",
                "nameroot": "quality",
                "nameext": ".txt",
                "file_checksum": "def567ghi890",
                "size": 4096,
                "meta": {"type": "quality_metrics", "version": "1.0"},
                "valid": true
            }
        }
    }
}
```

#### Invalid Files Fallback

When files don't exist in GCS or are invalid:

```json
{
    "output": "gs://bucket/nonexistent-file.txt",
    "outputs": {
        "invalid_file": "gs://bucket/nonexistent-file.txt",
        "valid_file": {
            "id": 463,
            "parent_id": null,
            "path": "gs://bucket/valid-file.txt",
            "basename": "valid-file.txt",
            "dirname": "gs://bucket",
            "nameroot": "valid-file",
            "nameext": ".txt",
            "file_checksum": "jkl123mno456",
            "size": 8192,
            "meta": null,
            "valid": true
        }
    }
}
```

### GraphQL API Response Format

#### Query

```graphql
query GetAnalysis($analysisId: Int!) {
    analysis(id: $analysisId) {
        id
        type
        status
        output
        outputs
        timestampCompleted
        active
        meta
        sequencingGroups {
            id
        }
        project {
            name
        }
    }
}
```

#### Response

```json
{
    "data": {
        "analysis": {
            "id": 123,
            "type": "joint-calling",
            "status": "COMPLETED",
            "output": "gs://bucket/primary-output.vcf.gz",
            "outputs": {
                "vcf": {
                    "id": 456,
                    "parent_id": null,
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
                            "parent_id": 456,
                            "path": "gs://bucket/results/joint.vcf.gz.tbi",
                            "basename": "joint.vcf.gz.tbi",
                            "dirname": "gs://bucket/results",
                            "nameroot": "joint.vcf.gz",
                            "nameext": ".tbi",
                            "file_checksum": "def456ghi789",
                            "size": 50000,
                            "meta": null,
                            "valid": true
                        }
                    }
                }
            },
            "timestampCompleted": "2024-06-27T10:30:00",
            "active": true,
            "meta": {"param1": "value1"},
            "sequencingGroups": [
                {"id": "ABC123"},
                {"id": "ABC124"}
            ],
            "project": {
                "name": "my-project"
            }
        }
    }
}
```

### Key Response Format Notes

1. **output vs outputs Fields**:
   - `output`: Legacy string field (deprecated) - contains primary output file path
   - `outputs`: Structured object containing full OutputFile information

2. **File Object Structure**: Each valid file in `outputs` contains complete OutputFile model data:
   - Database metadata (id, parent_id)
   - File path information (path, basename, dirname, nameroot, nameext)
   - GCS metadata (file_checksum, size, valid)
   - Custom metadata (meta)

3. **Secondary Files**: Nested within their parent file object under `secondary_files`

4. **Invalid Files**: Files that don't exist in GCS appear as simple strings instead of objects

5. **JSON Structure Preservation**: The original nested structure from input is maintained in the response

6. **Type Differences**:
   - REST API: Returns JSON objects directly
   - GraphQL: Uses `strawberry.scalars.JSON` type for `outputs` field

## Migration and Updates

The system includes migration scripts (`scripts/migrate_analysis_outputs.py`) for:

- **Schema Updates**: Migrating existing outputs to new format
- **Data Validation**: Checking file existence and updating validity status
- **Batch Processing**: Handling large numbers of existing analyses
