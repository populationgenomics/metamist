# Metamist Audit

This module contains the code for the Metamist audit tool. This tool is used to check the integrity of the
Metamist database and the various dataset upload buckets.

## Upload bucket auditor

This auditor looks for sequence files in the main upload bucket that can be deleted or ingested,
as well as samples that have no completed cram.

### Procedure

1. Input a Metamist dataset, sequence types to audit, and file types to search for in the bucket
2. Get all the participants, samples, sequencing groups, and assays for this dataset
3. Search the upload bucket for all assay files, and compare to the files in the metamist assay reads
    - If there are discrepencies, check if the file name and size is the same in the bucket as it is in metamist
      - If the name and file size are the same, assume this file has just been moved around in the bucket
4. Check if a sequencing group has a completed cram - if so, its assay read files can be deleted
5. Any remaining assay data in the bucket might require ingestion
6. Create reports in the audit_results folder of the upload bucket, containing assay read files to delete,
   ingest, and any sequencing groups without completed crams.


### How it works

The auditor initialises an instance of the UploadBucketAuditor class, inheriting from GenericAuditor.
The auditor queries the dataset using GraphQL, returning all the participants, samples, sequencing groups, and assays (in that hierarchy).
The auditor creates several mappings between the metamist entries. These include:

 - sequencing_group_id -> sample_id
 - assay_id -> sequencing_group_id
 - assay_id -> read_file
 - sequencing_group_id -> cram_file

The auditor then queries the upload bucket for all the assay files, and compares them to the assay files in the dataset. For each discovered file, the auditor decides if the file needs to be deleted or ingested. The mappings detailed above are used to determine if a file can be deleted or ingested.

For example, consider the results of the following query and the files we can see in the upload bucket.

```python
import json
from metamist.audit import UploadBucketAuditor

auditor = UploadBucketAuditor(dataset='test-dataset')
participant_data = auditor.get_participant_data_for_dataset()

print(json.dumps(participant_data))
```

```json
{
  [
    {
      "id": 123,
      "externalId": "Participant_1",
      "samples": [
        {
          "id": "XPG123456",
          "externalId": "Sample_1",
          "sequencingGroups": [
            {
              "id": "CPG123456",
              "type": "genome",
              "assays": [
                {
                  "id": 1092,
                  "meta": {
                    "reads": [
                      {
                        "location": "gs://cpg-test-dataset-upload/Sample_1_L001_R1.fastq.gz",
                        "size": 13990183007
                      },
                      {
                        "location": "gs://cpg-test-dataset-upload/Sample_1_L001_R2.fastq.gz",
                        "size": 14574318102
                      }
                    ],
                  }
                },
              ],
              "analyses": [
                {
                  "id": 456,
                  "output": "gs://cpg-test-dataset-main/cram/CPG123456.cram",
                  "timestampCompleted": "2023-09-01T05:04:24"
                }
              ]
            }
          ]
        }
      ]
    },
    {
      "id": 124,
      "externalId": "Participant_2",
      "samples": [
        {
          "id": "XPG123467",
          "externalId": "Sample_2",
          "sequencingGroups": [
            {
              "id": "CPG123467",
              "type": "genome",
              "assays": [
                {
                  "id": 1093,
                  "meta": {
                    "reads": [
                      {
                        "location": "gs://cpg-test-dataset-upload/Sample_2_L001_R2.fastq.gz",
                        "size": 13514368650
                      },
                      {
                        "location": "gs://cpg-test-dataset-upload/Sample_2_L001_R2.fastq.gz",
                        "size": 13834661895
                      }
                    ],
                  }
                },
              ],
              "analyses": []
            }
          ]
        }
      ]
    }
  ]
}
```

We can see that Participant_1 has a completed cram, and Participant_2 does not. In both cases the assay associated with each participant's sequencing group contains two read files. Next, look in the upload bucket to decide which files should be deleted and which should be ingested.

```bash
$ gsutil ls gs://cpg-test-dataset-upload

gs://cpg-test-dataset-upload/Sample_1_L001_R1.fastq.gz
gs://cpg-test-dataset-upload/Sample_1_L001_R2.fastq.gz
gs://cpg-test-dataset-upload/Sample_2_L001_R1.fastq.gz
gs://cpg-test-dataset-upload/Sample_2_L001_R2.fastq.gz

$ gsutil ls gs://cpg-test-dataset-main/cram
gs://cpg-test-dataset-main/cram/CPG123456.cram
```

For the first two fastq files, we know they are associated with assay 1092, and that the cram for sequencing group CPG123456 has been completed and matches a file in the /cram folder of the main bucket. Therefore, we should delete these files.

For the second two fastq files, we know they are associated with assay 1093, and that the cram for sequencing group CPG123467 has not been completed. Therefore, we should ingest these files.
