# Scripts

## Parsers

- Generic parser
- Sample file map parser (for seqr)

### Generic parser

This script allows you to parse CSV / TSV manifest of arbitrary format, by
specifying HOW you want the manifest to be mapped onto individual fields.

> This script loads the WHOLE file into memory

It groups rows by the sample ID, and collapses metadata from rows.

Eg:

```text
Sample ID       sample-collection-date  depth  qc_quality  Fastqs
<sample-id>     2021-09-16              30x    0.997       <sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz
# OR
<sample-id2>    2021-09-16              30x    0.997       <sample-id2>.filename-R1.fastq.gz
<sample-id2>    2021-09-16              30x    0.997       <sample-id2>.filename-R2.fastq.gz
```

Given the files are in a bucket `gs://cpg-upload-bucket/collaborator`,
and we want to achieve the following:

- Import this manifest into the "$dataset" project of SM
- Map the following to `sample.meta`:
    - "sample-collection-date" -> "collection_date"
- Map the following to `sequence.meta`:
    - "depth" -> "depth"
    - "qc_quality" -> "qc.quality" (ie: {"qc": {"quality": 0.997}})
- Add a qc analysis object with the following mapped `analysis.meta`:
    - "qc_quality" -> "quality"

```shell
python parse_generic_metadata.py \
    --project $dataset \
    --sample-name-column "Sample ID" \
    --reads-column "Fastqs" \
    --sample-meta-field-map "sample-collection-date" "collection_date" \
    --sequence-meta-field "depth" \
    --sequence-meta-field-map "qc_quality" "qc.quality" \
    --qc-meta-field-map "qc_quality" "quality" \
    --search-path "gs://cpg-upload-bucket/collaborator" \
    <manifest-path>
```

## Existing Cohorts Data Ingestion

Data is commonly received in the form of CSV files. Each CSV file can include data on a Participant, Sample, Sequence Group, and other related attributes.

As described above, the generic metadata parser takes in a CSV file to be ingested. Each row corresponds to a sequencing group/sample, and each column corresponds to an attribute. While we try to ensure all input data is homogenized, some inputs may need further work before they can be ingested. To handle specific data formats or conditions, we have developed additional parsers that build upon our generic metadata parser.

### Existing Cohorts Ingestion Workflow

1. Parse Manifest CSVs with `parse_existing_cohort.py`

    Usage:

    ```shell
    analysis-runner --dataset <DATASET> --access-level standard
    --description <DESCRIPTION> -o parser-tmp
    python3 -m scripts.parse_existing_cohort --project <PROJECT>
    --search-location <BUCKET CONTAINING FASTQS> --batch-number <BATCH>
    --include-participant-column <PATH TO CSV>
    ```

    For example:

    ```shell
    analysis-runner --dataset fewgenomes --access-level standard
    --description "Parse Dummy Samples" -o parser-tmp
    python3 -m scripts.parse_existing_cohort --project fewgenomes
    --search-location gs://cpg-fewgenomes-main/ --batch-number "1"
    --include-participant-column gs://cpg-fewgenomes-main-upload/fewgenomes_manifest.csv
    ```

    The existing cohort parser behaves as follows:

    - If the input CSV does not include a participant_id column, the external_id is used by default. If a participant column does exist a user can specify the —include-participant-column flag, which is by default False.
    - It assumes the batch-number column does not exist in the input CSV and should be specified as a parameter. Note, this means manifests should be ingested batch per batch. Often, the batch-number is found in the csv filename. The default behavior expects this parameter to be set.
    - It assumes that FASTQ URLs are not provided in the CSV, so it pulls them from the input bucket. It also handles the fact that while the data in the input CSV is provided according to a sample ID, these sample IDs are not present in the file path of the FASTQs. Instead, FASTQ’s are named according to their fluidX tube id The script matches samples to FASTQ's accordingly.
    - It discards the header.

2. Update Participant IDs with `fix_participant_ids.py`
If participant IDs need to be ingested and were not handled correctly, `fix_participant_ids.py` can be used to update external participant IDs. It takes a map of {old_external: new_external} as input.

3. Parse Pedigrees with `parse_ped.py`
Parsing ped files is not handled by the parser. To do so, `parse_ped.py` should be used. Note, step 2 must be completed first.

### Post-Ingestion

In order to test new and existing workflows, `-test` projects should be used.
For development work, you can use fewgenomes-test, thousandgenomes-test or hgdp-test.

Prior to running an existing workflow on a new dataset, a -test project should first be populated. The `create_test_subset.py` script handles this.

Usage:

```shell
analysis-runner --dataset <DATASET> --access-level standard --description
<DESCRIPTION> -o test-subset-tmp python3 -m scripts.create_test_subset --project
<PROJECT> --samples <N_SAMPLES> --skip-ped
```

Parameters
| Option        | Description                                                                  |
|---------------|------------------------------------------------------------------------------|
| --project     | The sample-metadata project ($DATASET)                                       |
| -n, --samples | Number of samples to subset                                                  |
| --families    | Minimal number of families to include                                        |
| --skip-ped    | Flag to be used when there isn't available pedigree/family information       |
| --add-family  | Additional families to include. All samples from these fams will be included |
| --add-sample  | Additional samples to include.                                               |

In some cases, a random subset of samples or families is sufficient. In this case the `--families` or `--samples` parameters should be used.
In other cases, a specific subset of samples or families is more useful (for example, in the case of a pipeline failing on a specific sample in production). In this instance you can use the `--add-family` or `--add-sample` parameters any number of times.
