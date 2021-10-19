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
    --sample-metadata-project $dataset \
    --sample-name-column "Sample ID" \
    --reads-column "Fastqs" \
    --sample-meta-field-map "sample-collection-date" "collection_date" \
    --sequence-meta-field "depth" \
    --sequence-meta-field-map "qc_quality" "qc.quality" \
    --qc-meta-field-map "qc_quality" "quality" \
    --search-path "gs://cpg-upload-bucket/collaborator" \
    <manifest-path>
```
