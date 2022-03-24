#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,no-self-use,wrong-import-order,unused-argument,too-many-arguments
from typing import Any, Dict, List, Optional, Tuple
import logging

import click

from sample_metadata.parser.generic_metadata_parser import (
    GenericMetadataParser,
)
from sample_metadata.parser.generic_parser import run_as_sync

__DOC = """
Parse CSV / TSV manifest of arbitrary format.
This script allows you to specify HOW you want the manifest
to be mapped onto individual data.

This script loads the WHOLE file into memory

It groups rows by the sample ID, and collapses metadata from rows.

EG:
    Sample ID       sample-collection-date  depth  qc_quality  Fastqs

    <sample-id>     2021-09-16              30x    0.997       <sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz

    # OR

    <sample-id2>    2021-09-16              30x    0.997       <sample-id2>.filename-R1.fastq.gz

    <sample-id2>    2021-09-16              30x    0.997       <sample-id2>.filename-R2.fastq.gz

Given the files are in a bucket called 'gs://cpg-upload-bucket/collaborator',
and we want to achieve the following:

- Import this manifest into the "$dataset" project of SM
- Map the following to `sample.meta`:
    - "sample-collection-date" -> "collection_date"
- Map the following to `sequence.meta`:
    - "depth" -> "depth"
    - "qc_quality" -> "qc.quality" (ie: {"qc": {"quality": 0.997}})
- Add a qc analysis object with the following mapped `analysis.meta`:
    - "qc_quality" -> "quality"

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
"""

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


@click.command(help=__DOC)
@click.option(
    '--sample-metadata-project',
    required=True,
    help='The sample-metadata project ($DATASET) to import manifest into',
)
@click.option('--sample-name-column', required=True)
@click.option(
    '--reads-column',
    help='Column where the reads information is held, comma-separated if multiple',
)
@click.option(
    '--gvcf-column',
    help='Column where the reads information is held, comma-separated if multiple',
)
@click.option(
    '--qc-meta-field-map',
    nargs=2,
    multiple=True,
    help='Two arguments per listing, eg: --qc-meta-field "name-in-manifest" "name-in-analysis.meta"',
)
@click.option(
    '--sample-meta-field',
    multiple=True,
    help='Single argument, key to pull out of row to put in sample.meta',
)
@click.option(
    '--participant-meta-field-map',
    nargs=2,
    multiple=True,
    help='Two arguments per listing, eg: --participant-meta-field-map "name-in-manifest" "name-in-participant.meta"',
)
@click.option(
    '--sample-meta-field-map',
    nargs=2,
    multiple=True,
    help='Two arguments per listing, eg: --sample-meta-field-map "name-in-manifest" "name-in-sample.meta"',
)
@click.option(
    '--sequence-meta-field',
    multiple=True,
    help='Single argument, key to pull out of row to put in sample.meta',
)
@click.option(
    '--sequence-meta-field-map',
    nargs=2,
    multiple=True,
    help='Two arguments per listing, eg: --sequence-meta-field "name-in-manifest" "name-in-sequence.meta"',
)
@click.option('--default-sample-type', default='blood')
@click.option('--default-sequence-type', default='wgs')
@click.option(
    '--confirm', is_flag=True, help='Confirm with user input before updating server'
)
@click.option('--search-path', multiple=True, required=True)
@click.argument('manifests', nargs=-1)
@run_as_sync
async def main(
    manifests,
    search_path: List[str],
    sample_metadata_project,
    sample_name_column: str,
    sample_meta_field: List[str],
    participant_meta_field_map: List[Tuple[str, str]],
    sample_meta_field_map: List[Tuple[str, str]],
    sequence_meta_field: List[str],
    sequence_meta_field_map: List[Tuple[str, str]],
    qc_meta_field_map: List[Tuple[str, str]] = None,
    reads_column: Optional[str] = None,
    gvcf_column: Optional[str] = None,
    default_sample_type='blood',
    default_sequence_type='wgs',
    confirm=False,
):
    """Run script from CLI arguments"""
    if not manifests:
        raise ValueError('Expected at least 1 manifest')

    extra_seach_paths = [m for m in manifests if m.startswith('gs://')]
    if extra_seach_paths:
        search_path = list(set(search_path).union(set(extra_seach_paths)))

    participant_meta_map: Dict[Any, Any] = {}
    sample_meta_map: Dict[Any, Any] = {}
    sequence_meta_map: Dict[Any, Any] = {}

    qc_meta_map = dict(qc_meta_field_map or {})
    if participant_meta_field_map:
        participant_meta_map.update(dict(participant_meta_map))
    if sample_meta_field_map:
        sample_meta_map.update(dict(sample_meta_field_map))
    if sample_meta_field:
        sample_meta_map.update({k: k for k in sample_meta_field})
    if sequence_meta_field_map:
        sequence_meta_map.update(dict(sequence_meta_field_map))
    if sequence_meta_field:
        sequence_meta_map.update({k: k for k in sequence_meta_field})

    parser = GenericMetadataParser(
        sample_metadata_project=sample_metadata_project,
        sample_name_column=sample_name_column,
        participant_meta_map=participant_meta_map,
        sample_meta_map=sample_meta_map,
        sequence_meta_map=sequence_meta_map,
        qc_meta_map=qc_meta_map,
        reads_column=reads_column,
        gvcf_column=gvcf_column,
        default_sample_type=default_sample_type,
        default_sequence_type=default_sequence_type,
        search_locations=search_path,
    )
    for manifest in manifests:
        logger.info(f'Importing {manifest}')

        await parser.from_manifest_path(manifest=manifest, confirm=confirm)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
