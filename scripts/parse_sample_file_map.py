#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,no-self-use,wrong-import-order,unused-argument
from typing import List
import logging

import click

from sample_metadata.parser.generic_parser import run_as_sync
from sample_metadata.parser.sample_file_map_parser import SampleFileMapParser

__DOC = """
The SampleFileMapParser is used for parsing files with format:

- 'Individual ID'
- 'Filenames'

EG:
    Individual ID   Filenames
    <sample-id>     <sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz
    # OR
    <sample-id2>    <sample-id2>.filename-R1.fastq.gz
    <sample-id2>    <sample-id2>.filename-R2.fastq.gz

This format is useful for ingesting filenames for the seqr loading pipeline
"""

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


@click.command(help=__DOC)
@click.option(
    '--sample-metadata-project',
    help='The sample-metadata project to import manifest into',
)
@click.option('--default-sample-type', default='blood')
@click.option('--default-sequence-type', default='wgs')
@click.option(
    '--confirm', is_flag=True, help='Confirm with user input before updating server'
)
@click.option(
    '--search-path',
    multiple=True,
    required=True,
    help='Search path to search for files within',
)
@click.option(
    '--dry-run', is_flag=True, help='Just prepare the run, without comitting it'
)
@click.argument('manifests', nargs=-1)
@run_as_sync
async def main(
    manifests,
    search_path: List[str],
    sample_metadata_project,
    default_sample_type='blood',
    default_sequence_type='wgs',
    confirm=False,
    dry_run=False,
):
    """Run script from CLI arguments"""
    if not manifests:
        raise ValueError('Expected at least 1 manifest')

    extra_seach_paths = [m for m in manifests if m.startswith('gs://')]
    if extra_seach_paths:
        search_path = list(set(search_path).union(set(extra_seach_paths)))

    parser = SampleFileMapParser(
        sample_metadata_project=sample_metadata_project,
        default_sample_type=default_sample_type,
        default_sequence_type=default_sequence_type,
        search_locations=search_path,
    )
    for manifest in manifests:
        logger.info(f'Importing {manifest}')
        resp = await parser.from_manifest_path(
            manifest=manifest,
            confirm=confirm,
            dry_run=dry_run,
        )
        print(resp)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
