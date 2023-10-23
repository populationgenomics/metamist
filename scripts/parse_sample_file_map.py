#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,wrong-import-order,unused-argument
import logging
from typing import List

import click

from metamist.parser.generic_metadata_parser import run_as_sync
from metamist.parser.sample_file_map_parser import SampleFileMapParser

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
    '--project',
    help='The metamist project to import manifest into',
)
@click.option('--default-sample-type', default='blood')
@click.option('--default-sequencing-type', default='wgs')
@click.option('--default-sequence-technology', default='short-read')
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
    '--ref',
    help='Path to genome reference used for crams. Required when ingesting crams. ',
)
@click.option(
    '--dry-run', is_flag=True, help='Just prepare the run, without comitting it'
)
@click.option(
    '--allow-extra-files-in-search_path',
    is_flag=True,
    help='By default, this parser will fail if there are crams, bams, fastqs '
    'in the search path that are not covered by the sample map.',
)
@click.argument('manifests', nargs=-1)
@run_as_sync
async def main(
    manifests,
    search_path: List[str],
    project,
    default_sample_type='blood',
    default_sequencing_type='wgs',
    default_sequence_technology='short-read',
    confirm=False,
    dry_run=False,
    allow_extra_files_in_search_path=False,
    ref=None,
):
    """Run script from CLI arguments"""
    if not manifests:
        raise ValueError('Expected at least 1 manifest')

    extra_seach_paths = [m for m in manifests if m.startswith('gs://')]
    if extra_seach_paths:
        search_path = list(set(search_path).union(set(extra_seach_paths)))

    parser = SampleFileMapParser(
        project=project,
        default_sample_type=default_sample_type,
        default_sequencing_type=default_sequencing_type,
        default_sequencing_technology=default_sequence_technology,
        search_locations=search_path,
        allow_extra_files_in_search_path=allow_extra_files_in_search_path,
        default_reference_assembly_location=ref,
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
