#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,no-self-use,wrong-import-order,unused-argument
from typing import List
import logging

import click

from sample_metadata.parser.generic_parser import run_as_sync
from sample_metadata.parser.generic_metadata_parser import GenericMetadataParser

INDIVIDUAL_ID_COL_NAME = 'Individual ID'
SAMPLE_ID_COL_NAME = 'Sample ID'
READS_COL_NAME = 'Filenames'
TYPE_COL_NAME = 'Type'

__DOC = """
The SampleFileMapParser is used for parsing files with format:

- ['Individual ID']
- 'Sample ID'
- 'Filenames'
- ['Type']

EG:
    Sample ID   Filenames
    <sample-id>     <sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz
    # OR
    <sample-id2>    <sample-id2>.filename-R1.fastq.gz
    <sample-id2>    <sample-id2>.filename-R2.fastq.gz

This format is useful for ingesting filenames for the seqr loading pipeline
"""

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class SampleFileMapParser(GenericMetadataParser):
    """Parser for SampleFileMap"""

    def __init__(
        self,
        search_locations: List[str],
        sample_metadata_project: str,
        default_sequence_type='genome',
        default_sample_type='blood',
        allow_extra_files_in_search_path=False,
    ):
        super().__init__(
            search_locations=search_locations,
            sample_metadata_project=sample_metadata_project,
            individual_column=INDIVIDUAL_ID_COL_NAME,
            sample_name_column=SAMPLE_ID_COL_NAME,
            reads_column=READS_COL_NAME,
            seq_type_column=TYPE_COL_NAME,
            default_sequence_type=default_sequence_type,
            default_sample_type=default_sample_type,
            sample_meta_map={},
            sequence_meta_map={},
            qc_meta_map={},
            allow_extra_files_in_search_path=allow_extra_files_in_search_path,
        )


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
    sample_metadata_project,
    default_sample_type='blood',
    default_sequence_type='wgs',
    confirm=False,
    dry_run=False,
    allow_extra_files_in_search_path=False,
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
        allow_extra_files_in_search_path=allow_extra_files_in_search_path,
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
