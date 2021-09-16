#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,no-self-use,wrong-import-order,unused-argument

from typing import List
import logging

import click

from parse_generic_metadata import GenericMetadataParser

SAMPLE_ID_COL_NAME = 'Individual ID'
READS_COL_NAME = 'Filenames'

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


class SampleFileMapParser(GenericMetadataParser):
    """Parser for SampleFileMap"""

    def __init__(
        self,
        search_locations: List[str],
        sample_metadata_project: str,
        default_sequence_type='wgs',
        default_sample_type='blood',
        confirm=False,
    ):
        super().__init__(
            search_locations=search_locations,
            sample_metadata_project=sample_metadata_project,
            sample_name_column=SAMPLE_ID_COL_NAME,
            reads_column=READS_COL_NAME,
            default_sequence_type=default_sequence_type,
            default_sample_type=default_sample_type,
            sample_meta_map={},
            sequence_meta_map={},
            qc_meta_map={},
            confirm=confirm,
        )

    @staticmethod
    def from_manifest_path(
        manifest: str,
        sample_metadata_project: str,
        default_sequence_type='wgs',
        default_sample_type='blood',
        search_paths=None,
        confirm=False,
    ):
        """From manifest path, same defaults in __init__"""
        super().from_manifest_path(
            manifest=manifest,
            search_paths=search_paths,
            sample_metadata_project=sample_metadata_project,
            sample_name_column=SAMPLE_ID_COL_NAME,
            reads_column=READS_COL_NAME,
            default_sequence_type=default_sequence_type,
            default_sample_type=default_sample_type,
            sample_meta_map={},
            sequence_meta_map={},
            qc_meta_map={},
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
@click.argument('manifests', nargs=-1)
def main(
    manifests,
    search_path: List[str],
    sample_metadata_project,
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

    for manifest in manifests:
        logger.info(f'Importing {manifest}')
        resp = SampleFileMapParser.from_manifest_path(
            manifest=manifest,
            sample_metadata_project=sample_metadata_project,
            default_sample_type=default_sample_type,
            default_sequence_type=default_sequence_type,
            confirm=confirm,
            search_paths=search_path,
        )
        print(resp)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
