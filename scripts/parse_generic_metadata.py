#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,no-self-use,wrong-import-order,unused-argument,too-many-arguments
from typing import Dict, List, Optional

import logging
import os
from io import StringIO

import click

from parser import GenericParser, GroupedRow

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class GenericMetadataParser(GenericParser):
    """Parser for GenericMetadataParser"""

    def __init__(
        self,
        search_locations: List[str],
        sample_name_column: str,
        sample_meta_fields: List[str],
        sequence_meta_fields: List[str],
        sample_metadata_project: str,
        reads_column: Optional[str] = None,
        gvcf_column: Optional[str] = None,
        default_sequence_type='wgs',
        default_sample_type='blood',
        confirm=False,
        delimeter='\t',
    ):
        super().__init__(
            path_prefix=search_locations[0],
            sample_metadata_project=sample_metadata_project,
            default_sequence_type=default_sequence_type,
            default_sample_type=default_sample_type,
            delimeter=delimeter,
            confirm=confirm,
        )
        self.search_locations = search_locations
        self.filename_map = {}
        self.populate_filename_map(self.search_locations)

        if not sample_name_column:
            raise ValueError('A sample name column MUST be provided')

        self.sample_name_column = sample_name_column
        self.sample_meta_fields = sample_meta_fields or []
        self.sequence_meta_fields = sequence_meta_fields or []
        self.reads_column = reads_column
        self.gvcf_column = gvcf_column

    def populate_filename_map(self, search_locations: List[str]):
        """
        FileMapParser uses search locations based on the filename,
        so let's prepopulate that filename_map from the search_locations!
        """
        self.filename_map = {}
        for directory in search_locations:
            for file in self.list_directory(directory):
                file_base = os.path.basename(file)
                if file_base in self.filename_map:
                    logger.warning(
                        f'File "{file}" already exists in directory map: {self.filename_map[file_base]}'
                    )
                    continue
                self.filename_map[file_base] = file

    def file_path(self, filename: str) -> str:
        """
        Get complete filepath of filename:
        - Includes gs://{bucket} if relevant
        - Includes path_prefix decided early on
        """
        if filename in self.filename_map:
            return self.filename_map[filename]

        return super().file_path(filename)

    def get_sample_id(self, row: Dict[str, any]) -> str:
        """Get external sample ID from row"""
        external_id = row[self.sample_name_column]
        return external_id

    def get_sample_meta(self, sample_id: str, row: GroupedRow) -> Dict[str, any]:
        """Get sample-metadata from row"""
        if not self.sample_meta_fields:
            return {}

        if isinstance(row, list):
            return {
                col: ','.join(set(r[col] for r in row))
                for col in self.sample_meta_fields
            }

        return {key: row[key] for key in row}

    def get_sequence_meta(self, sample_id: str, row: GroupedRow) -> Dict[str, any]:
        """Get sequence-metadata from row"""
        collapsed_sequence_meta = {}

        read_filenames = []
        gvcf_filenames = []
        if isinstance(row, list):
            for r in row:
                if self.reads_column and self.reads_column in row:
                    read_filenames.extend(r[self.reads_column].split(','))
                if self.gvcf_column and self.gvcf_column in row:
                    gvcf_filenames.extend(r[self.gvcf_column].split(','))

            collapsed_sequence_meta = {
                col: ','.join(set(r[col] for r in row))
                for col in self.sequence_meta_fields
            }
        else:
            if self.reads_column and self.reads_column in row:
                read_filenames.extend(row[self.reads_column].split(','))
            if self.gvcf_column and self.gvcf_column in row:
                gvcf_filenames.extend(row[self.gvcf_column].split(','))

            collapsed_sequence_meta = {
                key: row[key] for key in self.sequence_meta_fields
            }

        # strip in case collaborator put "file1, file2"
        if read_filenames:
            full_filenames = [self.file_path(f.strip()) for f in read_filenames]
            reads, reads_type = self.parse_file(full_filenames)

            collapsed_sequence_meta['reads'] = reads
            collapsed_sequence_meta['reads_type'] = reads_type

        if gvcf_filenames:
            full_filenames = [self.file_path(f.strip()) for f in gvcf_filenames]
            gvcfs, gvcf_types = self.parse_file(full_filenames)

            collapsed_sequence_meta['gvcfs'] = gvcfs
            collapsed_sequence_meta['gvcf_types'] = gvcf_types

        return collapsed_sequence_meta

    def get_sequence_status(self, sample_id: str, row: GroupedRow) -> str:
        """Get sequence status from row"""
        return 'uploaded'

    @staticmethod
    def from_manifest_path(
        manifest: str,
        sample_metadata_project: str,
        sample_name_column: str,
        sample_meta_fields: List[str],
        sequence_meta_fields: List[str],
        reads_column: Optional[str] = None,
        gvcf_column: Optional[str] = None,
        default_sequence_type='wgs',
        default_sample_type='blood',
        search_paths=None,
        confirm=False,
        delimiter=',',
    ):
        """Parse manifest from path, and return result of parsing manifest"""
        parser = GenericMetadataParser(
            search_locations=search_paths,
            sample_metadata_project=sample_metadata_project,
            sample_name_column=sample_name_column,
            sample_meta_fields=sample_meta_fields,
            sequence_meta_fields=sequence_meta_fields,
            reads_column=reads_column,
            gvcf_column=gvcf_column,
            default_sequence_type=default_sequence_type,
            default_sample_type=default_sample_type,
            confirm=confirm,
        )

        file_contents = parser.file_contents(manifest)
        resp = parser.parse_manifest(StringIO(file_contents), delimiter=delimiter)

        return resp


@click.command(help='Parse manifest files')
@click.option(
    '--sample-metadata-project',
    help='The sample-metadata project to import manifest into (probably "seqr")',
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
@click.option('--sample-meta-fields', multiple=True)
@click.option('--sequence-meta-fields', multiple=True)
@click.option('--default-sample-type', default='blood')
@click.option('--default-sequence-type', default='wgs')
@click.option(
    '--confirm', is_flag=True, help='Confirm with user input before updating server'
)
@click.option('--search-path', multiple=True, required=True)
@click.argument('manifests', nargs=-1)
def main(
    manifests,
    search_path: List[str],
    sample_metadata_project,
    sample_name_column: str,
    sample_meta_fields: List[str],
    sequence_meta_fields: List[str],
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

    extension_to_delimeter = {'.csv': ',', '.tsv': '\t'}
    invalid_manifests = [
        manifest
        for manifest in manifests
        if not any(manifest.endswith(ext) for ext in extension_to_delimeter)
    ]
    if len(invalid_manifests):
        raise ValueError(
            'Unrecognised extensions on manifests: ' + ', '.join(invalid_manifests)
        )

    for manifest in manifests:
        logger.info(f'Importing {manifest}')
        delimiter = extension_to_delimeter[os.path.splitext(manifest)[1]]

        resp = GenericMetadataParser.from_manifest_path(
            manifest=manifest,
            sample_metadata_project=sample_metadata_project,
            sample_name_column=sample_name_column,
            sample_meta_fields=sample_meta_fields,
            sequence_meta_fields=sequence_meta_fields,
            reads_column=reads_column,
            gvcf_column=gvcf_column,
            default_sample_type=default_sample_type,
            default_sequence_type=default_sequence_type,
            delimiter=delimiter,
            confirm=confirm,
            search_paths=search_path,
        )
        print(resp)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
