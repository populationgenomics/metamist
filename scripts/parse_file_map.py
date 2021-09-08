# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,no-self-use,wrong-import-order,unused-argument
import logging
import os
from io import StringIO
from parser import GenericParser, GroupedRow
from typing import Dict, List

import click

from sample_metadata.models.sequence_status import SequenceStatus

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class Columns:
    """Column keys for file_map"""

    INDIVIDUAL_ID = 'Individual ID'
    FILENAMES = 'Filenames'


class SampleMapParser(GenericParser):
    """Parser for SampleMap"""

    def __init__(
        self,
        search_locations: List[str],
        sample_metadata_project: str,
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

    def populate_filename_map(self, search_locations: List[str]):
        """
        ParseFileMap uses search locations based on the filename,
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

    def get_sample_id(self, row: Dict[str, any]):
        """Get external sample ID from row"""
        external_id = row[Columns.INDIVIDUAL_ID]
        return external_id

    def get_sample_meta(self, sample_id: str, row: GroupedRow):
        """Get sample-metadata from row"""
        return {}

    def get_sequence_meta(self, sample_id: str, row: GroupedRow):
        """Get sequence-metadata from row"""
        collapsed_sample_meta = {}
        if isinstance(row, list):
            filenames = []
            for r in row:
                filenames.extend(r[Columns.FILENAMES].split(','))
        else:
            filenames = row[Columns.FILENAMES].split(',')
        # strip in case collaborator put "file1, file2"
        full_filenames = [self.file_path(f.strip()) for f in filenames]
        reads, reads_type = self.parse_file(full_filenames)

        collapsed_sample_meta['reads'] = reads
        collapsed_sample_meta['reads_type'] = reads_type

        return collapsed_sample_meta

    def get_sequence_status(self, sample_id: str, row: GroupedRow) -> SequenceStatus:
        """Get sequence status from row"""
        return SequenceStatus('uploaded')

    @staticmethod
    def from_manifest_path(
        manifest: str,
        sample_metadata_project: str,
        default_sequence_type='wgs',
        default_sample_type='blood',
        search_paths=None,
        confirm=False,
    ):
        """Parse manifest from path, and return result of parsing manifest"""
        parser = SampleMapParser(
            search_locations=search_paths,
            sample_metadata_project=sample_metadata_project,
            default_sequence_type=default_sequence_type,
            default_sample_type=default_sample_type,
            confirm=confirm,
        )

        file_contents = parser.file_contents(manifest)
        resp = parser.parse_manifest(StringIO(file_contents))

        return resp


@click.command(help='Parse manifest files')
@click.option(
    '--sample-metadata-project',
    help='The sample-metadata project to import manifest into (probably "seqr")',
)
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
    default_sample_type='blood',
    default_sequence_type='wgs',
    confirm=False,
):
    """Run script from CLI arguments"""
    if not manifests:
        raise ValueError('Expected at least 1 manifest')
    for manifest in manifests:
        logger.info(f'Importing {manifest}')
        resp = SampleMapParser.from_manifest_path(
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
