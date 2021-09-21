# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,no-self-use,wrong-import-order
import logging
import os
import re
from io import StringIO
from typing import Dict

import click

from parse_generic_metadata import GenericMetadataParser, GroupedRow

rmatch = re.compile(r'_[Rr]\d')

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class Columns:
    """Column keys for VCGS manifest"""

    PHS_ACCESSION = 'phs_accession'
    SAMPLE_NAME = 'sample_name'
    LIBRARY_ID = 'library_ID'
    TITLE = 'title'
    LIBRARY_STRATEGY = 'library_strategy'
    LIBRARY_SOURCE = 'library_source'
    LIBRARY_SELECTION = 'library_selection'
    LIBRARY_LAYOUT = 'library_layout'
    PLATFORM = 'platform'
    INSTRUMENT_MODEL = 'instrument_model'
    DESIGN_DESCRIPTION = 'design_description'
    REFERENCE_GENOME_ASSEMBLY = 'reference_genome_assembly'
    ALIGNMENT_SOFTWARE = 'alignment_software'
    FORWARD_READ_LENGTH = 'forward_read_length'
    REVERSE_READ_LENGTH = 'reverse_read_length'
    FILETYPE = 'filetype'
    FILENAME = 'filename'

    @staticmethod
    def sequence_columns():
        """Columns that will be put into sequence.meta"""
        return [
            Columns.LIBRARY_ID,
            Columns.LIBRARY_STRATEGY,
            Columns.LIBRARY_SOURCE,
            Columns.LIBRARY_SELECTION,
            Columns.LIBRARY_LAYOUT,
            Columns.PLATFORM,
            Columns.INSTRUMENT_MODEL,
            Columns.DESIGN_DESCRIPTION,
            Columns.FORWARD_READ_LENGTH,
            Columns.REVERSE_READ_LENGTH,
        ]

    @staticmethod
    def sample_columns():
        """Columns that will be put into sample.meta"""
        return [
            Columns.PHS_ACCESSION,
        ]


class VcgsManifestParser(GenericMetadataParser):
    """Parser for VCGS manifest"""

    def get_sample_id(self, row: Dict[str, any]) -> str:
        """Get external sample ID from row"""
        external_id = row[self.sample_name_column]
        if '-' in external_id:
            external_id = external_id.split('-')[0]
        return external_id

    def get_sequence_type(self, sample_id: str, row: GroupedRow) -> str:
        """
        Parse sequencing type (wgs / single-cell, etc)
        """
        if not isinstance(row, list):
            row = [row]

        types = list(
            set(r[Columns.LIBRARY_STRATEGY] for r in row if r[Columns.LIBRARY_STRATEGY])
        )
        if len(types) <= 0:
            if (
                self.default_sequence_type is None
                or self.default_sequence_type.lower() == 'none'
            ):
                raise ValueError(
                    f"Couldn't detect sequence type for sample {sample_id}, and "
                    'no default was available.'
                )
            return self.default_sequence_type
        if len(types) > 1:
            raise ValueError(
                f'Multiple library types for same sample {sample_id}, '
                f'maybe there are multiples types of sequencing in the same '
                f'manifest? If so, please raise an issue with mfranklin to '
                f'change the groupby to include {Columns.LIBRARY_STRATEGY}.'
            )

        type_ = types[0].lower()
        if type_ == 'wgs':
            return 'wgs'
        if type_ in ('single-cell', 'ss'):
            return 'single-cell'

        raise ValueError(f'Unrecognised sequencing type {type_}')

    @staticmethod
    def from_manifest_path(
        manifest: str,
        sample_metadata_project: str,
        default_sequence_type='wgs',
        default_sample_type='blood',
        path_prefix=None,
        confirm=False,
    ):
        """Parse manifest from path, and return result of parsing manifest"""
        if path_prefix is None:
            path_prefix = os.path.dirname(manifest)

        sequence_meta_map = {
            'library_ID': 'library_ID',
            'library_strategy': 'library_strategy',
            'library_source': 'library_source',
            'library_selection': 'library_selection',
            'library_layout': 'library_layout',
            'platform': 'platform',
            'instrument_model': 'instrument_model',
            'design_description': 'design_description',
            'reference_genome_assembly': 'reference_genome_assembly',
            'alignment_software': 'alignment_software',
            'forward_read_length': 'forward_read_length',
            'reverse_read_length': 'reverse_read_length',
        }

        sample_meta_map = {
            'phs_accession': 'phs_accession',
        }

        parser = VcgsManifestParser(
            search_locations=[path_prefix],
            sample_metadata_project=sample_metadata_project,
            sample_name_column='filename',
            sample_meta_map=sample_meta_map,
            sequence_meta_map=sequence_meta_map,
            qc_meta_map={},
            default_sequence_type=default_sequence_type,
            default_sample_type=default_sample_type,
            confirm=confirm,
        )

        delimiter = VcgsManifestParser.guess_delimiter_from_filename(manifest)

        file_contents = parser.file_contents(manifest)
        resp = parser.parse_manifest(StringIO(file_contents), delimiter=delimiter)

        return resp


@click.command(help='GCS path to manifest file')
@click.option(
    '--sample-metadata-project',
    help='The sample-metadata project to import manifest into (probably "seqr")',
)
@click.option('--default-sample-type', default='blood')
@click.option('--default-sequence-type', default='wgs')
@click.option(
    '--confirm', is_flag=True, help='Confirm with user input before updating server'
)
@click.argument('manifests', nargs=-1)
def main(
    manifests,
    sample_metadata_project,
    default_sample_type='blood',
    default_sequence_type='wgs',
    confirm=False,
):
    """Run script from CLI arguments"""

    for manifest in manifests:
        logger.info(f'Importing {manifest}')
        resp = VcgsManifestParser.from_manifest_path(
            manifest=manifest,
            sample_metadata_project=sample_metadata_project,
            default_sample_type=default_sample_type,
            default_sequence_type=default_sequence_type,
            confirm=confirm,
        )
        print(resp)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
