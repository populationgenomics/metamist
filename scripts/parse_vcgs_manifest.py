# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,no-self-use,wrong-import-order
import logging
import os
import re
from io import StringIO
from typing import Dict

import click

from sample_metadata.model.sample_type import SampleType
from sample_metadata.model.sequence_status import SequenceStatus
from sample_metadata.model.sequence_type import SequenceType

from parser import GenericParser, GroupedRow

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


class VcgsManifestParser(GenericParser):
    """Parser for VCGS manifest"""

    def __init__(
        self,
        path_prefix: str,
        sample_metadata_project: str,
        default_sequence_type='wgs',
        default_sample_type='blood',
        confirm=False,
        delimeter='\t',
    ):
        super().__init__(
            path_prefix=path_prefix,
            sample_metadata_project=sample_metadata_project,
            default_sequence_type=default_sequence_type,
            default_sample_type=default_sample_type,
            delimeter=delimeter,
            confirm=confirm,
        )

    def get_sample_id(self, row: Dict[str, any]):
        """Get external sample ID from row"""
        external_id = row[Columns.SAMPLE_NAME]
        if '-' in external_id:
            external_id = external_id.split('-')[0]
        return external_id

    def get_sample_meta(self, sample_id: str, row: GroupedRow):
        """Get sample-metadata from row"""
        if isinstance(row, list):
            collapsed_sample_meta = {
                col: ','.join(set(r[col] for r in row))
                for col in Columns.sample_columns()
            }
            filenames = [self.file_path(r[Columns.FILENAME]) for r in row]
            reads, reads_type = self.parse_file(filenames)
        else:
            collapsed_sample_meta = {col: row[col] for col in Columns.sample_columns()}
            reads, reads_type = self.parse_file([self.file_path(row[Columns.FILENAME])])

        collapsed_sample_meta['reads'] = reads
        collapsed_sample_meta['reads_type'] = reads_type

        return collapsed_sample_meta

    def get_sequence_meta(self, sample_id: str, row: GroupedRow):
        """Get sequence-metadata from row"""
        if isinstance(row, list):
            return {
                col: ','.join(set(r[col] for r in row))
                for col in Columns.sequence_columns()
            }
        return {col: row[col] for col in Columns.sequence_columns()}

    def get_sequence_type(self, sample_id: str, row: GroupedRow) -> SequenceType:
        """
        Parse sequencing type (wgs / single-cell, etc)
        """
        # filter false-y values
        if not isinstance(row, list):
            return SequenceType(row[Columns.LIBRARY_STRATEGY])

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
            return SequenceType('wgs')
        if type_ in ('single-cell', 'ss'):
            return SequenceType('single-cell')

        raise ValueError(f'Unrecognised sequencing type {type_}')

    def get_sample_type(self, sample_id: str, row: GroupedRow) -> SampleType:
        """Get sample type from row"""
        return self.default_sample_type

    def get_sequence_status(self, sample_id: str, row: GroupedRow) -> SequenceStatus:
        """Get sequence status from row"""
        return SequenceStatus('uploaded')

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

        parser = VcgsManifestParser(
            path_prefix,
            sample_metadata_project=sample_metadata_project,
            default_sequence_type=default_sequence_type,
            default_sample_type=default_sample_type,
            confirm=confirm,
        )

        file_contents = parser.file_contents(manifest)
        resp = parser.parse_manifest(StringIO(file_contents))

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
