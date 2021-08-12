# pylint: disable=too-many-instance-attributes,too-many-locals
import logging
import os
from io import StringIO
from typing import Dict, Optional

import click

from sample_metadata.models.sample_type import SampleType
from sample_metadata.models.sequence_status import SequenceStatus
from sample_metadata.models.sequence_type import SequenceType

from scripts.parser import GenericParser, GroupedRow


logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class Columns:
    """Column keys for VCGS manifest"""

    KCCG_ID = 'sample.id'
    SAMPLE_NAME = 'sample.sample_name'
    FLOWCELL_LANE = 'sample.flowcell_lane'
    LIBRARY_ID = 'sample.library_id'
    PLATFORM = 'sample.platform'
    CENTRE = 'sample.centre'
    REFERENCE_GENOME = 'sample.reference_genome'
    FREEMIX = 'raw_data.FREEMIX'
    PCT_CHIMERAS = 'raw_data.PCT_CHIMERAS'
    PERCENT_DUPLICATION = 'raw_data.PERCENT_DUPLICATION'
    MEDIAN_INSERT_SIZE = 'raw_data.MEDIAN_INSERT_SIZE'
    MEDIAN_COVERAGE = 'raw_data.MEDIAN_COVERAGE'
    BATCH_NAME = 'batch.batch_name'
    SAMPLE_COUNT = 'batch.sample_count'

    @staticmethod
    def sequence_columns():
        """Columns that will be put into sequence.meta"""
        return [
            Columns.FLOWCELL_LANE,
            Columns.LIBRARY_ID,
            Columns.PLATFORM,
            Columns.CENTRE,
            Columns.REFERENCE_GENOME,
            Columns.FREEMIX,
            Columns.PCT_CHIMERAS,
            Columns.PERCENT_DUPLICATION,
            Columns.MEDIAN_INSERT_SIZE,
            Columns.MEDIAN_COVERAGE,
        ]


class TobWgsParser(GenericParser):
    """Parser for TOBWgs manifest"""

    buckets = {}

    def get_gvcf_path(self, sample_id: str, batch_number: int) -> Optional[str]:
        """
        GVCFs can be in a few places, so please find it from search locations
        """
        filename = f'{sample_id}.g.vcf.gz'
        dirs = [
            'gs://cpg-tob-wgs-main-upload/',
            f'gs://cpg-tob-wgs-main/gvcf/batch{batch_number}/',
        ]
        for base in dirs:
            fpath = os.path.join(base, filename)
            if self.file_exists(fpath):
                return fpath

        return None

    def get_cram_path(self, sample_id: str, batch_number: int) -> Optional[str]:
        """
        CRAMs can be in a few places, so please find it from search locations
        """
        filename = f'{sample_id}.cram'
        dirs = [
            'gs://cpg-tob-wgs-main-upload/',
            f'gs://cpg-tob-wgs-archive/cram/batch{batch_number}/',
        ]
        for base in dirs:
            fpath = os.path.join(base, filename)
            if self.file_exists(fpath):
                return fpath

        return None

    def get_sample_id(self, row: Dict[str, any]):
        """Get external sample ID from row"""
        return row[Columns.SAMPLE_NAME]

    def get_sample_meta(self, sample_id: str, row: GroupedRow):
        """Get sample-metadata from row"""
        collapsed_sample_meta = {}

        return collapsed_sample_meta

    def get_sequence_meta(self, sample_id: str, row: GroupedRow):
        """Get sequence-metadata from row"""

        if isinstance(row, list):
            collapsed_sequence_meta = {
                col: ','.join(set(r[col] for r in row))
                for col in Columns.sequence_columns()
            }
        else:
            collapsed_sequence_meta = {
                col: row[col] for col in Columns.sequence_columns()
            }

        batch_number = int(row[Columns.BATCH_NAME][-3:])

        gvcf, variants_type = self.parse_file(
            [self.get_gvcf_path(sample_id, batch_number)]
        )
        reads, reads_type = self.parse_file(
            [self.get_cram_path(sample_id, batch_number)]
        )

        if len(gvcf) == 1:
            gvcf = gvcf[0]
        if len(reads) == 1:
            reads = reads[0]

        collapsed_sequence_meta['reads'] = reads
        collapsed_sequence_meta['reads_type'] = reads_type
        collapsed_sequence_meta['gvcf'] = gvcf
        collapsed_sequence_meta['gvcf_type'] = variants_type

        collapsed_sequence_meta['batch'] = batch_number

        return collapsed_sequence_meta

    def get_sequence_type(self, sample_id: str, row: GroupedRow) -> SequenceType:
        """
        Parse sequencing type (wgs / single-cell, etc)
        """
        # filter false-y values
        return self.default_sequence_type

    def get_sample_type(self, sample_id: str, row: GroupedRow) -> SampleType:
        """Get sample type"""
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
            manifest_filename = os.path.basename(manifest)
        else:
            manifest_filename = manifest

        parser = TobWgsParser(
            path_prefix,
            sample_metadata_project=sample_metadata_project,
            default_sequence_type=default_sequence_type,
            default_sample_type=default_sample_type,
            confirm=confirm,
        )
        file_contents = parser.file_contents(manifest_filename)
        resp = parser.parse_manifest(StringIO(file_contents))

        return resp


@click.command(help='GCS path to manifest file')
@click.option(
    '--sample-metadata-project',
    help='The sample-metadata project to import manifest into (probably "seqr")',
)
@click.option('--default-sample-type', default='blood')
@click.option('--default-sequence-type', default='wgs')
@click.option('--path-prefix', default=None)
@click.option(
    '--confirm', is_flag=True, help='Confirm with user input before updating server'
)
@click.argument('manifests', nargs=-1)
def main(
    manifests,
    sample_metadata_project,
    default_sample_type='blood',
    default_sequence_type='wgs',
    path_prefix=None,
    confirm=False,
):
    """Run script from CLI arguments"""

    for manifest in manifests:
        logger.info(f'Importing {manifest}')
        resp = TobWgsParser.from_manifest_path(
            manifest=manifest,
            sample_metadata_project=sample_metadata_project,
            default_sample_type=default_sample_type,
            default_sequence_type=default_sequence_type,
            path_prefix=path_prefix,
            confirm=confirm,
        )
        print(resp)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
