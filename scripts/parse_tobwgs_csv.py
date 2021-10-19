# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,no-self-use,wrong-import-order
from typing import Optional
import logging
import os
from io import StringIO

import click

from parse_generic_metadata import GenericMetadataParser, GroupedRow

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class TobWgsParser(GenericMetadataParser):
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

    def get_sequence_meta(self, sample_id: str, row: GroupedRow):
        """Get sequence-metadata from row"""
        collapsed_sequence_meta = super().get_sequence_meta(sample_id, row)
        batch_number = int(row['batch.batch_name'][-3:])
        collapsed_sequence_meta['batch'] = batch_number

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

        return collapsed_sequence_meta

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

        delimiter = TobWgsParser.guess_delimiter_from_filename(manifest_filename)

        sequence_map = {
            'sample.flowcell_lane': 'flowcell_lane',
            'sample.library_id': 'library_id',
            'sample.platform': 'platform',
            'sample.centre': 'centre',
            'sample.reference_genome': 'reference_genome',
            'raw_data.FREEMIX': 'freemix',
            'raw_data.PCT_CHIMERAS': 'pct_chimeras',
            'raw_data.PERCENT_DUPLICATION': 'percent_duplication',
            'raw_data.MEDIAN_INSERT_SIZE': 'median_insert_size',
            'raw_data.MEDIAN_COVERAGE': 'median_coverage',
            'batch.sample_count': 'sample_count',
        }

        parser = TobWgsParser(
            path_prefix,
            sample_metadata_project=sample_metadata_project,
            sequence_meta_map=sequence_map,
            sample_name_column='sample.sample_name',
            sample_meta_map={},
            qc_meta_map={},
            default_sequence_type=default_sequence_type,
            default_sample_type=default_sample_type,
            confirm=confirm,
        )

        file_contents = parser.file_contents(manifest_filename)
        resp = parser.parse_manifest(StringIO(file_contents), delimiter=delimiter)

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
