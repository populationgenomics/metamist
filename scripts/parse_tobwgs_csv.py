# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,no-self-use,wrong-import-order
import logging
import os
from typing import Optional, Union, List, Dict, Any

import click

from sample_metadata.models import AnalysisType, AnalysisStatus, AnalysisModel
from sample_metadata.parser.generic_metadata_parser import (
    GenericMetadataParser,
    GroupedRow,
)

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

SEQUENCE_MAP = {
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


class TobWgsParser(GenericMetadataParser):
    """Parser for TOBWgs manifest"""

    def __init__(
        self,
        sample_metadata_project: str,
        search_locations: List[str],
        default_sequence_type='wgs',
        default_sample_type='blood',
    ):
        super().__init__(
            search_locations=search_locations,
            sample_metadata_project=sample_metadata_project,
            default_sequence_type=default_sequence_type,
            default_sample_type=default_sample_type,
            sample_name_column='sample.sample_name',
            sample_meta_map={},
            sequence_meta_map=SEQUENCE_MAP,
            qc_meta_map={},
        )

    def find_file(
        self, sample_id: str, batch_number: int, analysis_type: str
    ) -> Optional[str]:
        """
        GVCFs and CRAMs can be in a few places, so please find it from search locations
        """
        assert analysis_type in ['cram', 'gvcf']
        extention = 'g.vcf.gz' if analysis_type == 'gvcf' else 'cram'
        filename = f'{sample_id}.{extention}'
        dirs = [
            'gs://cpg-tob-wgs-main-upload/',
            f'gs://cpg-tob-wgs-archive/{analysis_type}/batch{batch_number}/',
        ]
        for base in dirs:
            fpath = os.path.join(base, filename)
            if self.file_exists(fpath):
                return fpath

        return None

    def get_analyses(self, sample_id: str, row: Dict[str, any]) -> List[AnalysisModel]:
        """Get Analysis entries from a row"""
        analyses = super().get_analyses(sample_id, row)

        batch_number = int(row['batch.batch_name'][-3:])

        for atype in ['gvcf', 'cram']:
            file_path, file_type = self.parse_file(
                [self.find_file(sample_id, batch_number, atype)]
            )
            assert file_type == atype, (file_type, atype, sample_id)

            if not isinstance(file_path, str):
                file_path = file_path[0]

            analyses.append(
                AnalysisModel(
                    sample_ids=['<none>'],
                    type=AnalysisType(atype),
                    status=AnalysisStatus('completed'),
                    output=file_path,
                    meta={
                        # To distinguish TOB processed on Terra as part from NAGIM
                        # from those processed at the KCCG:
                        'source': 'kccg',
                        # Indicating that files need to be renamed to use CPG IDs,
                        # and moved from -upload to -test/-main. (For gvcf, also
                        # need to reblock):
                        'staging': True,
                    },
                )
            )
        return analyses

    def get_sequence_meta(self, sample_id: str, row: GroupedRow):
        """Get sequence-metadata from row"""
        assert not isinstance(row, list)
        collapsed_sequence_meta = super().get_sequence_meta(sample_id, row)
        batch_number = int(row['batch.batch_name'][-3:])
        collapsed_sequence_meta['batch'] = batch_number

        batch_name = row['batch.batch_name'][:-5]
        collapsed_sequence_meta['batch_name'] = batch_name

        return collapsed_sequence_meta


@click.command(help='GCS path to manifest file')
@click.option(
    '--sample-metadata-project',
    help='The sample-metadata project to import manifest into (probably "seqr")',
)
@click.option('--default-sample-type', default='blood')
@click.option('--default-sequence-type', default='wgs')
@click.option('--path-prefix', default=None, multiple=True)
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
    _search_locations = path_prefix if isinstance(path_prefix, list) else [path_prefix]

    parser = TobWgsParser(
        default_sample_type=default_sample_type,
        default_sequence_type=default_sequence_type,
        sample_metadata_project=sample_metadata_project,
        search_locations=_search_locations,
    )
    for manifest in manifests:
        logger.info(f'Importing {manifest}')
        resp = parser.from_manifest_path(
            manifest=manifest,
            confirm=confirm,
        )
        print(resp)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
