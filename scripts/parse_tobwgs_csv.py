# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,wrong-import-order
import logging
from typing import Optional, List

import click

from sample_metadata.models import AnalysisType, AnalysisStatus
from sample_metadata.parser.generic_metadata_parser import (
    GenericMetadataParser,
    run_as_sync,
    ParsedSequencingGroup,
    ParsedAnalysis,
    ParsedSequencing,
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


# Labelling analysis outputs to distinguish from Nagim
SOURCE = 'kccg'


class TobWgsParser(GenericMetadataParser):
    """Parser for TOBWgs manifest"""

    def __init__(
        self,
        project: str,
        search_locations: List[str],
        default_sequence_type='wgs',
        default_sample_type='blood',
    ):
        super().__init__(
            search_locations=search_locations,
            project=project,
            default_sequence_type=default_sequence_type,
            default_sample_type=default_sample_type,
            sample_name_column='sample.sample_name',
            participant_meta_map={},
            sample_meta_map={},
            sequence_meta_map=SEQUENCE_MAP,
            qc_meta_map={},
        )

    async def find_gvcf(
        self, sample_id: str, cpg_id: Optional[str] = None
    ) -> Optional[str]:
        """
        Find GVCF for the sample.
        """
        extension = 'g.vcf.gz'

        search_locations = [f'gs://cpg-tob-wgs-main-upload/{sample_id}.{extension}']

        if cpg_id:
            # Sample was added before and CPG ID is known, so can search the locations
            # where downstream upload processor and pipelines might have moved files.
            search_locations += [
                # After GVCFs were processed with joint-calling, staging ones
                # are moved to the archive:
                f'gs://cpg-tob-wgs-main-archive/{SOURCE}/gvcf/staging/{cpg_id}.{extension}',
                # Upload processor moves GVCFs from upload to the main bucket
                # as "staging" for processing by joint-calling:
                f'gs://cpg-tob-wgs-main/gvcf/staging/{cpg_id}.{extension}',
            ]

        for fpath in search_locations:
            if await self.file_exists(fpath):
                return fpath

        return None

    async def find_cram(
        self, sample_id: str, cpg_id: Optional[str] = None
    ) -> Optional[str]:
        """
        Find CRAM for the sample.
        """
        extension = 'cram'

        search_locations = [f'gs://cpg-tob-wgs-main-upload/{sample_id}.{extension}']

        if cpg_id:
            # Sample was added before and CPG ID is known, so can search the locations
            # where downstream upload processor and pipelines might have moved files.
            search_locations += [
                # Upload processor moves CRAMs to the archive:
                f'gs://cpg-tob-wgs-main-archive/{SOURCE}/cram/{cpg_id}.{extension}',
            ]

        for fpath in search_locations:
            if await self.file_exists(fpath):
                return fpath

        return None

    async def get_analyses_from_sequence_group(
        self, sequence_group: ParsedSequencingGroup
    ) -> list[ParsedAnalysis]:

        """
        Get Analysis entries from a row.
        cpg_id is known for previously added samples.
        """
        analyses = await super().get_analyses_from_sequence_group(sequence_group)

        sample_id = sequence_group.sample.external_sid
        cpg_id = sequence_group.sample.internal_sid

        for analysis_type in ['gvcf', 'cram']:
            if analysis_type == 'gvcf':
                file_path = await self.find_gvcf(sample_id, cpg_id)
            else:
                file_path = await self.find_cram(sample_id, cpg_id)

            if not file_path:
                logger.warning(f'Not found {analysis_type} file for {sample_id}')
                continue

            analyses.append(
                ParsedAnalysis(
                    sequence_group=sequence_group,
                    type_=AnalysisType(analysis_type),
                    status=AnalysisStatus('completed'),
                    output=file_path,
                    meta={
                        # To distinguish TOB processed on Terra as part from NAGIM
                        # from those processed at the KCCG:
                        'source': SOURCE,
                        # Indicating that files need to be renamed to use CPG IDs,
                        # and moved from -upload to -test/-main. (For gvcf, also
                        # need to reblock):
                        'staging': True,
                    },
                    rows=[],
                )
            )
        return analyses

    async def get_sequences_from_group(
        self, sequence_group: ParsedSequencingGroup
    ) -> list[ParsedSequencing]:
        sequences = await super().get_sequences_from_group(sequence_group)

        for sequence in sequences:
            row = sequence.rows[0]
            sequence.meta['batch'] = int(row['batch.batch_name'][-3:])
            sequence.meta['batch_name'] = row['batch.batch_name'][:-5]

        return sequences


@click.command(help='GCS path to manifest file')
@click.option(
    '--project',
    help='The sample-metadata project to import manifest into (probably "seqr")',
)
@click.option('--default-sample-type', default='blood')
@click.option('--default-sequence-type', default='wgs')
@click.option('--path-prefix', default=None, multiple=True)
@click.option(
    '--confirm', is_flag=True, help='Confirm with user input before updating server'
)
@click.option('--dry-run', 'dry_run', is_flag=True)
@click.argument('manifests', nargs=-1)
@run_as_sync
async def main(
    manifests,
    project,
    default_sample_type='blood',
    default_sequence_type='wgs',
    path_prefix=None,
    confirm=False,
    dry_run=False,
):
    """Run script from CLI arguments"""
    _search_locations = path_prefix if isinstance(path_prefix, list) else [path_prefix]

    parser = TobWgsParser(
        default_sample_type=default_sample_type,
        default_sequence_type=default_sequence_type,
        project=project,
        search_locations=_search_locations,
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
