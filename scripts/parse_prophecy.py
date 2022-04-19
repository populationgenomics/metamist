# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,no-self-use,wrong-import-order
"""
Parser for prophecy metadata.

parse_prophecy.py \
gs://cpg-prophecy-test-upload/R_220208_BINKAN1_PROPHECY_M001.csv \
gs://cpg-prophecy-test-upload/R_220208_BINKAN1_PROPHECY_M002.csv \
--search-location gs://cpg-prophecy-test-upload
"""

import logging
import traceback
from typing import List, Dict, Any
import click

from sample_metadata.exceptions import ApiException
from sample_metadata.api.participant_api import ParticipantUpdateModel
from sample_metadata.api.sample_api import SampleApi
from sample_metadata.api.participant_api import ParticipantApi
from sample_metadata.parser.generic_metadata_parser import (
    GenericMetadataParser,
    GroupedRow,
    run_as_sync,
)

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

SEQUENCE_META_MAP = {
    'Application': 'platform',
    'External ID': 'external_id',
    'Sample Concentration (ng/ul)': 'concentration',
    'Volume (uL)': 'volume',
}

SAMPLE_META_MAP = {
    'Sex': 'sex',
}

PARTICIPANT_META_MAP = {
    'Sex': 'sex',
}

PROJECT = 'prophecy-test'


class ProphecyParser(GenericMetadataParser):
    """Parser for Prophecy manifests"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def file_pointer_to_sample_map(
        self,
        file_pointer,
        delimiter: str,
    ) -> Dict[str, List]:
        """Parse manifest into a dict"""

        # Skipping header metadata lines
        for line in file_pointer:
            if line.strip() == '""':
                break

        return await super().file_pointer_to_sample_map(file_pointer, delimiter)

    async def file_pointer_to_participant_map(
        self,
        file_pointer,
        delimiter: str,
    ) -> Dict[Any, Dict[Any, List[Any]]]:
        """Parse manifest into a dict"""

        # Skipping header metadata lines
        for line in file_pointer:
            if line.strip() == '""':
                break

        return await super().file_pointer_to_participant_map(file_pointer, delimiter)

    def get_participant_id(self, row) -> str:
        """Get external participant ID from row"""
        if not self.participant_column or self.participant_column not in row:
            raise ValueError('Participant column does not exist')
        return row[self.participant_column][:-4]

    def fastq_file_name_to_sample_id(self, filename: str) -> str:
        """
        HG3FMDSX3_2_220208_FD02700641_Homo-sapiens_AACGAGGCCG-ATCCAGGTAT_R_220208_BINKAN1_PROPHECY_M002_R1.
        -> 220208_FD02700641
        """
        return '_'.join(filename.split('_')[2:4])

    async def get_read_filenames(self, sample_id: str, row: GroupedRow) -> List[str]:
        """
        We don't have fastq urls in a manifest, so overriding this method to take
        urls from a bucket listing.
        """
        return [
            path
            for filename, path in self.filename_map.items()
            if self.fastq_file_name_to_sample_id(filename) == sample_id
        ]

    def has_participants(self, file_pointer, delimiter: str) -> bool:
        """Returns True if the file has a Participants column"""
        # Skipping header metadata lines
        for line in file_pointer:
            if line.strip() == '""':
                break

        return super().has_participants(file_pointer, delimiter)


@click.command(help='GCS path to manifest file')
@click.option(
    '--sample-metadata-project',
    help='The sample-metadata project to import manifest into',
    default=PROJECT,
)
@click.option('--search-location', 'search_locations', multiple=True)
@click.option(
    '--confirm', is_flag=True, help='Confirm with user input before updating server'
)
@click.option('--dry-run', 'dry_run', is_flag=True)
@click.argument('manifests', nargs=-1)
@run_as_sync
async def main(
    manifests: List[str],
    sample_metadata_project: str,
    search_locations: List[str],
    confirm=True,
    dry_run=False,
):
    """Run script from CLI arguments"""
    parser = ProphecyParser(
        sample_metadata_project=sample_metadata_project,
        search_locations=search_locations,
        sample_name_column='External ID',
        participant_column='External ID',
        participant_meta_map=PARTICIPANT_META_MAP,
        sample_meta_map=SAMPLE_META_MAP,
        qc_meta_map={},
        sequence_meta_map=SEQUENCE_META_MAP,
    )

    for manifest_path in manifests:
        logger.info(f'Importing {manifest_path}')

        await parser.from_manifest_path(
            manifest=manifest_path,
            confirm=confirm,
            dry_run=dry_run,
        )

    # add_participant_meta(sample_metadata_project)


def add_participant_meta(sample_metadata_project: str):
    """
    Fill in Participant entries. We don't have pedigree data, we only
    need to add the reported_gender field, similar to tob-wgs.
    """
    papi = ParticipantApi()
    papi.fill_in_missing_participants(sample_metadata_project)

    sapi = SampleApi()
    samples = sapi.get_samples(
        body_get_samples_by_criteria_api_v1_sample_post={
            'project_ids': [sample_metadata_project],
            'active': True,
        }
    )

    id_map = papi.get_participant_id_map_by_external_ids(
        sample_metadata_project, [s['external_id'] for s in samples]
    )

    for sample in samples:
        updated_participant = ParticipantUpdateModel()
        updated_participant['reported_gender'] = sample['meta'].get('sex')
        participant_id = id_map[sample['external_id']]
        try:
            papi.update_participant(
                participant_id=participant_id,
                participant_update_model=updated_participant,
            )
        except ApiException:
            traceback.print_exc()
            print(f'Error updating participant {participant_id}, skipping.')


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
