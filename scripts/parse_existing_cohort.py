# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,wrong-import-order
"""
Parser for existing-cohort metadata.

Usage:
parse_existing_cohort.py \
--project bioheart \
--search-location gs://cpg-bioheart-upload \
--batch-number M001 \
gs://cpg-bioheart-upload/R_220405_BINKAN1_BIOHEART_M001.csv


2022-10-19 vivbak:
This script will:
- Upsert Participants (currently assumes esid == epid)
- Upsert Samples
- Upsert WGS Sequences

This script does NOT:
- Create Projects
- Upsert Analysis Objects
- Upsert Pedigrees
- Upsert HPO terms

The fields from the CSV file that will be ingested are defined in the columns class.

On top of the GenericMetadataParser this script handles the extra lines found at the top of the
input CSV files that should be discarded.

Additionally, the reads-column is not provided for existing-cohort csvs.
This information is derived from the fluidX id pulled from the filename.

"""

import logging
import csv
from typing import List, Optional
import click

from sample_metadata.parser.generic_metadata_parser import (
    GenericMetadataParser,
    SingleRow,
    GroupedRow,
    run_as_sync,
)

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


COLUMN_MAP = {
    'Application': 'platform',
    'External ID': 'external_id',
    'Sample Concentration (ng/ul)': 'concentration',
    'Volume (uL)': 'volume',
    'Sex': 'sex',
    'Reference Genome': 'reference_genome',
    'Sample/Name': 'fluid_x_tube_id',
    'Participant ID': 'participant_id',
}


class Columns:
    """Column keys for manifest"""

    EXTERNAL_ID = 'External ID'
    PLATFORM = 'Application'
    CONCENTRATION = 'Sample Concentration (ng/ul)'
    VOLUME = 'Volume (uL)'
    SEX = 'Sex'
    MANIFEST_FLUID_X = 'Sample/Name'
    REFERENCE_GENOME = 'Reference Genome'
    PARTICIPANT_COLUMN = 'Participant ID'

    @staticmethod
    def sequence_meta_map():
        """Columns that will be put into sequence.meta"""
        fields = [
            Columns.PLATFORM,
            Columns.CONCENTRATION,
            Columns.VOLUME,
            Columns.MANIFEST_FLUID_X,
            Columns.REFERENCE_GENOME,
        ]
        return {k: COLUMN_MAP[k] for k in fields}


def fastq_file_name_to_sample_id(filename: str) -> str:
    """
    HG3FMDSX3_2_220208_FD02700641_Homo-sapiens_AACGAGGCCG-ATCCAGGTAT_R_220208_BINKAN1_PROPHECY_M002_R1.
    -> 220208_FD02700641
    """
    return '_'.join(filename.split('_')[2:4])


class ExistingCohortParser(GenericMetadataParser):
    """Parser for existing cohort manifests"""

    def __init__(
        self,
        project,
        search_locations,
        batch_number,
        include_participant_column,
    ):

        if include_participant_column:
            participant_column = Columns.PARTICIPANT_COLUMN
        else:
            participant_column = Columns.EXTERNAL_ID

        super().__init__(
            project=project,
            search_locations=search_locations,
            sample_name_column=Columns.EXTERNAL_ID,
            participant_column=participant_column,
            reported_gender_column=Columns.SEX,
            sample_meta_map={},
            qc_meta_map={},
            participant_meta_map={},
            sequence_meta_map=Columns.sequence_meta_map(),
            batch_number=batch_number,
        )

    def _get_dict_reader(self, file_pointer, delimiter: str):
        # Skipping header metadata lines
        for line in file_pointer:
            if line.strip() == '""':
                break
        reader = csv.DictReader(file_pointer, delimiter=delimiter)
        return reader

    async def get_read_filenames(
        self, sample_id: Optional[str], row: SingleRow
    ) -> List[str]:
        """
        We don't have fastq urls in a manifest, so overriding this method to take
        urls from a bucket listing.
        """

        return [
            path
            for filename, path in self.filename_map.items()
            if fastq_file_name_to_sample_id(filename) == row[Columns.MANIFEST_FLUID_X]
        ]

    def get_sequence_id(self, row: GroupedRow) -> Optional[dict[str, str]]:
        """Get external sequence ID from sequence file name"""
        for filename, _path in self.filename_map.items():
            if (
                fastq_file_name_to_sample_id(filename)
                == row[0][Columns.MANIFEST_FLUID_X]
            ):
                split_filename = filename.split('_')[0:4]
                external_sequence_id = '_'.join(split_filename)

        return {'kccg_id': external_sequence_id}


@click.command(help='GCS path to manifest file')
@click.option(
    '--project',
    help='The sample-metadata project to import manifest into',
)
@click.option('--search-location', 'search_locations', multiple=True)
@click.option(
    '--confirm', is_flag=True, help='Confirm with user input before updating server'
)
@click.option('--batch-number', 'batch_number')
@click.option('--dry-run', 'dry_run', is_flag=True)
@click.option(
    '--include-participant-column', 'include_participant_column', is_flag=True
)
@click.argument('manifests', nargs=-1)
@run_as_sync
async def main(
    manifests: List[str],
    project: str,
    search_locations: List[str],
    batch_number: Optional[str],
    confirm=True,
    dry_run=False,
    include_participant_column=False,
):
    """Run script from CLI arguments"""

    parser = ExistingCohortParser(
        project=project,
        search_locations=search_locations,
        batch_number=batch_number,
        include_participant_column=include_participant_column,
    )

    for manifest_path in manifests:
        logger.info(f'Importing {manifest_path}')

        await parser.from_manifest_path(
            manifest=manifest_path,
            confirm=confirm,
            dry_run=dry_run,
        )


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
