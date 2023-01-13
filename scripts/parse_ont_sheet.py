#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,wrong-import-order,unused-argument
from typing import List
import logging

import click

from sample_metadata.parser.generic_metadata_parser import (
    run_as_sync,
    GenericMetadataParser,
)

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class Columns:
    """Columns for ONT data sheet from Garvan"""

    SEQUENCING_DATE = 'Sequencing_date'
    EXPERIMENT_NAME = 'Experiment name'
    SAMPLE_ID = 'Sample ID'
    PROTOCOL = 'Protocol'
    FLOW_CELL = 'Flow cell'
    BARCODING = 'Barcoding'
    DEVICE = 'Device'
    FLOWCELL_ID = 'Flowcell ID'
    MUX_TOTAL = 'MUX total'
    BASECALLING = 'Basecalling'
    FAIL_FASTQ_FILENAME = 'Fail FASTQ filename'
    PASS_FASTQ_FILENAME = 'Pass FASTQ filename'


class OntParser(GenericMetadataParser):
    """Parser for ONT sheet data - provided by Garvan"""

    def __init__(
        self,
        search_locations: List[str],
        project: str,
        default_sequence_type='genome',
        default_sequence_technology='long-reads',
        default_sample_type='blood',
        allow_extra_files_in_search_path=False,
    ):
        sequence_meta_map = {
            Columns.SEQUENCING_DATE: Columns.SEQUENCING_DATE,
            Columns.EXPERIMENT_NAME: Columns.EXPERIMENT_NAME,
            Columns.PROTOCOL: Columns.PROTOCOL,
            Columns.FLOW_CELL: Columns.FLOW_CELL,
            Columns.BARCODING: Columns.BARCODING,
            Columns.DEVICE: Columns.DEVICE,
            Columns.FLOWCELL_ID: Columns.FLOWCELL_ID,
            Columns.MUX_TOTAL: Columns.MUX_TOTAL,
            Columns.BASECALLING: Columns.BASECALLING,
        }

        sequence_meta_map = {
            k: v.lower().replace(' ', '_') for k, v in sequence_meta_map.items()
        }

        super().__init__(
            search_locations=search_locations,
            project=project,
            participant_column=Columns.SAMPLE_ID,
            sample_name_column=Columns.SAMPLE_ID,
            reads_column=Columns.PASS_FASTQ_FILENAME,
            default_sequence_type=default_sequence_type,
            default_sample_type=default_sample_type,
            default_sequence_technology=default_sequence_technology,
            participant_meta_map={},
            sample_meta_map={},
            sequence_meta_map=sequence_meta_map,
            qc_meta_map={},
            allow_extra_files_in_search_path=allow_extra_files_in_search_path,
        )

    async def get_all_files_from_row(self, sample_id: str, row):
        """Override get all files to include FAIL_FASTQ_FILENAME"""
        fns = []
        for r in row if isinstance(row, list) else [row]:
            fns.extend(await self.get_read_filenames(sample_id, r))
            fns.append(r[Columns.FAIL_FASTQ_FILENAME])

        return fns

    @staticmethod
    def parse_fastqs_structure(fastqs) -> List[List[str]]:
        """
        In ONT, there is no pairs of fastqs as we don't
        have to read forwards and backwards :D
        """
        return [fastqs]

    async def get_sequence_meta(
        self,
        seq_group: SequenceMetaGroup,
        sample_id: str | None = None,
    ) -> SequenceMetaGroup:
        """
        Get sequence meta, override to include formed failed_fastqs
        """
        seq_group = await super().get_sequence_meta(seq_group, sample_id)

        failed_fastqs: list[str] = []

        for r in seq_group.rows:
            parsed_failed_fastqs = await self.parse_files(
                sample_id, r[Columns.FAIL_FASTQ_FILENAME]
            )
            if 'reads' not in parsed_failed_fastqs:
                raise ValueError(
                    f'Could not find "reads" key in parsed failed fastqs: {parsed_failed_fastqs}'
                )
            parsed_failed_fastq_reads = parsed_failed_fastqs['reads']
            if (
                len(parsed_failed_fastq_reads) != 1
                or 'fastq' not in parsed_failed_fastq_reads
            ):
                raise ValueError(
                    f'Failed to parse ONT failed fastqs, expected 1 key "fastq": {parsed_failed_fastq_reads}'
                )
            failed_fastqs.extend(parsed_failed_fastq_reads['fastq'])

        seq_group.meta['failed_reads'] = failed_fastqs

        return seq_group


@click.command()
@click.option(
    '--project',
    help='The sample-metadata project to import manifest into',
)
@click.option('--default-sample-type', default='blood')
@click.option('--default-sequence-type', default='ont')
@click.option(
    '--confirm', is_flag=True, help='Confirm with user input before updating server'
)
@click.option(
    '--search-path',
    multiple=True,
    required=True,
    help='Search path to search for files within',
)
@click.option(
    '--dry-run', is_flag=True, help='Just prepare the run, without comitting it'
)
@click.option(
    '--allow-extra-files-in-search_path',
    is_flag=True,
    help='By default, this parser will fail if there are crams, bams, fastqs '
    'in the search path that are not covered by the sample map.',
)
@click.argument('manifests', nargs=-1)
@run_as_sync
async def main(
    manifests,
    search_path: List[str],
    project: str,
    default_sample_type='blood',
    default_sequence_type='wgs',
    confirm=False,
    dry_run=False,
    allow_extra_files_in_search_path=False,
):
    """Run script from CLI arguments"""
    if not manifests:
        raise ValueError('Expected at least 1 manifest')

    extra_seach_paths = [m for m in manifests if m.startswith('gs://')]
    if extra_seach_paths:
        search_path = list(set(search_path).union(set(extra_seach_paths)))

    parser = OntParser(
        project=project,
        default_sample_type=default_sample_type,
        default_sequence_type=default_sequence_type,
        search_locations=search_path,
        allow_extra_files_in_search_path=allow_extra_files_in_search_path,
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
