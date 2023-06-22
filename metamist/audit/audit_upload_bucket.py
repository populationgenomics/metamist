#!/usr/bin/env python
"""
This auditor looks for sequence files in the main upload bucket that can be deleted or ingested,
as well as samples that have no completed cram.

Procedure:
1. Input a Metamist dataset, sequence types to audit, and file types to search for in the bucket
2. Get all the participants, samples, and sequences for this dataset from Metamist
3. Search the upload bucket for all sequence files, and compare to the files in the metamist sequences
    - If there are discrepencies, check if the file name and size is the same in the bucket as it is in metamist
      - If the name and file size are the same, assume this file has just been moved around in the bucket
4. Check if a sample has a completed cram - if so, its sequence data can be removed
5. Any remaining sequence data in the bucket might require ingestion
6. Create reports in the audit_results folder of the upload bucket, containing sequence files to delete,
   ingest, and any samples without completed crams.
"""


from datetime import datetime
import sys
import logging
import os
import asyncio
import click

from cpg_utils.config import get_config

from metamist.audit.generic_auditor import GenericAuditor

FASTQ_EXTENSIONS = ('.fq.gz', '.fastq.gz', '.fq', '.fastq')
BAM_EXTENSIONS = ('.bam',)
CRAM_EXTENSIONS = ('.cram',)
READ_EXTENSIONS = FASTQ_EXTENSIONS + BAM_EXTENSIONS + CRAM_EXTENSIONS
GVCF_EXTENSIONS = ('.g.vcf.gz',)
VCF_EXTENSIONS = ('.vcf', '.vcf.gz')
ALL_EXTENSIONS = (
    FASTQ_EXTENSIONS
    + BAM_EXTENSIONS
    + CRAM_EXTENSIONS
    + GVCF_EXTENSIONS
    + VCF_EXTENSIONS
)

FILE_TYPES_MAP = {
    'fastq': FASTQ_EXTENSIONS,
    'bam': BAM_EXTENSIONS,
    'cram': CRAM_EXTENSIONS,
    'all_reads': READ_EXTENSIONS,
    'gvcf': GVCF_EXTENSIONS,
    'vcf': VCF_EXTENSIONS,
    'all': ALL_EXTENSIONS,
}

SEQUENCE_TYPES_MAP = {
    'genome': [
        'genome',
    ],
    'exome': [
        'exome',
    ],
    'all': [
        'genome',
        'exome',
    ],
}


class UploadBucketAuditor(GenericAuditor):
    """Auditor specifically for upload cloud storage buckets"""

    def __init__(
        self,
        dataset: str,
        sequencing_type: list[str],
        file_types: tuple[str],
        default_analysis_type='cram',
        default_analysis_status='completed',
    ):
        super().__init__(
            dataset=dataset,
            sequencing_type=sequencing_type,
            file_types=file_types,
            default_analysis_type=default_analysis_type,
            default_analysis_status=default_analysis_status,
        )

    def write_upload_bucket_audit_reports(
        self,
        bucket_name: str,
        sequencing_type_str: str,
        file_types_str: str,
        sequence_files_to_delete: list[tuple[str, int, str, list[int]]],
        sequence_files_to_ingest: list[tuple[str, str, str, int, str]],
        incomplete_samples: list[tuple[str, str]],
    ):
        """Write the sequences to delete and ingest to csv files and upload them to the bucket"""
        # Writing the output to files with the file type, sequence type, and date specified
        today = datetime.today().strftime('%Y-%m-%d')

        report_path = f'{bucket_name}/audit_results/{today}/'

        # Sequences to delete report has several data points for validation
        if not sequence_files_to_delete:
            logging.info('No sequence read files to delete found. Skipping report...')
        else:
            sequences_to_delete_file = f'{self.dataset}_{file_types_str}_{sequencing_type_str}_sequences_to_delete_{today}.csv'
            self.write_csv_report_to_cloud(
                data_to_write=sequence_files_to_delete,
                report_path=os.path.join(report_path, sequences_to_delete_file),
                header_row=[
                    'Sample_ID',
                    'Sequence_ID',
                    'Sequence_Path',
                    'Analysis_IDs',
                    'Filesize',
                ],
            )

        # Sequences to ingest file only contains paths to the (possibly) uningested files
        if not sequence_files_to_ingest:
            logging.info('No sequence reads to ingest found. Skipping report...')
        else:
            sequences_to_ingest_file = f'{self.dataset}_{file_types_str}_{sequencing_type_str}_sequences_to_ingest_{today}.csv'
            self.write_csv_report_to_cloud(
                data_to_write=sequence_files_to_ingest,
                report_path=os.path.join(report_path, sequences_to_ingest_file),
                header_row=[
                    'Sequence_Path',
                    'Extenal_Sample_ID',
                    'Internal_Sample_ID',
                    'CRAM_Analysis_ID',
                    'CRAM_Path',
                ],
            )

        # Write the samples without any completed cram to a csv
        if not incomplete_samples:
            logging.info(f'No samples without crams found. Skipping report...')
        else:
            incomplete_samples_file = f'{self.dataset}_{file_types_str}_{sequencing_type_str}_samples_without_crams_{today}.csv'
            self.write_csv_report_to_cloud(
                data_to_write=incomplete_samples,
                report_path=os.path.join(report_path, incomplete_samples_file),
                header_row=['Internal_Sample_ID', 'External_Sample_ID'],
            )


async def audit_upload_bucket_files(
    dataset, sequencing_type, file_types, default_analysis_type, default_analysis_status
):
    """
    Finds sequence files for samples with completed CRAMs and adds these to a csv for deletion.
    Also finds any extra files in the upload bucket which may be uningested sequence data.
    Reports any files to delete, files to ingest, and samples without completed crams in output files
    """
    config = get_config()
    if not dataset:
        dataset = config['workflow']['dataset']

    bucket_name = config['storage']['default']['upload']

    auditor = UploadBucketAuditor(
        dataset=dataset,
        sequencing_type=SEQUENCE_TYPES_MAP.get(sequencing_type),
        file_types=FILE_TYPES_MAP.get(file_types),
        default_analysis_type=default_analysis_type,
        default_analysis_status=default_analysis_status,
    )

    # Get all the participants and all the samples mapped to participants
    participant_data = auditor.get_participant_data_for_dataset()

    # Get all the samples
    sample_internal_external_id_map = auditor.map_internal_to_external_sample_ids(
        participant_data
    )

    # Get all the sequences for the samples in the dataset and map them to their samples and reads
    (
        seq_id_sample_id_map,
        sequence_filepaths_filesizes,
    ) = auditor.get_sequence_map_from_participants(participant_data)

    # Get all completed cram output paths for the samples in the dataset and validate them
    sample_cram_paths = auditor.get_analysis_cram_paths_for_dataset_samples(
        sample_internal_external_id_map
    )

    # Identify samples with and without completed crams
    sample_completion = auditor.get_complete_and_incomplete_samples(
        sample_internal_external_id_map, sample_cram_paths
    )
    incomplete_samples = [
        (sample_id, sample_internal_external_id_map.get(sample_id))
        for sample_id in sample_completion.get('incomplete')
    ]

    # Samples with completed crams can have their sequences deleted - these are the obvious ones
    (
        sequence_reads_to_delete,
        sequence_files_to_ingest,
    ) = await auditor.get_reads_to_delete_or_ingest(
        bucket_name,
        sample_completion.get('complete'),
        sequence_filepaths_filesizes,
        seq_id_sample_id_map,
        sample_internal_external_id_map,
    )

    possible_sequence_ingests = auditor.find_crams_for_reads_to_ingest(
        sequence_files_to_ingest, sample_cram_paths
    )

    auditor.write_upload_bucket_audit_reports(
        bucket_name,
        sequencing_type_str=sequencing_type,
        file_types_str=file_types,
        sequence_files_to_delete=sequence_reads_to_delete,
        sequence_files_to_ingest=possible_sequence_ingests,
        incomplete_samples=incomplete_samples,
    )


@click.command()
@click.option(
    '--dataset',
    help='Metamist dataset, used to filter samples',
)
@click.option(
    '--sequence-type',
    '-s',
    type=click.Choice(['genome', 'exome', 'all']),
    required='True',
    help='genome, exome, or all',
)
@click.option(
    '--file-types',
    '-f',
    type=click.Choice(['fastq', 'bam', 'cram', 'all_reads', 'gvcf', 'vcf', 'all']),
    required='True',
    help='Find fastq, bam, cram, gvcf, vcf, all_reads (fastq + bam + cram), or all sequence file types',
)
def main(
    dataset,
    sequencing_type,
    file_types,
    default_analysis_type='cram',
    default_analysis_status='completed',
):
    """Runs the auditor on the dataset"""
    asyncio.run(
        audit_upload_bucket_files(
            dataset,
            sequencing_type,
            file_types,
            default_analysis_type,
            default_analysis_status,
        )
    )


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )
    main()  # pylint: disable=no-value-for-parameter
