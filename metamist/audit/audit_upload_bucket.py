#!/usr/bin/env python
"""
This auditor looks for sequence files in the main upload bucket that can be deleted or ingested,
as well as samples that have no completed cram.

Procedure:
1. Input a Metamist dataset, sequence types to audit, and file types to search for in the bucket
2. Get all the participants, samples, sequencing groups, and assays for this dataset
3. Search the upload bucket for all assay files, and compare to the files in the metamist assay reads
    - If there are discrepencies, check if the file name and size is the same in the bucket as it is in metamist
      - If the name and file size are the same, assume this file has just been moved around in the bucket
4. Check if a sequencing group has a completed cram - if so, its assay read files can be removed
5. Any remaining assay data in the bucket might require ingestion
6. Create reports in the audit_results folder of the upload bucket, containing assay read files to delete,
   ingest, and any sequencing groups without completed crams.
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

SEQUENCING_TYPES_MAP = {
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
        assay_files_to_delete: list[tuple[str, int, str, list[int]]],
        assay_files_to_ingest: list[tuple[str, str, str, int, str]],
        unaligned_sgs: list[tuple[str, str]],
    ):
        """
        Writes the 'sequence files to delete/ingest' csv reports and upload them to the bucket.
        Also writes a report for any sequence files found that match existing samples and may
        require ingestion.

        The reports name includes the file types, sequencing types, and date of the audit.
        """
        today = datetime.today().strftime('%Y-%m-%d')

        report_path = f'{bucket_name}/audit_results/{today}/'

        if not assay_files_to_delete:
            logging.info('No assay read files to delete found. Skipping report...')
        else:
            assays_to_delete_file = f'{self.dataset}_{file_types_str}_{sequencing_type_str}_assay_files_to_delete_{today}.csv'
            self.write_csv_report_to_cloud(
                data_to_write=assay_files_to_delete,
                report_path=os.path.join(report_path, assays_to_delete_file),
                header_row=[
                    'SG_ID',
                    'Assay_ID',
                    'Assay_Read_File_Path',
                    'Analysis_IDs',
                    'Filesize',
                ],
            )

        # 'Sequences to ingest' report contains paths to the (possibly) uningested files - and any samples/SGs that might be related
        if not assay_files_to_ingest:
            logging.info('No assay reads to ingest found. Skipping report...')
        else:
            assays_to_ingest_file = f'{self.dataset}_{file_types_str}_{sequencing_type_str}_assay_files_to_ingest_{today}.csv'
            self.write_csv_report_to_cloud(
                data_to_write=assay_files_to_ingest,
                report_path=os.path.join(report_path, assays_to_ingest_file),
                header_row=[
                    'Assay_File_Path',
                    'SG_ID',
                    'Sample_ID',
                    'Sample_External_ID',
                    'CRAM_Analysis_ID',
                    'CRAM_Path',
                ],
            )

        # Write the sequencing groups without any completed cram to a csv
        if not unaligned_sgs:
            logging.info(
                f'No sequencing groups without crams found. Skipping report...'
            )
        else:
            unaligned_sgs_file = f'{self.dataset}_{file_types_str}_{sequencing_type_str}_unaligned_sgs_{today}.csv'
            self.write_csv_report_to_cloud(
                data_to_write=unaligned_sgs,
                report_path=os.path.join(report_path, unaligned_sgs_file),
                header_row=['SG_ID', 'Sample_ID', 'Sample_External_ID'],
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
    bucket_name = f'gs://cpg-{dataset}-main-upload'

    auditor = UploadBucketAuditor(
        dataset=dataset,
        sequencing_type=SEQUENCING_TYPES_MAP.get(sequencing_type),
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
        sg_sample_id_map,
        assay_sg_id_map,
        assay_filepaths_filesizes,
    ) = auditor.get_assay_map_from_participants(participant_data)

    # Get all completed cram output paths for the samples in the dataset and validate them
    sg_cram_paths = auditor.get_analysis_cram_paths_for_dataset_sgs(assay_sg_id_map)

    # Identify sgs with and without completed crams
    sg_completion = auditor.get_complete_and_incomplete_sgs(
        assay_sg_id_map, sg_cram_paths
    )
    unaligned_sgs = [
        (
            sg_id,
            sg_sample_id_map[sg_id],
            sample_internal_external_id_map.get(sg_sample_id_map[sg_id]),
        )
        for sg_id in sg_completion.get('incomplete')
    ]

    # Samples with completed crams can have their sequences deleted - these are the obvious ones
    (
        reads_to_delete,
        reads_to_ingest,
    ) = await auditor.get_reads_to_delete_or_ingest(
        bucket_name,
        sg_completion.get('complete'),
        assay_filepaths_filesizes,
        sg_sample_id_map,
        assay_sg_id_map,
        sample_internal_external_id_map,
    )

    possible_assay_ingests = auditor.find_crams_for_reads_to_ingest(
        reads_to_ingest, sg_cram_paths
    )

    auditor.write_upload_bucket_audit_reports(
        bucket_name,
        sequencing_type_str=sequencing_type,
        file_types_str=file_types,
        assay_files_to_delete=reads_to_delete,
        assay_files_to_ingest=possible_assay_ingests,
        unaligned_sgs=unaligned_sgs,
    )


@click.command()
@click.option(
    '--dataset',
    '-d',
    help='Metamist dataset, used to filter samples',
)
@click.option(
    '--sequencing-type',
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
