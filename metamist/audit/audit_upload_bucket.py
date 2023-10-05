#!/usr/bin/env python
"""
Report assay files in the main-upload bucket that can be deleted, ingested,
and sequencing groups that have no aligned CRAM.
"""

from datetime import datetime
import sys
import logging
import os
import asyncio
from functools import cache
import click

from cpg_utils.config import get_config


from metamist.audit.generic_auditor import GenericAuditor
from metamist.graphql import gql, query


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

SEQUENCING_TYPES_QUERY = gql(
    """
    query seqTypes {
        enum {
            sequencingType
        }
    }
    """
)


@cache
def get_sequencing_types():
    """Return the list of sequencing types from the enum table."""
    logging.getLogger().setLevel(logging.WARN)
    sequencing_types = query(SEQUENCING_TYPES_QUERY)
    logging.getLogger().setLevel(logging.INFO)
    return sequencing_types['enum'][  # pylint: disable=unsubscriptable-object
        'sequencingType'
    ]


def audit_upload_bucket(
    dataset: str,
    sequencing_types: list[str],
    file_types: list[str],
    default_analysis_type: str,
    default_analysis_status: str,
):
    """Entrypoint for running upload bucket auditor asynchronously."""
    asyncio.get_event_loop().run_until_complete(
        asyncio.wait_for(
            audit_upload_bucket_async(
                dataset,
                sequencing_types,
                file_types,
                default_analysis_type,
                default_analysis_status,
            ),
            timeout=60,
        )
    )


class UploadBucketAuditor(GenericAuditor):
    """Auditor for upload cloud storage buckets"""

    def __init__(
        self,
        dataset: str,
        sequencing_types: list[str],
        file_types: tuple[str],
        default_analysis_type='cram',
        default_analysis_status='completed',
    ):
        super().__init__(
            dataset=dataset,
            sequencing_types=sequencing_types,
            file_types=file_types,
            default_analysis_type=default_analysis_type,
            default_analysis_status=default_analysis_status,
        )

    async def write_upload_bucket_audit_reports(
        self,
        bucket_name: str,
        sequencing_types: list[str],
        file_types: list[str],
        assay_files_to_delete: list[tuple[str, int, str, list[int]]],
        assay_files_to_ingest: list[tuple[str, str, str, int, str]],
        unaligned_sgs: list[tuple[str, str]],
    ):
        """
        Writes the 'assay files to delete/ingest' csv reports and upload them to the bucket.
        Also writes a report for any assay files found that match existing samples and may
        require ingestion.

        The report names include the file types, sequencing types, and date of the audit.
        """
        today = datetime.today().strftime('%Y-%m-%d')

        report_path = f'gs://{bucket_name}/audit_results/{today}/'

        if set(sequencing_types) == set(get_sequencing_types()):
            sequencing_types_str = 'all'
        else:
            sequencing_types_str = ('_').join(sequencing_types)

        if set(file_types) == set(ALL_EXTENSIONS):
            file_types_str = 'all'
        elif set(file_types) == set(READ_EXTENSIONS):
            file_types_str = 'all_reads'
        else:
            file_types_str = ('_').join(file_types)

        report_prefix = f'{self.dataset}_{file_types_str}_{sequencing_types_str}'

        if not assay_files_to_delete:
            logging.info('No assay read files to delete found. Skipping report...')
        else:
            assays_to_delete_file = f'{report_prefix}_assay_files_to_delete_{today}.csv'
            self.write_csv_report_to_cloud(
                data_to_write=assay_files_to_delete,
                report_path=os.path.join(report_path, assays_to_delete_file),
                header_row=[
                    'SG_ID',
                    'Assay_ID',
                    'Assay_Read_File_Path',
                    'CRAM_Analysis_ID',
                    'Filesize',
                ],
            )

        # 'Sequences to ingest' report contains paths to the (possibly) uningested files - and any samples/SGs that might be related
        if not assay_files_to_ingest:
            logging.info('No assay reads to ingest found. Skipping report...')
        else:
            assays_to_ingest_file = f'{report_prefix}_assay_files_to_ingest_{today}.csv'
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
            logging.info('No sequencing groups without crams found. Skipping report...')
        else:
            unaligned_sgs_file = f'{report_prefix}_unaligned_sgs_{today}.csv'
            self.write_csv_report_to_cloud(
                data_to_write=unaligned_sgs,
                report_path=os.path.join(report_path, unaligned_sgs_file),
                header_row=['SG_ID', 'Sample_ID', 'Sample_External_ID'],
            )


async def audit_upload_bucket_async(
    dataset: str,
    sequencing_types: list[str],
    file_types: list[str],
    default_analysis_type: str,
    default_analysis_status: str,
):
    """
    Finds sequence files for samples with completed CRAMs and adds these to a csv for deletion.
    Also finds any extra files in the upload bucket which may be uningested sequence data.
    Reports any files to delete, files to ingest, and samples without completed crams in output files

    Arguments:
        dataset: The name of the dataset to audit
        sequencing_types: The list of sequencing types to audit
        file_types: The list of file types to audit
        default_analysis_type: The default analysis type to audit
        default_analysis_status: The default analysis status to audit
    """

    # Validate user inputs
    allowed_sequencing_types = get_sequencing_types()
    if sequencing_types != ('all',) and any(
        st not in allowed_sequencing_types for st in sequencing_types
    ):
        raise ValueError(
            f'Input sequencing types "{sequencing_types}" must be in the allowed types: {allowed_sequencing_types}'
        )
    if sequencing_types == ('all',):
        sequencing_types = allowed_sequencing_types

    if file_types not in (('all',), ('all_reads',)):
        if any(ft not in FILE_TYPES_MAP for ft in file_types):
            raise ValueError(
                f'Input file types "{file_types}" must be in the allowed types {(", ").join(list(FILE_TYPES_MAP.keys()))}'
            )
    else:
        file_types = FILE_TYPES_MAP[file_types[0]]

    config = get_config()
    if not dataset:
        dataset = config['workflow']['dataset']
    bucket = config['storage'][dataset]['upload']

    # Initialise the auditor
    auditor = UploadBucketAuditor(
        dataset=dataset,
        sequencing_types=sequencing_types,
        file_types=file_types,
        default_analysis_type=default_analysis_type,
        default_analysis_status=default_analysis_status,
    )

    participant_data = await auditor.get_participant_data_for_dataset()
    sample_internal_external_id_map = auditor.map_internal_to_external_sample_ids(
        participant_data
    )
    (
        sg_sample_id_map,
        assay_sg_id_map,
        assay_filepaths_filesizes,
    ) = auditor.get_assay_map_from_participants(participant_data)

    # Get all completed cram output paths for the samples in the dataset and validate them
    sg_cram_paths = await auditor.get_analysis_cram_paths_for_dataset_sgs(
        assay_sg_id_map
    )

    # Identify sgs with and without completed crams
    sg_completion = await auditor.get_complete_and_incomplete_sgs(
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

    (
        reads_to_delete,
        reads_to_ingest,
    ) = await auditor.get_reads_to_delete_or_ingest(
        bucket,
        sg_completion.get('complete'),
        assay_filepaths_filesizes,
        sg_sample_id_map,
        assay_sg_id_map,
        sample_internal_external_id_map,
    )

    possible_assay_ingests = auditor.find_crams_for_reads_to_ingest(
        reads_to_ingest, sg_cram_paths
    )

    # Write the reads to delete, reads to ingest, and unaligned SGs reports
    await auditor.write_upload_bucket_audit_reports(
        bucket,
        sequencing_types=sequencing_types,
        file_types=file_types,
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
    '--sequencing-types',
    '-s',
    multiple=True,
    required=True,
    help=f'"all", or any of {", ".join(get_sequencing_types())}',
)
@click.option(
    '--file-types',
    '-f',
    multiple=True,
    required='True',
    help='Find fastq, bam, cram, gvcf, vcf, all_reads (fastq + bam + cram), or all sequence file types',
)
def main(
    dataset: str,
    sequencing_types: tuple[str],
    file_types: tuple[str],
    default_analysis_type='cram',
    default_analysis_status='completed',
):
    """Runs the auditor on the dataset"""
    audit_upload_bucket(
        dataset,
        sequencing_types,
        file_types,
        default_analysis_type,
        default_analysis_status,
    )


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stderr,
    )
    main()  # pylint: disable=no-value-for-parameter
