#!/usr/bin/env python
"""
Report assay files in the main-upload bucket that can be deleted, ingested,
and sequencing groups that have no aligned CRAM.
"""

import asyncio
import logging
import sys
from datetime import datetime

import click

from cpg_utils.config import config_retrieve, dataset_path

from metamist.audit.audithelper import FILE_TYPES_MAP, get_sequencing_types
from metamist.audit.generic_auditor import GenericAuditor, SequencingGroupData


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
        # audit_report_assay_files_to_delete,
        # audit_report_assay_files_to_ingest,
        # audit_report_unaligned_sgs,
        audit_reports: dict[str, list[dict[str, str]]],
        report_extension: str = 'tsv',
    ):
        """
        Writes the 'assay files to delete/ingest' csv reports and upload them to the bucket.
        Also writes a report for any assay files found that match existing samples and may
        require ingestion.

        The report names include the file types, sequencing types, and date of the audit.
        """
        today = datetime.today().strftime('%Y-%m-%d')
        report_prefix = self.get_audit_report_prefix(
            seq_types=self.sequencing_types, file_types=self.file_types
        )
        for audit_report_type, audit_report in audit_reports.items():
            self.write_report_to_cloud(
                data_to_write=audit_report,
                bucket_name=bucket_name,
                blob_path=f'audit_results/{today}/{report_prefix}_{audit_report_type}.{report_extension}',
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
    
    # Initialise the auditor
    auditor = UploadBucketAuditor(
        dataset=dataset,
        sequencing_types=sequencing_types,
        file_types=file_types,
        default_analysis_type=default_analysis_type,
        default_analysis_status=default_analysis_status,
    )

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

    if not dataset:
        dataset = config_retrieve(['workflow', 'dataset'])
    bucket_name = dataset_path(dataset=dataset, category='upload')



    # participant_data = await auditor.get_participant_data_for_dataset()
    # sample_internal_external_id_map = auditor.map_internal_to_external_sample_ids(
    #     participant_data
    # )
    # (
    #     sg_sample_id_map,
    #     assay_sg_id_map,
    #     assay_filepaths_filesizes,
    # ) = auditor.get_assay_map_from_participants(participant_data)
    
    sequencing_groups: list[SequencingGroupData] = await auditor.get_sg_assays_for_dataset()

    # Get all completed cram output paths for the samples in the dataset and validate them
    sg_cram_paths = await auditor.get_analysis_cram_paths_for_dataset_sgs(
        sequencing_groups
    )

    # Identify sgs with and without completed crams
    sg_completion = await auditor.get_complete_and_incomplete_sgs(
        sequencing_groups, sg_cram_paths
    )
    
    # Get the unaligned sequencing groups
    unaligned_sgs = []
    for sg in sequencing_groups:
        if sg.id in sg_completion.get('incomplete'):
            unaligned_sgs.append(
                {
                    'sg_id': sg.id,
                    'sample_id': sg.sample.id,
                    'sample_external_id': sg.sample.external_id,
                    'participant_id': sg.sample.participant.id,
                    'participant_external_id': sg.sample.participant.external_id,
                }
            )
        
    # Extract the assay file paths and sizes for the dataset's SGs
    assay_id_to_paths_sizes = {assay.id: assay.read_files_paths_sizes for sg in sequencing_groups for assay in sg.assays}
    
    # Get the assay files to delete and ingest
    (
        reads_to_delete,
        reads_to_ingest,
    ) = await auditor.get_reads_to_delete_or_ingest(
        bucket_name=bucket_name,
        sequencing_groups=sequencing_groups,
        completed_sgs=sg_completion.get('complete'),
        assay_id_to_paths_sizes=assay_id_to_paths_sizes,
    )

    possible_assay_ingests = auditor.find_crams_for_reads_to_ingest(
        reads_to_ingest, sg_cram_paths
    )

    # Write the reads to delete, reads to ingest, and unaligned SGs reports
    await auditor.write_upload_bucket_audit_reports(
        bucket_name=bucket_name,
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
        level=logging.WARNING,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stderr,
    )
    main()  # pylint: disable=no-value-for-parameter
