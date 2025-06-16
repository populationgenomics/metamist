#!/usr/bin/env python
"""
Report assay files in the main-upload bucket that can be deleted, ingested,
and sequencing groups that have no aligned CRAM.
"""

import asyncio
from datetime import datetime

import click

from metamist.audit.audithelper import SequencingGroupData, AuditReportEntry, logger
from metamist.audit.generic_auditor import GenericAuditor


def audit_upload_bucket(
    dataset: str,
    sequencing_types: list[str],
    sequencing_technologies: list[str],
    file_types: list[str],
    excluded_prefixes: tuple[str] | None = None,
):
    """Entrypoint for running upload bucket auditor asynchronously."""
    asyncio.get_event_loop().run_until_complete(
        asyncio.wait_for(
            audit_upload_bucket_async(
                dataset,
                sequencing_types,
                sequencing_technologies,
                file_types,
                excluded_prefixes=excluded_prefixes,
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
        sequencing_technologies: list[str],
        file_types: tuple[str],
        excluded_prefixes: tuple[str] | None = None,
        default_analysis_type='cram',
        default_analysis_status='completed',
    ):
        super().__init__(
            dataset=dataset,
            sequencing_types=sequencing_types,
            sequencing_technologies=sequencing_technologies,
            file_types=file_types,
            excluded_prefixes=excluded_prefixes,
            default_analysis_type=default_analysis_type,
            default_analysis_status=default_analysis_status,
        )
        logger.info(f'Initialising UploadBucketAuditor for {dataset}\n')

    async def write_unaligned_sgs_report(
        self,
        unaligned_sgs: list[SequencingGroupData],
        report_extension: str = 'tsv',
        report_name: str = 'UNALIGNED SEQUENCING GROUPS',
    ):
        """Writes a report of the unaligned sequencing groups to the bucket"""
        today = datetime.today().strftime('%Y-%m-%d')
        report_prefix = self.get_audit_report_prefix(
            seq_types=self.sequencing_types, file_types=self.file_types
        )
        self.write_report_to_cloud(
            data_to_write=self.get_audit_report_records_from_sgs(unaligned_sgs),
            bucket_name=self.bucket_name,
            blob_path=f'audit_results/{today}/{report_prefix}_unaligned_sgs.{report_extension}',
            report_name=report_name,
        )

    async def write_upload_bucket_audit_reports(
        self,
        bucket_name: str,
        audit_reports: dict[str, list[AuditReportEntry]],
        report_extension: str = 'csv',
    ):
        """
        Writes the 'assay files to delete/ingest' reports and upload them to the bucket.

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
                report_name='FILES TO DELETE'
                if audit_report_type == 'reads_to_delete'
                else 'FILES TO INGEST',
            )


async def audit_upload_bucket_async(
    dataset: str,
    sequencing_types: list[str],
    sequencing_technologies: list[str],
    file_types: list[str],
    excluded_prefixes: tuple[str] | None = None,
):
    """
    Finds sequence files for samples with completed CRAMs and adds these to a csv for deletion.
    Also finds any extra files in the upload bucket which may be uningested sequence data.
    Reports any files to delete, files to ingest, and samples without completed crams in output files

    Arguments:
        dataset: The name of the dataset to audit
        sequencing_types: The list of sequencing types to audit
        sequencing_technologies: The list of sequencing technologies to audit
        file_types: The list of file types to audit
        exclude_prefixes: Optional prefixes to exclude from the audit
    """
    # Initialise the auditor
    auditor = UploadBucketAuditor(
        dataset=dataset,
        sequencing_types=sequencing_types,
        sequencing_technologies=sequencing_technologies,
        file_types=file_types,
        excluded_prefixes=excluded_prefixes,
    )
    sequencing_groups: list[SequencingGroupData] = await auditor.get_sgs_for_dataset()

    # Update the sequencing groups with their completed cram analyses
    await auditor.update_sequencing_groups_with_crams(sequencing_groups)

    # Identify sgs with and without completed crams
    sg_completion: dict[str, list[SequencingGroupData]] = await auditor.check_sg_crams(
        sequencing_groups
    )

    # Write a report of the unaligned sequencing groups if any
    await auditor.write_unaligned_sgs_report(
        unaligned_sgs=sg_completion.get('incomplete'),
    )

    # Get the reads to delete and ingest
    (
        reads_to_delete,
        reads_to_ingest,
    ) = await auditor.get_audit_report_records_for_reads_to_delete_and_reads_to_ingest(
        sequencing_groups=sequencing_groups
    )

    # Write the reads to delete and ingest reports
    await auditor.write_upload_bucket_audit_reports(
        bucket_name=auditor.bucket_name,
        audit_reports={
            'reads_to_delete': reads_to_delete,
            'reads_to_ingest': reads_to_ingest,
        },
    )

    logger.info(f'{dataset} upload bucket audit complete')


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
    help='"all", or any of the enum sequencing types',
)
@click.option(
    '--sequencing-technologies',
    '-t',
    multiple=True,
    required=True,
    help='"all", or any of the enum sequencing technologies',
)
@click.option(
    '--file-types',
    '-f',
    multiple=True,
    required='True',
    help='Find fastq, bam, cram, gvcf, vcf, all_reads (fastq + bam + cram), or all sequence file types',
)
@click.option(
    '--excluded-prefixes',
    '-e',
    multiple=True,
    help='Exclude files with these prefixes from the audit',
)
def main(
    dataset: str,
    sequencing_types: tuple[str],
    sequencing_technologies: tuple[str],
    file_types: tuple[str],
    excluded_prefixes: tuple[str] | None = None,
):
    """Runs the auditor on the dataset"""
    audit_upload_bucket(
        dataset,
        sequencing_types,
        sequencing_technologies,
        file_types,
        excluded_prefixes=excluded_prefixes,
    )


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
