"""CLI entry point for deleting files in audit results."""

import logging

import click

from ..data_access import GCSDataAccess, MetamistDataAccess
from ..models import AuditReportEntry, DeletionResult
from ..services import AuditLogger, ReportGenerator

from metamist.apis import AnalysisApi
from metamist.models import (
    Analysis,
    AnalysisStatus,
    AnalysisUpdateModel,
)

from cpg_utils import to_path
from cpg_utils.config import config_retrieve


def delete_from_audit_results(
    dataset: str,
    gcp_project: str,
    results_folder: str,
    report_name: str,
    dry_run: bool = False,
):
    """Delete files listed in the audit results."""
    logger = AuditLogger(dataset, 'audit_deletions').logger

    gcs_client = GCSDataAccess(dataset, gcp_project)
    report_generator = ReportGenerator(gcs_client, results_folder, logger)

    report_rows = report_generator.get_report_rows(
        report_path=to_path(
            f'gs://{gcs_client.analysis_bucket}/{results_folder}/{report_name}.csv'
        )
    )

    deletion_result = delete_files_from_report(
        gcs_client, report_name, report_rows, dry_run, logger
    )

    deletion_report = report_generator.write_deleted_files_report(deletion_result)
    stats = report_generator.get_report_stats(deletion_report)

    if not dry_run:
        upsert_deleted_files_analysis(
            dataset, report_name, str(deletion_report), stats, logger
        )


def delete_files_from_report(
    gcs_client: GCSDataAccess,
    report_name: str,
    rows: list[AuditReportEntry],
    dry_run: bool,
    logger: logging.Logger,
) -> DeletionResult:
    """
    Delete files in the upload bucket from the audit results.
    Only delete files from rows marked with 'DELETE' action.

    If deleting from 'files_to_delete', automatically add this action.
    """
    delete_file_rows: list[AuditReportEntry] = []
    for row in rows:
        if report_name == 'files_to_delete':
            row.update_action('DELETE')
            row.update_review_comment('Deleted due to audit determination')

        if not row.action or row.action.upper() != 'DELETE':
            continue

        if not row.review_comment:
            raise ValueError(
                "Review Comment is required for rows outside the 'files_to_delete' report. "
                'Use `review_audit_results.py` to add comments justifying the deletion, then try again.'
            )

        file_path = to_path(row.filepath)
        if not file_path.exists():
            logger.warning(f'File {file_path} does not exist, skipping deletion.')
            continue

        delete_file_rows.append(row)

    total_bytes = sum(int(row.filesize) for row in delete_file_rows)
    if dry_run:
        logger.info(
            f'Dry run: would have deleted {len(delete_file_rows)} files ({total_bytes / (1024**3):.2f} GiB)'
        )
    else:
        gcs_client.delete_blobs(
            gcs_client.upload_bucket,
            [to_path(row.filepath).blob for row in delete_file_rows],
        )
        logger.info(
            f'Deleted: {len(delete_file_rows)} files ({total_bytes / (1024**3):.2f} GiB)'
        )

    return DeletionResult(deleted_files=delete_file_rows)


async def upsert_deleted_files_analysis(
    dataset: str,
    audited_report_name: str,
    deletion_report_path: str,
    stats: dict,
    logger: logging.Logger,
):
    """
    Inserts or updates an analysis record for deleted files.
    Populates the meta dict with deleted files stats.
    """
    logger.info('Upserting analysis record for deleted files report.')
    if existing_analysis := await MetamistDataAccess().get_audit_deletion_analysis(
        dataset, deletion_report_path
    ):
        logger.info(
            f'Found existing analysis record for {deletion_report_path}, ID: {existing_analysis["id"]}. Updating...'
        )
        analysis_update = AnalysisUpdateModel(
            status=AnalysisStatus('completed'),
            output=existing_analysis['output'],
            meta=existing_analysis['meta'] | {audited_report_name: stats},
        )
        AnalysisApi().update_analysis(existing_analysis['id'], analysis_update)

    else:
        logger.info(
            f'No existing analysis record for {deletion_report_path}. Creating new record...'
        )
        analysis = Analysis(
            type='audit_deletion',
            output=str(deletion_report_path),
            status=AnalysisStatus('completed'),
            meta={audited_report_name: stats},
        )
        AnalysisApi().create_analysis(dataset, analysis)

    logger.info(f'Upserted analysis record for {deletion_report_path}')


@click.command()
@click.option('--dataset', '-d', required=True, help='Dataset name')
@click.option('--gcp-project', '-g', help='GCP project')
@click.option('--results-folder', '-r', required=True, help='Audit results folder')
@click.option(
    '--report-name',
    '-n',
    required=True,
    help="The report to delete files from. e.g. 'files_to_delete'.",
)
@click.option('--dry-run', is_flag=True, help='If set, will not delete objects.')
def main(
    dataset: str,
    gcp_project: str | None,
    results_folder: str,
    report_name: str,
    dry_run: bool = False,
):
    """
    Reads the report at location gs://cpg-dataset-main-analysis/audit_results/{results_folder}/{report_name}.csv

    If the report is 'files_to_delete', it will delete the files listed in the report.

    For any other report, files must be marked for deletion in the report, alongside a
    comment justifying the deletion.
    """
    gcp_project = gcp_project or config_retrieve(['workflow', dataset, 'gcp_project'])
    if not gcp_project:
        raise ValueError('GCP project is required from analysis-runner config or CLI.')

    delete_from_audit_results(
        dataset, gcp_project, results_folder, report_name, dry_run
    )


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
