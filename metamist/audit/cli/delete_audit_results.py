"""CLI entry point for deleting files in audit results."""

import logging
import csv

import click

from ..adapters import StorageClient
from ..data_access.gcs_data_access import GCSDataAccess
from ..models import AuditReportEntry, DeletionResult
from ..services import AuditLogger, ReportGenerator

from metamist.apis import AnalysisApi
from metamist.models import (
    Analysis,
    AnalysisQueryModel,
    AnalysisStatus,
    AnalysisUpdateModel,
)

from cpg_utils import Path, to_path
from cpg_utils.config import config_retrieve


def delete_from_audit_results(
    dataset: str,
    results_folder: str,
    report_name: str = 'files_to_delete',
    dry_run: bool = False,
):
    """Delete files listed in the audit results."""
    logger = AuditLogger(dataset, 'audit_deletions').logger

    gcp_project = config_retrieve(['workflow', dataset, 'gcp_project'])
    if not gcp_project:
        raise ValueError('GCP project is required')

    storage_client = StorageClient(project=gcp_project)
    gcs_data = GCSDataAccess(storage_client)
    report_generator = ReportGenerator(storage_client, results_folder, logger)

    bucket_name = gcs_data.get_bucket_name(dataset, 'upload')

    report_path = to_path(f'gs://{bucket_name}/{results_folder}/{report_name}.csv')
    if not report_path.exists():
        logger.error(f'Report file {report_path} does not exist.')
        return

    with report_path.open('r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    deletion_result = delete_files(storage_client, report_name, rows, dry_run, logger)

    report_path = report_generator.write_deleted_files_report(
        deletion_result, bucket_name
    )

    # Get the stats from the report by reading it again
    stats = get_report_stats(to_path(report_path))

    logger.info('Upserting analysis record for deleted files report.')
    upsert_deleted_files_analysis(dataset, report_name, report_path, stats, logger)


def delete_files(
    storage_client: StorageClient,
    report_name: str,
    rows: list[AuditReportEntry],
    dry_run: bool,
    logger: logging.Logger,
) -> DeletionResult:
    """Delete files from audit results based on filters."""
    total_files = 0
    total_bytes = 0
    deleted_files_rows = []
    for row in rows:
        if report_name == 'files_to_delete':
            row.update_action('DELETE')
            row.update_review_comment('Deleted due to audit determination')

        if not row.action or row.action.upper() != 'DELETE':
            continue

        if not row.review_comment:
            raise ValueError(
                "Review Comment is required for rows outside the 'files_to_delete' report."
                "Use the 'review_audit_results.py' script to add comments justifying the deletion."
            )

        file_path = to_path(row.filepath)
        if not file_path.exists():
            logger.warning(f'File {file_path} does not exist, skipping deletion.')
            continue
        total_files += 1
        total_bytes += int(row.filesize)

        if dry_run:
            logger.info(f'Dry run: would delete {file_path}')
        else:
            try:
                storage_client.delete_blob(file_path)
                logger.info(f'Successfully deleted {file_path}')
                deleted_files_rows.append(row)
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error(f'Error deleting {file_path}: {e}')

    if dry_run:
        logger.info(
            f'Dry run: would delete {total_files} files ({total_bytes / (1024**3):.2f} GiB)'
        )
    else:
        logger.info(f'Deleted: {total_files} files ({total_bytes / (1024**3):.2f} GiB)')

    return DeletionResult(deleted_files=deleted_files_rows)


def get_report_stats(report_path: Path) -> dict:
    """Count the rows and sum the total file size for all rows in the report"""
    with report_path.open('r') as f:
        reader = csv.DictReader(f)
        total_size = sum(int(row['File Size']) for row in reader)
        row_count = sum(1 for _ in reader)
    return {'total_size': total_size, 'file_count': row_count}


def upsert_deleted_files_analysis(
    dataset: str,
    report_name: str,
    report_path: str,
    stats: dict,
    logger: logging.Logger,
):
    """Inserts or updates an analysis record for deleted files."""
    if existing_analysis := get_existing_analysis(dataset, report_path):
        logger.info(
            f'Found existing analysis record for {report_path}, ID: {existing_analysis["id"]}. Updating...'
        )
        analysis_update = AnalysisUpdateModel(
            status=AnalysisStatus('completed'),
            output=existing_analysis['output'],
            meta=existing_analysis['meta'] | {report_name: stats},
        )
        AnalysisApi().update_analysis(existing_analysis['id'], analysis_update)

    else:
        logger.info(
            f'No existing analysis record for {report_path}. Creating new record...'
        )
        analysis = Analysis(
            type='audit_deletion',
            output=str(report_path),
            status=AnalysisStatus('completed'),
            meta={report_name: stats},
        )
        AnalysisApi().create_analysis(dataset, analysis)
    logger.info(f'Upserted analysis record for {report_path}')


def get_existing_analysis(dataset: str, report_path: str) -> Analysis | None:
    """Retrieve an existing analysis record for the given dataset and report path."""
    analyses = AnalysisApi().query_analyses(
        AnalysisQueryModel(
            projects=[
                dataset,
            ],
            sequencing_group_ids=[],
            type='audit_deletion',
            status='completed',
            active=True,
        )
    )

    for analysis in analyses:
        if analysis['output'] == report_path:
            return analysis
    return None


@click.command()
@click.option('--dataset', required=True, help='Dataset name for logging context')
@click.option(
    '--results-folder', help='Path to the folder containing audit results files'
)
@click.option(
    '--report-name',
    default='files_to_delete',
    help='Name of the report to delete files from',
)
@click.option('--dry-run', is_flag=True, help='If set, will not delete objects.')
def main(
    dataset: str,
    results_folder: str,
    report_name: str = 'files_to_delete',
    dry_run: bool = False,
):
    """Main function to delete objects."""
    delete_from_audit_results(dataset, results_folder, report_name, dry_run)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
