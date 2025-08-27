"""CLI entry point for deleting files in audit results."""

import logging
import csv

import click

from ..adapters import StorageClient
from ..data_access.gcs_data_access import GCSDataAccess

from metamist.apis import AnalysisApi
from metamist.models import Analysis, AnalysisQueryModel, AnalysisStatus, AnalysisUpdateModel

from cpg_utils import Path, to_path
from cpg_utils.config import config_retrieve


def setup_logger(dataset: str) -> logging.Logger:
    """Set up logger for audit deletions."""
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(dataset)s :: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    handler.setFormatter(formatter)
    logger = logging.getLogger('audit_deletions')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    return logging.LoggerAdapter(logger, {'dataset': dataset})


def delete_from_audit_results(
    dataset: str,
    results_folder: str,
    report_name: str = 'files_to_delete',
    dry_run: bool = False,
):
    """Delete files listed in the audit results."""
    # Set up logger
    logger = setup_logger(dataset)

    # Get GCP project
    gcp_project = config_retrieve(['workflow', dataset, 'gcp_project'])
    if not gcp_project:
        raise ValueError('GCP project is required')

    storage_client = StorageClient(project=gcp_project)
    gcs_data = GCSDataAccess(storage_client)

    bucket_name = gcs_data.get_bucket_name(dataset, 'upload')

    report_path = to_path(f'gs://{bucket_name}/{results_folder}/{report_name}.csv')
    if not report_path.exists():
        logger.error(f'Report file {report_path} does not exist.')
        return

    with report_path.open('r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    deleted_files_rows = delete_files(storage_client, report_name, rows, dry_run, logger)

    report_path = write_deleted_files_report(deleted_files_rows, bucket_name, results_folder, logger)
    
    # Get the stats from the report by reading it again
    stats = get_report_stats(report_path)
    
    logger.info('Upserting analysis record for deleted files report.')
    upsert_deleted_files_analysis(
        dataset,
        report_path,
        stats,
        logger
    )


def delete_files(
    storage_client: StorageClient,
    report_name: str,
    rows: list[dict],
    dry_run: bool,
    logger: logging.Logger,
) -> list[dict]:
    """Delete files from audit results based on filters."""
    total_files = 0
    total_bytes = 0
    deleted_files_rows = []
    for row in rows:
        if report_name == 'files_to_delete':
            row = row | {'Review Comment': 'Deleted due to audit determination'}

        if not row.get('Review Comment'):
            raise ValueError(
                'Review Comment is required for rows outside the \'files_to_delete\' report.'
                'Use the \'review_audit_results.py\' script to add comments justifying the deletion.'
            )

        file_path = to_path(row['File Path'])
        if not file_path.exists():
            logger.warning(f'File {file_path} does not exist, skipping deletion.')
            continue
        total_files += 1
        total_bytes += int(row['File Size'])

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
        
    return deleted_files_rows


def write_deleted_files_report(
    deleted_files_rows: list[dict],
    bucket_name: str,
    results_folder: str,
    logger: logging.Logger,
):
    """Write a report of deleted files."""
    if not deleted_files_rows:
        logger.info('No files deleted, skipping report.')
        return

    report_path = to_path(f'gs://{bucket_name}/{results_folder}/deleted_files.csv')
    # If the report exists, append to it. If not, create it and write the header.
    if not report_path.exists():
        logger.info(f'Creating new report {report_path}')
        with report_path.open('w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=deleted_files_rows[0].keys())
            writer.writeheader()
            writer.writerows(deleted_files_rows)
    else:
        logger.info(f'Appending to existing report {report_path}')
        with report_path.open('a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=deleted_files_rows[0].keys())
            writer.writerows(deleted_files_rows)

    logger.info(f'Deleted files report written to {report_path}')

    return report_path


def get_report_stats(report_path: Path) -> dict:
    """Count the rows and sum the total file size for all rows in the report"""
    with report_path.open('r') as f:
        reader = csv.DictReader(f)
        total_size = sum(int(row['File Size']) for row in reader)
        row_count = sum(1 for _ in reader)
    return {'total_size': total_size, 'file_count': row_count}


def upsert_deleted_files_analysis(
    dataset: str,
    report_path: str,
    stats: dict,
    logger: logging.Logger,
):
    """Inserts or updates an analysis record for deleted files."""
    if existing_analysis := get_existing_analysis(dataset, report_path):
        logger.info(f'Found existing analysis record for {report_path}, ID: {existing_analysis["id"]}. Updating...')
        analysis_update = AnalysisUpdateModel(
            status=AnalysisStatus('completed'),
            output=existing_analysis['output'],
            meta=stats,
        )
        AnalysisApi().update_analysis(existing_analysis['id'], analysis_update)

    else:
        logger.info(f'No existing analysis record for {report_path}. Creating new record...')
        analysis = Analysis(
            type='audit_deletion',
            output=str(report_path),
            status=AnalysisStatus('completed'),
            meta=stats,
        )
        AnalysisApi().create_analysis(dataset, analysis)
    logger.info(f'Upserted analysis record for {report_path}')


def get_existing_analysis(dataset: str, report_path: str) -> Analysis | None:
    """Retrieve an existing analysis record for the given dataset and report path."""
    analyses = AnalysisApi().query_analyses(
        AnalysisQueryModel(
            projects=[dataset,],
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
@click.option('--report-name', default='files_to_delete', help='Name of the report to delete files from')
@click.option('--dry-run', is_flag=True, help='If set, will not delete objects.')
def main(
    dataset: str,
    results_folder: str,
    report_name: str = 'files_to_delete',
    dry_run: bool = False,
):
    """Main function to delete objects."""
    delete_from_audit_results(
        dataset, results_folder, report_name, dry_run
    )


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
