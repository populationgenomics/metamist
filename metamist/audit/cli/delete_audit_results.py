"""CLI entry point for deleting files in audit results."""

import logging
import csv

import click

from ..adapters import StorageClient
from ..data_access.gcs_data_access import GCSDataAccess

from cpg_utils import to_path
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


def delete_from_audit_results(dataset: str, results_folder: str, report: str = 'files_to_delete', filter_func: callable = None, dry_run: bool = False):
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

    report_path = to_path(f'gs://{bucket_name}/{results_folder}/{report}.csv')
    if not report_path.exists():
        logger.error(f'Report file {report_path} does not exist.')
        return

    with report_path.open('r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    delete_filtered_files(storage_client, rows, filter_func, dry_run, logger)


def delete_filtered_files(storage_client: StorageClient, rows: list[dict], filter_func: callable, dry_run: bool, logger: logging.Logger):
    """Delete files from audit results based on filters."""
    total_files = 0
    total_bytes = 0
    for row in rows:
        if filter_func and not filter_func(row):
            logger.info(f'Skipping {row["File Path"]} due to filters: {filter_func}')
            continue

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
            except Exception as e:
                logger.error(f'Error deleting {file_path}: {e}')

    if dry_run:
        logger.info(f'Dry run: would delete {total_files} files ({total_bytes / (1024**3):.2f} GiB)')
    else:
        logger.info(f'Deleted: {total_files} files ({total_bytes / (1024**3):.2f} GiB)')

def parse_filter_expressions(filter_expressions: list[str]) -> callable:
    """Parse filter expressions into a callable filter function."""
    def matches_filters(row: dict) -> bool:
        for expr in filter_expressions:
            if not evaluate_filter_expression(expr, row):
                return False
        return True
    return matches_filters

def evaluate_filter_expression(expr: str, row: dict) -> bool:
    """Evaluate a single filter expression against a row."""
    # Handle contains operator
    if ' contains ' in expr:
        field, value = expr.split(' contains ', 1)
        return value.strip('"\'') in str(row.get(field.strip(), ''))
    
    # Handle equality
    elif '==' in expr:
        field, value = expr.split('==', 1)
        return str(row.get(field.strip(), '')).strip() == value.strip().strip('"\'')
    
    # Handle inequality
    elif '!=' in expr:
        field, value = expr.split('!=', 1)
        return str(row.get(field.strip(), '')).strip() != value.strip().strip('"\'')
    
    return True

@click.command()
@click.option('--dataset', required=True, help='Dataset name for logging context')
@click.option('--results-folder', help='Path to the folder containing audit results files')
@click.option('--filter', 'filter_expressions', multiple=True, help='Filter expression like "SG Type==exome" or "File Path contains 2025-06-01"')
@click.option('--dry-run', is_flag=True, help='If set, will not delete objects.')
def main(dataset: str, results_folder: str, filter_expressions: tuple = (), dry_run: bool = False):
    """Main function to delete objects."""
    filter_func = parse_filter_expressions(list(filter_expressions)) if filter_expressions else None
    delete_from_audit_results(dataset, results_folder, filters=filter_func, dry_run=dry_run)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
