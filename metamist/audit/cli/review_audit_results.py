"""CLI entry point for reviewing audit results."""

import logging
import csv

import click

from ..adapters import StorageClient
from ..data_access.gcs_data_access import GCSDataAccess

from cpg_utils import to_path
from cpg_utils.config import config_retrieve


def setup_logger(dataset: str) -> logging.Logger:
    """Set up logger for audit reviews."""
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


def review_audit_results(
    dataset: str,
    results_folder: str,
    report: str = 'files_to_review',
    comment: str = '',
    filter_func: callable = None,
):
    """Review files listed in the audit results."""
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

    reviewed_rows = review_filtered_files(rows, filter_func, comment, logger)
    logger.info(f'Added comments to {len(reviewed_rows)} / {len(rows)} files.')

    write_reviewed_files_report(reviewed_rows, bucket_name, results_folder, logger)


def review_filtered_files(
    rows: list[dict],
    filter_func: callable,
    comment: str,
    logger: logging.Logger,
):
    """Review files from audit results based on filters and comments."""
    logger.info(f'Annotating rows with comment: {comment}')
    reviewed_rows = []
    for row in rows:
        if filter_func and not filter_func(row):
            logger.info(f'Skipping {row["File Path"]} due to filters: {filter_func}')
            continue

        file_path = to_path(row['File Path'])
        if not file_path.exists():
            logger.warning(f'File {file_path} does not exist, skipping review.')
            continue

        reviewed_rows.append(row | {'Review Comment': comment})


def write_reviewed_files_report(
    reviewed_files_rows: list[dict],
    bucket_name: str,
    results_folder: str,
    logger: logging.Logger,
):
    """Write a report of reviewed files."""
    if not reviewed_files_rows:
        logger.info('No files reviewed, skipping report.')
        return

    report_path = to_path(f'gs://{bucket_name}/{results_folder}/reviewed_files.csv')
    # If the report exists, append to it. If not, create it and write the header.
    if not report_path.exists():
        logger.info(f'Creating new report {report_path}')
        with report_path.open('w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=reviewed_files_rows[0].keys())
            writer.writeheader()
            writer.writerows(reviewed_files_rows)
    else:
        logger.info(f'Appending to existing report {report_path}')
        with report_path.open('a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=reviewed_files_rows[0].keys())
            writer.writerows(reviewed_files_rows)

    logger.info(f'Reviewed files report written to {report_path}')



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
    if '==' in expr:
        field, value = expr.split('==', 1)
        return str(row.get(field.strip(), '')).strip() == value.strip().strip('"\'')

    # Handle inequality
    if '!=' in expr:
        field, value = expr.split('!=', 1)
        return str(row.get(field.strip(), '')).strip() != value.strip().strip('"\'')

    return True


@click.command()
@click.option('--dataset', required=True, help='Dataset name for logging context')
@click.option(
    '--results-folder', help='Path to the folder containing audit results files'
)
@click.option('--comment', required=True, help='Comment to add to reviewed files.')
@click.option(
    '--filter',
    'filter_expressions',
    multiple=True,
    help='Filter expression like "SG Type==exome" or "File Path contains 2025-06-01"',
)
def main(
    dataset: str,
    results_folder: str,
    comment: str,
    filter_expressions: tuple = (),
):
    """Main function to review objects."""
    filter_func = (
        parse_filter_expressions(list(filter_expressions))
        if filter_expressions
        else None
    )
    review_audit_results(
        dataset, results_folder, comment=comment, filter_func=filter_func
    )


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
