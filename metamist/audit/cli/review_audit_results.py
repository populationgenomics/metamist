"""CLI entry point for reviewing audit results."""

import logging
import csv

import click

from ..data_access.gcs_data_access import GCSDataAccess
from ..models import AuditReportEntry, ReviewResult
from ..services import AuditLogger, ReportGenerator

from cpg_utils import to_path
from cpg_utils.config import config_retrieve


def review_audit_report(
    dataset: str,
    results_folder: str,
    report: str = 'files_to_review',
    action: str = 'DELETE',
    comment: str = '',
    filter_func: callable = None,
):
    """Review files listed in the audit results."""
    logger = AuditLogger(dataset, 'audit_review').logger

    gcp_project = config_retrieve(['workflow', dataset, 'gcp_project'])
    if not gcp_project:
        raise ValueError('GCP project is required')

    gcs_data = GCSDataAccess(dataset, gcp_project)
    report_generator = ReportGenerator(gcs_data, results_folder, logger)

    report_path = to_path(
        f'gs://{gcs_data.analysis_bucket}/{results_folder}/{report}.csv'
    )
    if not report_path.exists():
        logger.error(f'Report file {report_path} does not exist.')
        return

    with report_path.open('r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    review_result = review_filtered_files(rows, filter_func, action, comment, logger)
    logger.info(
        f'Added comments to {len(review_result.reviewed_files)} / {len(rows)} files.'
    )

    report_generator.write_reviewed_files_report(review_result)


def review_filtered_files(
    rows: list[AuditReportEntry],
    filter_func: callable,
    action: str,
    comment: str,
    logger: logging.Logger,
) -> ReviewResult:
    """Review files from audit results based on filters and comments."""
    logger.info(f'Annotating rows with comment: {comment}')
    reviewed_rows: list[AuditReportEntry] = []
    for row in rows:
        if filter_func and not filter_func(row):
            logger.info(f'Skipping {row["File Path"]} due to filters: {filter_func}')
            continue

        file_path = to_path(row['File Path'])
        if not file_path.exists():
            logger.warning(f'File {file_path} does not exist, skipping review.')
            continue

        row.update_action(action)
        row.update_review_comment(comment)

        reviewed_rows.append(
            AuditReportEntry(**{**row, 'action': action, 'review_comment': comment})
        )

    return ReviewResult(reviewed_files=reviewed_rows)


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
@click.option('--action', default='DELETE', help='Action to perform on reviewed files.')
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
    action: str,
    comment: str,
    filter_expressions: tuple = (),
):
    """Main function to review objects."""
    filter_func = (
        parse_filter_expressions(list(filter_expressions))
        if filter_expressions
        else None
    )
    review_audit_report(
        dataset, results_folder, action=action, comment=comment, filter_func=filter_func
    )


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
