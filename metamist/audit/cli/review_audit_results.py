"""CLI entry point for reviewing audit results."""

import click

from metamist.audit.data_access.gcs_data_access import GCSDataAccess
from metamist.audit.models import AuditReportEntry, ReviewResult
from metamist.audit.services import AuditLogs, ReportGenerator

from cpg_utils import to_path

ACTIONS = ['DELETE', 'INGEST', 'REVIEW']


def review_audit_report(
    dataset: str,
    results_folder: str,
    report: str = 'files_to_review',
    action: str = 'DELETE',
    comment: str = '',
    filter_func: callable = None,
):
    """Review files listed in the audit results."""
    audit_logs = AuditLogs(dataset, 'audit_review')

    gcs_data = GCSDataAccess(dataset)
    reporter = ReportGenerator(gcs_data, audit_logs, results_folder)

    audit_logs.info_nl(f"Reading report '{report}'".center(50, '~'))
    rows = reporter.get_report_rows_from_name(report)

    review_result = review_filtered_files(
        rows, filter_func, action, comment, audit_logs
    )
    audit_logs.info_nl(
        f'Added comments to {len(review_result.reviewed_files)} / {len(rows)} files.'
    )
    if review_result.reviewed_files:
        reporter.write_reviewed_files_report(review_result)

    audit_logs.info_nl('Review complete!')


def review_filtered_files(
    rows: list[AuditReportEntry],
    filter_func: callable,
    action: str,
    comment: str,
    audit_logs: AuditLogs,
) -> ReviewResult:
    """Review files from audit results based on filters and comments."""
    audit_logs.info_nl(f"Annotating rows with comment: '{comment}'")
    reviewed_rows: list[AuditReportEntry] = []
    for row in rows:
        if filter_func and not filter_func(row.to_report_dict()):
            # Skip row due to filter(s)
            continue

        file_path = to_path(row.filepath)
        if not file_path.exists():
            audit_logs.warning(f'File {file_path} does not exist, skipping review.')
            continue

        row.update_action(action)
        row.update_review_comment(comment)
        reviewed_rows.append(row)

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
    # Recursive cases first
    if ' or ' in expr:
        # Evaluate either side of the 'or'
        left, right = expr.split(' or ', 1)
        return evaluate_filter_expression(
            left.strip(), row
        ) or evaluate_filter_expression(right.strip(), row)

    if ' and ' in expr:
        # Evaluate either side of the 'and'
        left, right = expr.split(' and ', 1)
        return evaluate_filter_expression(
            left.strip(), row
        ) and evaluate_filter_expression(right.strip(), row)

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
@click.option('--dataset', '-d', required=True, help='Dataset name for logging context')
@click.option(
    '--results-folder', '-r', help='Path to the folder containing audit results files'
)
@click.option(
    '--action', '-a', default='DELETE', help='Action to perform on reviewed files.'
)
@click.option(
    '--comment', '-c', required=True, help='Comment to add to reviewed files.'
)
@click.option(
    '--filter',
    '-f',
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
    action = action.upper()
    if action not in ACTIONS:
        raise ValueError(f'Invalid action: {action}. Must be one of {ACTIONS}.')
    filter_func = (
        parse_filter_expressions(list(filter_expressions))
        if filter_expressions
        else None
    )
    review_audit_report(
        dataset=dataset,
        results_folder=results_folder,
        action=action,
        comment=comment,
        filter_func=filter_func,
    )


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
