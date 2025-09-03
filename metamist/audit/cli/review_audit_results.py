"""CLI entry point for reviewing audit results."""

import click

from metamist.audit.data_access.gcs_data_access import GCSDataAccess
from metamist.audit.models import AuditReportEntry, ReviewResult
from metamist.audit.services import AuditLogs, ReportGenerator

from cpg_utils import to_path


def review_audit_report(
    dataset: str,
    gcp_project: str,
    results_folder: str,
    report: str = 'files_to_review',
    action: str = 'DELETE',
    comment: str = '',
    filter_func: callable = None,
):
    """Review files listed in the audit results."""
    audit_logs = AuditLogs(dataset, 'audit_review')

    gcs_data = GCSDataAccess(dataset, gcp_project)
    reporter = ReportGenerator(gcs_data, results_folder, audit_logs)

    rows = reporter.get_report_rows_from_name(report)

    review_result = review_filtered_files(
        rows, filter_func, action, comment, audit_logs.logger
    )
    audit_logs.info(
        f'Added comments to {len(review_result.reviewed_files)} / {len(rows)} files.'
    )

    reporter.write_reviewed_files_report(review_result)


def review_filtered_files(
    rows: list[AuditReportEntry],
    filter_func: callable,
    action: str,
    comment: str,
    audit_logs: AuditLogs,
) -> ReviewResult:
    """Review files from audit results based on filters and comments."""
    audit_logs.info(f'Annotating rows with comment: {comment}')
    reviewed_rows: list[AuditReportEntry] = []
    for row in rows:
        if filter_func and not filter_func(row):
            audit_logs.info(
                f'Skipping {row["File Path"]} due to filters: {filter_func}'
            )
            continue

        file_path = to_path(row['File Path'])
        if not file_path.exists():
            audit_logs.warning(f'File {file_path} does not exist, skipping review.')
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
@click.option('--gcp-project', required=True, help='GCP project ID')
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
    gcp_project: str,
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
        dataset,
        gcp_project,
        results_folder,
        action=action,
        comment=comment,
        filter_func=filter_func,
    )


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
