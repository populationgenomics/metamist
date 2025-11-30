"""CLI entry point for reviewing audit results."""

import click

from metamist.audit.data_access.gcs_data_access import GCSDataAccess
from metamist.audit.models import AuditReportEntry, ReviewResult
from metamist.audit.services import BucketAuditLogger, Reporter

from cpg_utils import to_path

ACTIONS = ['DELETE', 'INGEST', 'REVIEW']


def review_audit_report(
    dataset: str,
    results_folder: str,
    report: str = 'files_to_review',
    action: str = 'DELETE',
    comment: str = '',
    filter_expressions: list[str] | None = None,
):
    """Review files listed in the audit results."""
    audit_logs = BucketAuditLogger(dataset, 'audit_review')

    gcs_data = GCSDataAccess(dataset)
    reporter = Reporter(gcs_data, audit_logs, results_folder)

    audit_logs.info_nl(f"Reading report '{report}'".center(50, '~'))
    rows = reporter.get_report_rows_from_name(report)

    rows = reporter.filter_rows(filter_expressions, rows)
    review_result = review_rows(rows, action, comment, audit_logs)
    audit_logs.info_nl(
        f'Added comments to {len(review_result.reviewed_files)} / {len(rows)} files.'
    )
    if review_result.reviewed_files:
        reporter.write_reviewed_files_report(review_result)

    audit_logs.info_nl('Review complete!')


def review_rows(
    rows: list[AuditReportEntry],
    action: str,
    comment: str,
    audit_logs: BucketAuditLogger,
) -> ReviewResult:
    """Review files from audit results based on filters and comments."""
    action = action.upper()
    if action not in ACTIONS:
        raise ValueError(f'Invalid action: {action}. Must be one of {ACTIONS}.')
    audit_logs.info_nl(f"Annotating rows with comment: '{comment}'")
    reviewed_rows: list[AuditReportEntry] = []
    for row in rows:
        file_path = to_path(row.filepath)
        if not file_path.exists():
            audit_logs.warning(f'File {file_path} does not exist, skipping review.')
            continue

        row.update_action(action)
        row.update_review_comment(comment)
        reviewed_rows.append(row)

    return ReviewResult(reviewed_files=reviewed_rows)


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
    review_audit_report(
        dataset=dataset,
        results_folder=results_folder,
        action=action,
        comment=comment,
        filter_expressions=list(filter_expressions),
    )


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
