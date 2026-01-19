"""CLI entry point for deleting files in audit results."""

import click

from metamist.audit.data_access import GCSDataAccess, MetamistDataAccess
from metamist.audit.models import AuditReportEntry, DeletionResult
from metamist.audit.services import BucketAuditLogger, Reporter

from cpg_utils import Path, to_path


def delete_from_audit_results(
    dataset: str,
    results_folder: str,
    report: str,
    dry_run: bool = False,
):
    """Delete files listed in the audit results."""
    audit_logs = BucketAuditLogger(dataset, 'audit_deletions')

    gcs = GCSDataAccess(dataset)
    reporter = Reporter(gcs, audit_logs, results_folder)

    audit_logs.info_nl(f"Reading report '{report}'".center(50, '~'))

    report_rows = reporter.get_report_rows_from_name(report)

    deletion_result = delete_files_from_report(
        gcs, report, report_rows, audit_logs, dry_run
    )

    deletion_report = reporter.write_deleted_files_report(deletion_result, dry_run)
    stats = reporter.get_report_entries_stats(deletion_result.deleted_files)

    if not dry_run:
        upsert_deleted_files_analysis(
            dataset, report, results_folder, deletion_report, stats, audit_logs
        )

    all_stats = reporter.get_report_stats(deletion_report)

    audit_logs.info_nl(f"Finished processing deletes from report: '{report}'")
    audit_logs.info_nl(
        f'New deletions: {stats["file_count"]} files, {stats["total_size"] / (1024**3):.2f} GiB'
    )
    audit_logs.info_nl(
        f'Total deletions: {all_stats["file_count"]} files, {all_stats["total_size"] / (1024**3):.2f} GiB'
    )


# TODO - make sure we're tracking exactly which report the deletions came from
# as the reviewed_files report could be updated many times with different actions/comments
# and we want to track exactly which version of the report the deletions came from


# TODO - make this script agnostic of the report name, all actions and review comments should
# be handled in the review_audit_results.py script. This script should just delete files
# marked for deletion, and error if any files are not marked for deletion or missing comments.
# This will make it easier to maintain and reason about.
def delete_files_from_report(
    gcs: GCSDataAccess,
    report_name: str,
    rows: list[AuditReportEntry],
    audit_logs: BucketAuditLogger,
    dry_run: bool,
) -> DeletionResult:
    """
    Delete files in the upload bucket from the audit results.
    Only delete files from rows marked with 'DELETE' action.

    If deleting from 'files_to_delete', automatically add this action.
    """
    to_delete: list[AuditReportEntry] = []
    for row in rows:
        # TODO move this into the files_to_delete report generation
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
            audit_logs.warning(f'File {file_path} does not exist, skipping deletion.')
            continue

        to_delete.append(row)

    total_bytes = sum(int(row.filesize) for row in to_delete)
    if dry_run:
        audit_logs.info_nl(
            f'Dry run: would have deleted {len(to_delete)} files ({total_bytes / (1024**3):.2f} GiB)'
        )
    else:
        gcs.delete_blobs(
            gcs.upload_bucket,
            [to_path(row.filepath).blob for row in to_delete],
        )
        audit_logs.info_nl(
            f'Deleted: {len(to_delete)} files ({total_bytes / (1024**3):.2f} GiB)'
        )

    return DeletionResult(deleted_files=to_delete)


def upsert_deleted_files_analysis(
    dataset: str,
    audited_report_name: str,
    results_folder: str,
    deletion_report: Path,
    stats: dict,
    audit_logs: BucketAuditLogger,
):
    """
    Inserts or updates an analysis record for deleted files.
    Populates the meta dict with deleted files stats.
    """
    audit_logs.info_nl('Upserting analysis record for deleted files report.')
    if existing_analysis := MetamistDataAccess().get_audit_deletion_analysis(
        dataset, str(deletion_report)
    ):
        audit_logs.info_nl(
            f'Found existing analysis record (ID: {existing_analysis["id"]}) for {deletion_report}. Updating...'
        )
        MetamistDataAccess().update_audit_deletion_analysis(
            existing_analysis, audited_report_name, stats
        )
        audit_logs.info_nl(f'Updated analysis {existing_analysis["id"]}.')

    else:
        audit_logs.info_nl(f'Creating new record for {deletion_report}')
        cohort_name = f'{dataset}_{results_folder}'
        aid = MetamistDataAccess().create_audit_deletion_analysis(
            dataset, cohort_name, audited_report_name, str(deletion_report), stats
        )
        audit_logs.info_nl(f'Created analysis {aid}.')


@click.command()
@click.option('--dataset', '-d', required=True, help='Dataset name')
@click.option('--results-folder', '-r', required=True, help='Audit results folder')
@click.option(
    '--report-name',
    '-n',
    help="The report to delete files from. e.g. 'files_to_delete'.",
    default='files_to_delete',
)
@click.option('--dry-run', is_flag=True, help='If set, will not delete objects.')
def main(
    dataset: str,
    results_folder: str,
    report_name: str = 'files_to_delete',
    dry_run: bool = False,
):
    """
    Reads the report at location:
        gs://cpg-dataset-main-analysis/audit_results/{results_folder}/{report_name}.csv

    If the report is 'files_to_delete', it will delete the files listed in the report.

    For any other report, files must be marked for deletion in the report, alongside a
    comment justifying the deletion.

    Deleting files will create or update an analysis record of type 'audit_deletions'.
    """
    delete_from_audit_results(dataset, results_folder, report_name, dry_run)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
