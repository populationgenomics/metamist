"""CLI entry point for deleting files in audit results."""

import click

from metamist.audit.data_access import GCSDataAccess, MetamistDataAccess
from metamist.audit.models import AuditReportEntry, DeletionResult
from metamist.audit.services import AuditLogs, ReportGenerator

from cpg_utils import to_path


def delete_from_audit_results(
    dataset: str,
    gcp_project: str | None,
    results_folder: str,
    report_name: str,
    dry_run: bool = False,
):
    """Delete files listed in the audit results."""
    audit_logs = AuditLogs(dataset, 'audit_deletions')

    gcs = GCSDataAccess(dataset, gcp_project=gcp_project)
    reporter = ReportGenerator(gcs, results_folder, audit_logs)

    report_rows = reporter.get_report_rows(report_name)

    deletion_result = delete_files_from_report(
        gcs, report_name, report_rows, dry_run, audit_logs
    )

    deletion_report = reporter.write_deleted_files_report(deletion_result)
    stats = reporter.get_report_stats(deletion_report)

    if not dry_run:
        upsert_deleted_files_analysis(
            dataset, report_name, deletion_report.as_uri(), stats, audit_logs
        )

    audit_logs.info_nl(f'Finished processing deletes from report: {report_name}')


def delete_files_from_report(
    gcs: GCSDataAccess,
    report_name: str,
    rows: list[AuditReportEntry],
    dry_run: bool,
    audit_logs: AuditLogs,
) -> DeletionResult:
    """
    Delete files in the upload bucket from the audit results.
    Only delete files from rows marked with 'DELETE' action.

    If deleting from 'files_to_delete', automatically add this action.
    """
    to_delete: list[AuditReportEntry] = []
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


async def upsert_deleted_files_analysis(
    dataset: str,
    audited_report_name: str,
    deletion_report_path: str,
    stats: dict,
    audit_logs: AuditLogs,
):
    """
    Inserts or updates an analysis record for deleted files.
    Populates the meta dict with deleted files stats.
    """
    audit_logs.info_nl('Upserting analysis record for deleted files report.')
    if existing_analysis := await MetamistDataAccess().get_audit_deletion_analysis(
        dataset, deletion_report_path
    ):
        audit_logs.info_nl(
            f'Found existing analysis record (ID: {existing_analysis["id"]}) for {deletion_report_path}. Updating...'
        )
        await MetamistDataAccess().update_audit_deletion_analysis(
            existing_analysis, audited_report_name, stats
        )
        audit_logs.info_nl(f'Updated analysis {existing_analysis["id"]}.')

    else:
        audit_logs.info_nl(f'Creating new record for {deletion_report_path}')
        aid = await MetamistDataAccess().create_audit_deletion_analysis(
            dataset, audited_report_name, deletion_report_path, stats
        )
        audit_logs.info_nl(f'Created analysis {aid}.')


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
    delete_from_audit_results(
        dataset, gcp_project, results_folder, report_name, dry_run
    )


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
