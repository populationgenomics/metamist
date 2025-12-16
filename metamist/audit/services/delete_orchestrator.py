"""
Class for orchestrating the upload bucket audit deletion process.
"""

from metamist.audit.data_access import GCSDataAccess, MetamistDataAccess
from metamist.audit.models import AuditReportEntry, DeletionResult
from metamist.audit.services import BucketAuditLogger, Reporter

from cpg_utils import Path, to_path


class AuditDeleteOrchestrator:
    """
    A class that manages and orchestrates the audit deletion process.
    """

    def __init__(
        self,
        dataset: str,
        results_folder: str,
        report: str,
    ):
        self.dataset = dataset
        self.results_folder = results_folder
        self.report = report

        self.audit_logger = BucketAuditLogger(self.dataset, 'audit_deletions')
        self.gcs = GCSDataAccess(self.dataset)
        self.reporter = Reporter(self.gcs, self.audit_logger, self.results_folder)

    def delete_from_audit_results(
        self,
        dry_run: bool = False,
    ):
        """Delete files listed in the audit results."""

        self.audit_logger.info_nl(f"Reading report '{self.report}'".center(50, '~'))

        report_rows = self.reporter.get_report_rows_from_name(self.report)

        deletion_result = self.delete_files_from_report(report_rows, dry_run)

        deletion_report = self.reporter.write_deleted_files_report(
            deletion_result, dry_run
        )
        stats = self.reporter.get_report_entries_stats(deletion_result.deleted_files)

        if not dry_run:
            self.upsert_deleted_files_analysis(self.report, deletion_report, stats)

        all_stats = self.reporter.get_report_stats(deletion_report)

        self.audit_logger.info_nl(
            f"Finished processing deletes from report: '{self.report}'"
        )
        self.audit_logger.info_nl(
            f'New deletions: {stats["file_count"]} files, {stats["total_size"] / (1024**3):.2f} GiB'
        )
        self.audit_logger.info_nl(
            f'Total deletions: {all_stats["file_count"]} files, {all_stats["total_size"] / (1024**3):.2f} GiB'
        )

    def delete_files_from_report(
        self,
        rows: list[AuditReportEntry],
        dry_run: bool,
    ) -> DeletionResult:
        """
        Delete files in the upload bucket from the audit results.
        Only delete files from rows marked with 'DELETE' action.

        If deleting from 'files_to_delete', automatically add this action.
        """
        to_delete: list[AuditReportEntry] = []
        for row in rows:
            if not row.action or row.action.upper() != 'DELETE':
                continue

            if not row.review_comment:
                raise ValueError(
                    "Review Comment is required for rows outside the 'files_to_delete' report. "
                    'Use `review_audit_results.py` to add comments justifying the deletion, then try again.'
                )

            file_path = to_path(row.filepath)
            if not file_path.exists():
                self.audit_logger.warning(
                    f'File {file_path} does not exist, skipping deletion.'
                )
                continue

            to_delete.append(row)

        total_bytes = sum(int(row.filesize) for row in to_delete)
        if dry_run:
            self.audit_logger.info_nl(
                f'Dry run: would have deleted {len(to_delete)} files ({total_bytes / (1024**3):.2f} GiB)'
            )
        else:
            self.gcs.delete_blobs(
                self.gcs.upload_bucket,
                [to_path(row.filepath).blob for row in to_delete],
            )
            self.audit_logger.info_nl(
                f'Deleted: {len(to_delete)} files ({total_bytes / (1024**3):.2f} GiB)'
            )

        return DeletionResult(deleted_files=to_delete)

    def upsert_deleted_files_analysis(
        self,
        audited_report_name: str,
        deletion_report: Path,
        stats: dict,
    ):
        """
        Inserts or updates an analysis record for deleted files.
        Populates the meta dict with deleted files stats.
        """
        self.audit_logger.info_nl('Upserting analysis record for deleted files report.')
        if existing_analysis := MetamistDataAccess().get_audit_deletion_analysis(
            self.dataset, str(deletion_report)
        ):
            self.audit_logger.info_nl(
                f'Found existing analysis record (ID: {existing_analysis["id"]}) for {deletion_report}. Updating...'
            )
            MetamistDataAccess().update_audit_deletion_analysis(
                existing_analysis, audited_report_name, stats
            )
            self.audit_logger.info_nl(f'Updated analysis {existing_analysis["id"]}.')

        else:
            self.audit_logger.info_nl(f'Creating new record for {deletion_report}')
            cohort_name = f'{self.dataset}_{self.results_folder}'
            aid = MetamistDataAccess().create_audit_deletion_analysis(
                self.dataset,
                cohort_name,
                audited_report_name,
                str(deletion_report),
                stats,
            )
            self.audit_logger.info_nl(f'Created analysis {aid}.')
