"""Report generation service for audit results."""

import csv
from datetime import datetime
from io import StringIO
import logging

from cpg_utils import Path, to_path

from ..data_access import GCSDataAccess

from ..models import (
    AuditResult,
    ReviewResult,
    DeletionResult,
    AuditReportEntry,
    SequencingGroup,
    AuditConfig,
)


class ReportGenerator:
    """Service for generating and writing audit reports."""

    def __init__(
        self,
        gcs_data: GCSDataAccess,
        timestamp: str | None = None,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the report generator.

        Args:
            storage_client: Google Cloud Storage client
            timestamp: Optional timestamp for report file prefix
            logger: Optional logger instance
        """
        self.gcs_data = gcs_data
        self.timestamp = timestamp or datetime.now().strftime('%Y-%m-%d_%H%M%S')
        self.logger = logger or logging.getLogger(__name__)

    def write_audit_reports(
        self,
        audit_result: AuditResult,
        config: AuditConfig,
    ):
        """
        Write all audit reports to GCS.

        Args:
            audit_result: The audit analysis results
            bucket_name: GCS bucket name for reports
            config: Audit configuration
            output_prefix: Optional prefix for output paths
        """
        output_dir = f'audit_results/{self.timestamp}'

        self._write_audit_config(f'{output_dir}/audit_config.txt', config)

        if audit_result.files_to_delete:
            self._write_csv_report(
                f'{output_dir}/files_to_delete.csv',
                audit_result.files_to_delete,
                'FILES TO DELETE',
            )

        if audit_result.files_to_review:
            self._write_csv_report(
                f'{output_dir}/files_to_review.csv',
                audit_result.files_to_review,
                'FILES TO REVIEW',
            )

        # Write moved files report
        if audit_result.moved_files:
            self._write_csv_report(
                f'{output_dir}/moved_files.csv',
                audit_result.moved_files,
                'MOVED FILES',
            )

        # Write unaligned SGs report
        if audit_result.unaligned_sequencing_groups:
            self._write_unaligned_sgs_report(
                f'{output_dir}/unaligned_sgs.tsv',
                audit_result.unaligned_sequencing_groups,
            )

        self.logger.info(
            f'Reports written to gs://{self.gcs_data.analysis_bucket}/{output_dir}'
        )

    def write_reviewed_files_report(
        self,
        review_result: ReviewResult,
    ) -> Path:
        """
        Write reviewed files report to GCS.

        Args:
            review_result: The review analysis results
            bucket: GCS bucket for reports
            output_prefix: Optional prefix for output paths
        """
        return self._write_csv_report(
            blob_path=f'audit_results/{self.timestamp}/reviewed_files.csv',
            entries=review_result.reviewed_files,
            report_name='FILES REVIEWED',
        )

    def write_deleted_files_report(
        self,
        deletion_result: DeletionResult,
    ) -> Path:
        """
        Write deleted files report to GCS.

        Args:
            deleted_files: The deleted files entries
            bucket: GCS bucket for reports
            output_prefix: Optional prefix for output paths
        """
        return self._write_csv_report(
            blob_path=f'audit_results/{self.timestamp}/deleted_files.csv',
            entries=deletion_result.deleted_files,
            report_name='FILES DELETED',
        )

    def _write_audit_config(self, blob_path: str, config: AuditConfig):
        """Write metadata about the audit config used in this run to a text file."""
        buffer = StringIO()

        lines = [
            f"Sequencing Types: {', '.join(sorted(config.sequencing_types))}",
            f"Sequencing Technologies: {', '.join(sorted(config.sequencing_technologies))}",
            f"Sequencing Platforms: {', '.join(sorted(config.sequencing_platforms))}",
            f"File Types: {', '.join(sorted(ft.name for ft in config.file_types))}",
            f"Analysis Types: {', '.join(sorted(config.analysis_types))}",
        ]
        if config.excluded_prefixes:
            lines.append(
                f"Excluded Prefixes: {', '.join(sorted(config.excluded_prefixes))}"
            )

        buffer.write('\n'.join(lines))

        blob = self.gcs_data.storage.get_blob(self.gcs_data.analysis_bucket, blob_path)
        self.gcs_data.storage.upload_from_buffer(blob, buffer)

        self.logger.info(
            f'Metadata report written to gs://{self.gcs_data.analysis_bucket}/{blob_path}'
        )

    def _write_csv_report(
        self,
        blob_path: str,
        entries: list[AuditReportEntry],
        report_name: str,
    ) -> Path:
        """Create or update a CSV report for audit entries."""
        self.logger.info(f'Writing {report_name} report.')

        fieldnames = entries[0].fieldnames()
        rows = [entry.to_report_dict() for entry in entries]
        if report_name == 'FILES TO DELETE':
            rows = sorted(rows, key=lambda x: x.get('SG ID', ''))

        blob = self.gcs_data.storage.get_blob(self.gcs_data.analysis_bucket, blob_path)
        output_path = f'gs://{self.gcs_data.analysis_bucket}/{blob_path}'
        if blob and blob.size > 0:
            self.logger.info(
                f'Existing report found, appending new rows to: {output_path}'
            )
            existing_data = blob.download_as_text(encoding='utf-8-sig')
            buffer = StringIO(existing_data)
            reader = csv.DictReader(buffer)
            rows = list(reader) + rows
            buffer.close()
        else:
            self.logger.info(f'Creating new report at: {output_path}')

        buffer = StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

        blob = self.gcs_data.storage.get_blob(self.gcs_data.analysis_bucket, blob_path)
        self.gcs_data.storage.upload_from_buffer(blob, buffer, 'text/csv')

        self.logger.info(
            f'{report_name}: {len(entries)} entries written to {output_path}'
        )

        return to_path(output_path)

    def _write_unaligned_sgs_report(
        self,
        blob_path: str,
        unaligned_sgs: list[SequencingGroup],
    ):
        """Write report of sequencing groups without completed CRAMs."""
        if not unaligned_sgs:
            self.logger.info('UNALIGNED SEQUENCING GROUPS - SKIPPED - No data to write')
            return

        buffer = StringIO()
        writer = csv.writer(buffer, delimiter='\t')

        # Write header
        writer.writerow(
            [
                'SG ID',
                'SG Type',
                'SG Technology',
                'SG Platform',
                'Sample ID',
                'Sample External ID',
                'Participant ID',
                'Participant External ID',
                'Assay Count',
                'Total Read Size',
            ]
        )

        # Write data
        for sg in unaligned_sgs:
            sample_ext_id = sg.sample.external_id or ', '.join(
                sg.sample.external_ids.values()
            )
            participant_ext_id = sg.sample.participant.external_id or ', '.join(
                sg.sample.participant.external_ids.values()
            )

            writer.writerow(
                [
                    sg.id,
                    sg.type,
                    sg.technology,
                    sg.platform,
                    sg.sample.id,
                    sample_ext_id,
                    sg.sample.participant.id,
                    participant_ext_id,
                    len(sg.assays),
                    sg.get_total_read_size(),
                ]
            )
        blob = self.gcs_data.storage.get_blob(self.gcs_data.analysis_bucket, blob_path)
        self.gcs_data.storage.upload_from_buffer(
            blob, buffer, 'text/tab-separated-values'
        )
        self.logger.info(
            f'UNALIGNED SEQUENCING GROUPS: {len(unaligned_sgs)} entries written to '
            f'gs://{self.gcs_data.analysis_bucket}/{blob_path}'
        )

    def get_report_rows(self, report_path: Path) -> list[AuditReportEntry]:
        """Retrieve rows from the audit report CSV."""
        if not report_path.exists():
            self.logger.error(f'Report file {report_path} does not exist.')
            return []

        with report_path.open('r') as f:
            reader = csv.DictReader(f)
            rows = [AuditReportEntry(**row) for row in reader]
        return rows

    def get_report_stats(self, report_path: Path) -> dict:
        """Count the rows and sum the total file size for all rows in the report"""
        rows = self.get_report_rows(report_path)
        total_size = sum(entry.filesize or 0 for entry in rows)
        return {'total_size': total_size, 'file_count': len(rows)}

    def generate_summary_statistics(self, audit_result: AuditResult) -> dict[str, int]:
        """
        Generate summary statistics from audit results.

        Args:
            audit_result: The audit analysis results

        Returns:
            Dictionary of statistics
        """
        total_size_to_delete = sum(
            entry.filesize or 0
            for entry in audit_result.files_to_delete + audit_result.moved_files
        )

        total_size_to_review = sum(
            entry.filesize or 0 for entry in audit_result.files_to_review
        )

        return {
            'files_to_delete': len(audit_result.files_to_delete),
            'files_to_delete_size_gb': total_size_to_delete / (1024**3),
            'moved_files': len(audit_result.moved_files),
            'files_to_review': len(audit_result.files_to_review),
            'files_to_review_size_gb': total_size_to_review / (1024**3),
            'unaligned_sgs': len(audit_result.unaligned_sequencing_groups),
        }
