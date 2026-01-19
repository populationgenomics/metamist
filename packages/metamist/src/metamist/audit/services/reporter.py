"""Report generation service for audit results."""

import csv
from datetime import datetime
from io import StringIO

from cpg_utils import Path, to_path

from .audit_logging import BucketAuditLogger
from metamist.audit.data_access import GCSDataAccess
from metamist.audit.models import (
    AuditResult,
    ReviewResult,
    DeletionResult,
    AuditReportEntry,
    SequencingGroup,
    AuditConfig,
)


class Reporter:
    """Service for generating and writing audit reports."""

    def __init__(
        self,
        gcs_data: GCSDataAccess,
        audit_logs: BucketAuditLogger,
        timestamp: str | None = None,
    ):
        """
        Initialize the report generator.

        Args:
            storage_client: Google Cloud Storage client
            timestamp: Optional timestamp for report file prefix
            audit_logs: Audit logs instance
        """
        self.gcs_data = gcs_data
        self.audit_logs = audit_logs
        self.timestamp = timestamp or datetime.now().strftime('%Y-%m-%d_%H%M%S')
        self.output_dir = f'audit_results/{self.timestamp}'

    # Reading reports #
    def get_report_rows(self, report_path: Path) -> list[AuditReportEntry]:
        """Retrieve rows from the audit report CSV."""
        if not report_path.exists():
            self.audit_logs.error(f'Report file {report_path} does not exist.')
            return []

        with report_path.open('r') as f:
            reader = csv.DictReader(f, fieldnames=AuditReportEntry().fieldnames())
            next(reader)  # Skip header row
            rows = [AuditReportEntry(**row) for row in reader]

        return rows

    def get_report_rows_from_name(self, name: str) -> list[AuditReportEntry]:
        """Retrieve rows from the audit report CSV by name."""
        report_path = to_path(
            f'gs://{self.gcs_data.analysis_bucket}/audit_results/{self.timestamp}/{name}.csv'
        )
        return self.get_report_rows(report_path)

    def get_report_stats(self, report_path: Path) -> dict:
        """Count the rows and sum the total file size for all rows in the report"""
        rows = self.get_report_rows(report_path)
        return self.get_report_entries_stats(rows)

    def get_report_entries_stats(self, entries: list[AuditReportEntry]) -> dict:
        """Get statistics for the given report entries."""
        total_size = sum(int(entry.filesize) or 0 for entry in entries)
        return {'total_size': total_size, 'file_count': len(entries)}

    # Writing reports #
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
        self.audit_logs.info_nl(
            f'Target: gs://{self.gcs_data.analysis_bucket}/{self.output_dir}/'
        )

        self._write_audit_config(f'{self.output_dir}/audit_config.txt', config)

        if audit_result.files_to_delete:
            self._write_csv_report(
                f'{self.output_dir}/files_to_delete.csv',
                audit_result.files_to_delete,
                'FILES TO DELETE',
            )

        if audit_result.files_to_review:
            self._write_csv_report(
                f'{self.output_dir}/files_to_review.csv',
                audit_result.files_to_review,
                'FILES TO REVIEW',
            )

        if audit_result.moved_files:
            self._write_csv_report(
                f'{self.output_dir}/moved_files.csv',
                audit_result.moved_files,
                'MOVED FILES',
            )

        if audit_result.unaligned_sequencing_groups:
            self._write_unaligned_sgs_report(
                f'{self.output_dir}/unaligned_sgs.tsv',
                audit_result.unaligned_sequencing_groups,
            )

    def _write_audit_config(self, blob_path: str, config: AuditConfig):
        """Write metadata about the audit config used in this run to a text file."""
        buffer = StringIO()

        lines = [
            f'Dataset: {config.dataset}',
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
        lines.append('')

        buffer.write('\n'.join(lines))

        blob = self.gcs_data.storage.get_blob(self.gcs_data.analysis_bucket, blob_path)
        self.gcs_data.storage.upload_from_buffer(blob, buffer)

        self.audit_logs.info_nl(
            f'{"AUDIT CONFIG":<20}: gs://{self.gcs_data.analysis_bucket}/{blob_path}'
        )

    def _write_csv_report(
        self,
        blob_path: str,
        entries: list[AuditReportEntry],
        report_name: str,
    ) -> Path:
        """Create or update a CSV report for audit entries."""
        rows = [entry.to_report_dict() for entry in entries]
        if report_name == 'FILES TO DELETE':
            rows = sorted(rows, key=lambda x: x.get('SG ID', ''))

        blob = self.gcs_data.storage.get_blob(self.gcs_data.analysis_bucket, blob_path)
        output_path = f'gs://{self.gcs_data.analysis_bucket}/{blob_path}'
        if blob.exists() and blob.size > 0:
            self.audit_logs.info_nl(
                f'Existing report found, appending new rows to: {output_path}'
            )
            existing_data = blob.download_as_text(encoding='utf-8-sig')
            buffer = StringIO(existing_data)
            reader = csv.DictReader(buffer)
            existing_rows = list(reader)
            # Prevent duplicates
            rows = existing_rows + [row for row in rows if row not in existing_rows]
            buffer.close()

        buffer = StringIO()
        writer = csv.DictWriter(buffer, fieldnames=AuditReportEntry().fieldnames())
        writer.writeheader()
        writer.writerows(rows)

        blob = self.gcs_data.storage.get_blob(self.gcs_data.analysis_bucket, blob_path)
        self.gcs_data.storage.upload_from_buffer(blob, buffer, 'text/csv')

        self.audit_logs.info_nl(
            f'{report_name:<20}: {output_path} ({len(entries)} entries)'
        )

        return to_path(output_path)

    def write_csv_report(
        self,
        blob_path: str,
        entries: list[AuditReportEntry],
        report_name: str,
    ) -> Path:
        """Write a CSV report to GCS."""
        return self._write_csv_report(
            blob_path=blob_path,
            entries=entries,
            report_name=report_name,
        )

    def _write_log_file(self, log_file: str, save_path: str):
        """Write audit logs to a file."""
        # Upload the log file to GCS
        blob = self.gcs_data.storage.get_blob(self.gcs_data.analysis_bucket, save_path)
        blob.upload_from_filename(log_file)
        return to_path(f'gs://{self.gcs_data.analysis_bucket}/{save_path}')

    def write_log_file(self, log_file: str, save_path: str) -> Path:
        """Write audit logs to a file."""
        return self._write_log_file(log_file, save_path)

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
        self, deletion_result: DeletionResult, dry_run: bool
    ) -> Path:
        """
        Write deleted files report to GCS.

        Args:
            deleted_files: The deleted files entries
            bucket: GCS bucket for reports
            dry_run: If True, will write to a dry run report
        """
        if dry_run:
            blob_path = f'audit_results/{self.timestamp}/deleted_files_dry_run.csv'
        else:
            blob_path = f'audit_results/{self.timestamp}/deleted_files.csv'
        return self._write_csv_report(
            blob_path=blob_path,
            entries=deletion_result.deleted_files,
            report_name='FILES DELETED',
        )

    def _write_unaligned_sgs_report(
        self,
        blob_path: str,
        unaligned_sgs: list[SequencingGroup],
    ):
        """Write report of sequencing groups without completed CRAMs."""
        if not unaligned_sgs:
            self.audit_logs.info_nl(
                'UNALIGNED SEQUENCING GROUPS - SKIPPED - No data to write'
            )
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
        self.audit_logs.info_nl(
            f'{"UNALIGNED SGS":<20}: gs://{self.gcs_data.analysis_bucket}/{blob_path} ({len(unaligned_sgs)} entries)'
        )

    # Summary statistics #
    def generate_summary_statistics(self, audit_result: AuditResult) -> dict[str, int]:
        """
        Generate summary statistics from audit results.

        Args:
            audit_result: The audit analysis results

        Returns:
            Dictionary of statistics
        """
        total_size_to_delete = sum(
            entry.filesize or 0 for entry in audit_result.files_to_delete
        )

        total_size_to_review = sum(
            entry.filesize or 0 for entry in audit_result.files_to_review
        )

        return {
            'files_to_delete': len(audit_result.files_to_delete),
            'files_to_delete_size_gb': total_size_to_delete / (1024**3),
            'files_to_review': len(audit_result.files_to_review),
            'files_to_review_size_gb': total_size_to_review / (1024**3),
            'unaligned_sgs': len(audit_result.unaligned_sequencing_groups),
        }

    # Filtering #
    def filter_rows(
        self, filter_expressions: list[str], rows: list[AuditReportEntry]
    ) -> list[AuditReportEntry]:
        """Filter report rows based on filter expressions."""
        if not filter_expressions:
            return rows

        filter_func = self.parse_filter_expressions(filter_expressions)

        filtered_rows = [
            row
            for row in rows
            if filter_func(row.to_report_dict(use_external_headers=False))
        ]
        self.audit_logs.info_nl(f'Filtered rows: {len(filtered_rows)} / {len(rows)}.')
        return filtered_rows

    def parse_filter_expressions(self, filter_expressions: list[str]) -> callable:
        """Parse filter expressions into a callable filter function."""

        def matches_filters(row: dict) -> bool:
            for expr in filter_expressions:
                if not evaluate_filter_expression(expr, row):
                    return False
            return True

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
                return str(row.get(field.strip(), '')).strip() == value.strip().strip(
                    '"\''
                )

            # Handle inequality
            if '!=' in expr:
                field, value = expr.split('!=', 1)
                return str(row.get(field.strip(), '')).strip() != value.strip().strip(
                    '"\''
                )
            if '>=' in expr:
                field, value = expr.split('>=', 1)
                return float(row.get(field.strip(), 0)) >= float(value.strip())
            if '<=' in expr:
                field, value = expr.split('<=', 1)
                return float(row.get(field.strip(), 0)) <= float(value.strip())
            if '>' in expr:
                field, value = expr.split('>', 1)
                return float(row.get(field.strip(), 0)) > float(value.strip())
            if '<' in expr:
                field, value = expr.split('<', 1)
                return float(row.get(field.strip(), 0)) < float(value.strip())

            return True

        return matches_filters
