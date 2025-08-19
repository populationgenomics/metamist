"""Report generation service for audit results."""

import csv
from datetime import datetime
from io import StringIO
from typing import List, Optional, Dict
import logging

from google.cloud import storage

from ..models import (
    AuditResult,
    AuditReportEntry,
    SequencingGroup,
    AuditConfig,
)


class ReportGenerator:
    """Service for generating and writing audit reports."""
    
    def __init__(
        self,
        storage_client: storage.Client,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the report generator.
        
        Args:
            storage_client: Google Cloud Storage client
            logger: Optional logger instance
        """
        self.storage_client = storage_client
        self.logger = logger or logging.getLogger(__name__)
    
    def write_reports(
        self,
        audit_result: AuditResult,
        bucket_name: str,
        config: AuditConfig,
        output_prefix: Optional[str] = None
    ):
        """
        Write all audit reports to GCS.
        
        Args:
            audit_result: The audit analysis results
            bucket_name: GCS bucket name for reports
            config: Audit configuration
            output_prefix: Optional prefix for output paths
        """
        timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
        output_dir = f"{output_prefix or 'audit_results'}/{timestamp}"
        
        # Write metadata report
        self._write_metadata_report(
            bucket_name,
            f"{output_dir}/audit_metadata.tsv",
            config
        )
        
        # Write files to delete report
        if audit_result.files_to_delete:
            self._write_csv_report(
                bucket_name,
                f"{output_dir}/files_to_delete.csv",
                audit_result.files_to_delete,
                "FILES TO DELETE"
            )
        
        # Write files to ingest report
        if audit_result.files_to_ingest:
            self._write_csv_report(
                bucket_name,
                f"{output_dir}/files_to_ingest.csv",
                audit_result.files_to_ingest,
                "FILES TO INGEST"
            )
        
        # Write moved files report
        if audit_result.moved_files:
            self._write_csv_report(
                bucket_name,
                f"{output_dir}/moved_files.csv",
                audit_result.moved_files,
                "MOVED FILES"
            )
        
        # Write unaligned SGs report
        if audit_result.unaligned_sequencing_groups:
            self._write_unaligned_sgs_report(
                bucket_name,
                f"{output_dir}/unaligned_sgs.tsv",
                audit_result.unaligned_sequencing_groups
            )
        
        self.logger.info(f"Reports written to gs://{bucket_name}/{output_dir}")
    
    def _write_metadata_report(
        self,
        bucket_name: str,
        blob_path: str,
        config: AuditConfig
    ):
        """Write audit configuration metadata."""
        buffer = StringIO()
        writer = csv.writer(buffer, delimiter='\t')
        
        writer.writerow([
            'Sequencing Type',
            'Sequencing Technology',
            'Sequencing Platform',
            'File Type',
            'Analysis Type',
            'Excluded Prefixes'
        ])
        
        writer.writerow([
            ', '.join(config.sequencing_types),
            ', '.join(config.sequencing_technologies),
            ', '.join(config.sequencing_platforms),
            ', '.join(ft.name for ft in config.file_types),
            ', '.join(config.analysis_types),
            ', '.join(config.excluded_prefixes) if config.excluded_prefixes else '',
        ])
        
        self._upload_buffer(bucket_name, blob_path, buffer, 'text/tab-separated-values')
        self.logger.info(f"Metadata report written to gs://{bucket_name}/{blob_path}")
    
    def _write_csv_report(
        self,
        bucket_name: str,
        blob_path: str,
        entries: List[AuditReportEntry],
        report_name: str
    ):
        """Write a CSV report for audit entries."""
        if not entries:
            self.logger.info(f"{report_name} - SKIPPED - No data to write")
            return
        
        buffer = StringIO()
        
        # Get fieldnames from first entry
        fieldnames = list(entries[0].to_report_dict().keys())
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        
        writer.writeheader()
        
        # Sort files to delete by SG ID for easier review
        rows = [entry.to_report_dict() for entry in entries]
        if report_name == "FILES TO DELETE":
            rows = sorted(rows, key=lambda x: x.get('SG ID', ''))
        
        writer.writerows(rows)
        
        self._upload_buffer(bucket_name, blob_path, buffer, 'text/csv')
        self.logger.info(f"{report_name}: {len(entries)} entries written to gs://{bucket_name}/{blob_path}")
    
    def _write_unaligned_sgs_report(
        self,
        bucket_name: str,
        blob_path: str,
        unaligned_sgs: List[SequencingGroup]
    ):
        """Write report of sequencing groups without completed CRAMs."""
        if not unaligned_sgs:
            self.logger.info("UNALIGNED SEQUENCING GROUPS - SKIPPED - No data to write")
            return
        
        buffer = StringIO()
        writer = csv.writer(buffer, delimiter='\t')
        
        # Write header
        writer.writerow([
            'SG ID',
            'SG Type',
            'SG Technology',
            'SG Platform',
            'Sample ID',
            'Sample External ID',
            'Participant ID',
            'Participant External ID',
            'Assay Count',
            'Total Read Size'
        ])
        
        # Write data
        for sg in unaligned_sgs:
            sample_ext_id = sg.sample.external_id or ', '.join(sg.sample.external_ids.values())
            participant_ext_id = sg.sample.participant.external_id or ', '.join(
                sg.sample.participant.external_ids.values()
            )
            
            writer.writerow([
                sg.id,
                sg.type,
                sg.technology,
                sg.platform,
                sg.sample.id,
                sample_ext_id,
                sg.sample.participant.id,
                participant_ext_id,
                len(sg.assays),
                sg.get_total_read_size()
            ])
        
        self._upload_buffer(bucket_name, blob_path, buffer, 'text/tab-separated-values')
        self.logger.info(
            f"UNALIGNED SEQUENCING GROUPS: {len(unaligned_sgs)} entries written to "
            f"gs://{bucket_name}/{blob_path}"
        )
    
    def _upload_buffer(
        self,
        bucket_name: str,
        blob_path: str,
        buffer: StringIO,
        content_type: str = 'text/plain'
    ):
        """Upload a StringIO buffer to GCS."""
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        blob.upload_from_string(
            buffer.getvalue(),
            content_type=content_type
        )
        
        buffer.close()
    
    def generate_summary_statistics(
        self,
        audit_result: AuditResult
    ) -> Dict[str, int]:
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
        
        total_size_to_ingest = sum(
            entry.filesize or 0
            for entry in audit_result.files_to_ingest
        )
        
        return {
            'files_to_delete': len(audit_result.files_to_delete),
            'files_to_delete_size_gb': total_size_to_delete / (1024**3),
            'moved_files': len(audit_result.moved_files),
            'files_to_ingest': len(audit_result.files_to_ingest),
            'files_to_ingest_size_gb': total_size_to_ingest / (1024**3),
            'unaligned_sgs': len(audit_result.unaligned_sequencing_groups),
        }
