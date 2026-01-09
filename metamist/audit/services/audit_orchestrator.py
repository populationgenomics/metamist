import asyncio

from types import SimpleNamespace

from cpg_utils import to_path
from cpg_utils.config import config_retrieve

from metamist.audit.data_access import MetamistDataAccess, GCSDataAccess
from metamist.audit.models import AuditConfig
from metamist.audit.services import AuditAnalyzer, BucketAuditLogger, Reporter


class AuditOrchestrator:
    """
    A class that manages and orchestrates the audit process with input validation.
    """

    def __init__(
        self,
        config_args: SimpleNamespace,
    ):
        """
        Initialize the orchestrator.

        Args:
            metamist_data_access: Data access layer for Metamist data
            gcs_data_access: Data access layer for GCS data
            analyzer: Audit analysis service
            reporter: Report generation service
            audit_logs: audit_logs instance
        """
        self.config = self.validate_config(AuditConfig.from_cli_args(config_args))
        self.config.audit_type = 'upload_bucket_audit'

        self.analyzer = AuditAnalyzer()
        self.metamist = MetamistDataAccess()
        self.audit_logger = BucketAuditLogger(
            self.config.dataset, self.config.audit_type
        )
        self.gcs = GCSDataAccess(self.config.dataset)
        self.reporter = Reporter(
            self.gcs,
            self.audit_logger,
            self.config.results_folder
            or config_retrieve(['workflow', 'output_prefix']),
        )

    def validate_config(self, config: AuditConfig) -> AuditConfig:
        """
        Validate the audit configuration against Metamist enums.

        Args:
            config: Audit configuration
        """
        loop = asyncio.get_event_loop()
        validated_config = loop.run_until_complete(self._validate_config_async(config))
        return validated_config

    async def _validate_config_async(self, config: AuditConfig) -> AuditConfig:
        """
        Asynchronously validate the audit configuration against Metamist enums.

        Args:
            config: Audit configuration
        """
        self.audit_logger.info_nl('Validating audit configuration'.center(50, '~'))
        return await self.metamist.validate_metamist_enums(config)

    async def run_audit(self) -> None:
        """
        Run the complete audit process.

        Args:
            config: Audit configuration
        """
        self.audit_logger.log_initialization(self.config)

        # 1. Fetch sequencing groups from Metamist
        self.audit_logger.info_nl('Getting sequencing groups'.center(50, '~'))
        sgs = await self.metamist.get_sequencing_groups(
            self.config.dataset,
            list(self.config.sequencing_types),
            list(self.config.sequencing_technologies),
            list(self.config.sequencing_platforms),
        )
        self.audit_logger.info_nl(f'Found {len(sgs)} sequencing groups')
        self.audit_logger.log_sg_summary(sgs)

        # 2. Fetch analyses for sequencing groups
        self.audit_logger.info_nl('Getting analyses'.center(50, '~'))
        sg_ids = [sg.id for sg in sgs]
        analyses = await self.metamist.get_analyses_for_sequencing_groups(
            self.config.dataset, sg_ids, list(self.config.analysis_types)
        )
        self.audit_logger.info_nl(f'Found {len(analyses)} analyses')

        # 3. Validate CRAM files exist
        if analyses:
            self.audit_logger.info_nl('Validating analyses existence'.center(50, '~'))
            cram_paths = [
                a.output_file.filepath for a in analyses if a.is_cram and a.output_file
            ]
            found, missing = self.gcs.validate_cram_files(cram_paths)
            for p in missing:
                self.audit_logger.warning(f'Missing expected CRAM file: {p}')

            self.audit_logger.info_nl(f'Validated {len(found)} CRAM files')

        # 4. Get upload bucket files
        self.audit_logger.info_nl('Scanning upload bucket'.center(50, '~'))
        self.audit_logger.info_nl(f'Target: gs://{self.gcs.upload_bucket}/')

        bucket_files = self.gcs.list_files_in_bucket(
            self.gcs.upload_bucket,
            list(self.config.file_types),
            self.config.excluded_prefixes,
        )
        self.audit_logger.info_nl(f'Found {len(bucket_files)} files in bucket')

        # 5. Run analysis
        self.audit_logger.info_nl('Analyzing audit data'.center(50, '~'))
        result = self.analyzer.analyze_sequencing_groups(
            sgs,
            bucket_files,
            analyses,
        )
        self.audit_logger.info_nl('Analysis complete.')

        # 6. Log summary
        self.audit_logger.log_result_summary(result)

        # 7. Write reports and log to the analysis bucket
        self.audit_logger.info_nl('Writing reports'.center(50, '~'))
        self.reporter.write_audit_reports(result, self.config)

        self.audit_logger.info_nl('Upload bucket audit complete!')
        log_file_path = self.write_log_file(
            self.audit_logger.log_file, f'{self.reporter.output_dir}/log.txt'
        )
        self.audit_logger.info_nl(f'Wrote log to {log_file_path}')

    def write_log_file(self, log_file: str, save_path: str):
        """Write audit logs to a file."""
        # Upload the log file to GCS
        blob = self.gcs.get_blob(self.gcs.analysis_bucket, save_path)
        blob.upload_from_filename(log_file)
        return to_path(f'gs://{self.gcs.analysis_bucket}/{save_path}')
