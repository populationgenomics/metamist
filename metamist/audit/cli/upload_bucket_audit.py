"""CLI entry point for upload bucket audit."""

import asyncio
import click
from collections import defaultdict
from types import SimpleNamespace

from metamist.audit.data_access import MetamistDataAccess, GCSDataAccess
from metamist.audit.models import AuditConfig, SequencingGroup
from metamist.audit.services import AuditAnalyzer, BucketAuditLogger, Reporter


class AuditOrchestrator:
    """Orchestrates the audit process with injected dependencies."""

    def __init__(
        self,
        metamist_data_access: MetamistDataAccess,
        gcs_data_access: GCSDataAccess,
        analyzer: AuditAnalyzer,
        reporter: Reporter,
        audit_logs: BucketAuditLogger,
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
        self.metamist = metamist_data_access
        self.gcs = gcs_data_access
        self.analyzer = analyzer
        self.reporter = reporter
        self.audit_logs = audit_logs

    async def run_audit(self, config: AuditConfig) -> None:
        """
        Run the complete audit process.

        Args:
            config: Audit configuration
        """
        self._log_initialization(config)

        # 1. Fetch sequencing groups from Metamist
        self.audit_logs.info_nl('Getting sequencing groups'.center(50, '~'))
        sgs = await self.metamist.get_sequencing_groups(
            config.dataset,
            list(config.sequencing_types),
            list(config.sequencing_technologies),
            list(config.sequencing_platforms),
        )
        self.audit_logs.info_nl(f'Found {len(sgs)} sequencing groups')
        self._log_sg_summary(sgs)

        # 2. Fetch analyses for sequencing groups
        self.audit_logs.info_nl('Getting analyses'.center(50, '~'))
        sg_ids = [sg.id for sg in sgs]
        analyses = await self.metamist.get_analyses_for_sequencing_groups(
            config.dataset, sg_ids, list(config.analysis_types)
        )
        self.audit_logs.info_nl(f'Found {len(analyses)} analyses')

        # 3. Validate CRAM files exist
        if analyses:
            self.audit_logs.info_nl('Validating analysis existence'.center(50, '~'))
            cram_paths = [
                a.output_file.filepath for a in analyses if a.is_cram and a.output_file
            ]
            found, missing = self.gcs.validate_cram_files(cram_paths)
            for p in missing:
                self.audit_logs.warning(f'Missing expected CRAM file: {p}')

            self.audit_logs.info_nl(f'Validated {len(found)} CRAM files')

        # 4. Get upload bucket files
        self.audit_logs.info_nl('Scanning upload bucket'.center(50, '~'))
        self.audit_logs.info_nl(f'Target: gs://{self.gcs.upload_bucket}/')

        bucket_files = self.gcs.list_files_in_bucket(
            self.gcs.upload_bucket, list(config.file_types), config.excluded_prefixes
        )
        self.audit_logs.info_nl(f'Found {len(bucket_files)} files in bucket')

        # 5. Run analysis
        self.audit_logs.info_nl('Analyzing audit data'.center(50, '~'))
        result = self.analyzer.analyze_sequencing_groups(
            sgs,
            bucket_files,
            analyses,
        )
        self.audit_logs.info_nl('Analysis complete.')

        # 6. Log summary
        self._log_summary(result)

        # 7. Write reports and log to the analysis bucket
        self.audit_logs.info_nl('Writing reports'.center(50, '~'))
        self.reporter.write_audit_reports(result, config)

        self.audit_logs.info_nl('Upload bucket audit complete!')
        log_file_path = self.reporter.write_log_file(
            self.audit_logs.log_file, f'{self.reporter.output_dir}/log.txt'
        )
        self.audit_logs.info_nl(f'Wrote log to {log_file_path}')

    def _log_initialization(self, config: AuditConfig):
        """Log audit initialization details."""
        self.audit_logs.info('Initializing audit'.center(50, '~'))
        self.audit_logs.info('')
        self.audit_logs.info(f'Dataset:                    {config.dataset}')
        self.audit_logs.info(
            f'Sequencing Types:           {", ".join(sorted(config.sequencing_types))}'
        )
        self.audit_logs.info(
            f'Sequencing Technologies:    {", ".join(sorted(config.sequencing_technologies))}'
        )
        self.audit_logs.info(
            f'Sequencing Platforms:       {", ".join(sorted(config.sequencing_platforms))}'
        )
        self.audit_logs.info(
            f'Analysis Types:             {", ".join(sorted(config.analysis_types))}'
        )
        self.audit_logs.info(
            f'File Types:                 {", ".join(sorted(ft.name for ft in config.file_types))}'
        )
        if config.excluded_prefixes:
            self.audit_logs.info(
                f'Excluded Prefixes:          {", ".join(sorted(config.excluded_prefixes))}'
            )
        self.audit_logs.info('')

    def _log_sg_summary(self, sgs: list[SequencingGroup]):
        """Log summary of sequencing groups by type/technology."""
        counts = defaultdict(int)
        for sg in sgs:
            key = f'{sg.technology}|{sg.type}|{sg.platform}'
            counts[key] += 1

        for key, count in sorted(counts.items()):
            tech, typ, platform = key.split('|')
            self.audit_logs.info(f'  {count:5} ({tech}, {typ}, {platform})')
        self.audit_logs.info('')

    def _log_summary(self, result):
        """Log audit result summary."""
        stats = self.reporter.generate_summary_statistics(result)

        self.audit_logs.info_nl('Audit Summary'.center(50, '~'))
        self.audit_logs.info(
            f'Files to delete:           {stats["files_to_delete"]} '
            f'({stats["files_to_delete_size_gb"]:.2f} GB)'
        )
        self.audit_logs.info(
            f'Files to review:           {stats["files_to_review"]} '
            f'({stats["files_to_review_size_gb"]:.2f} GB)'
        )
        self.audit_logs.info_nl(f'Unaligned SGs:             {stats["unaligned_sgs"]}')


async def audit_upload_bucket_async(config_args: SimpleNamespace):
    """
    Async entry point for upload bucket audit.

    Args:
        config_args: Audit configuration
    """
    # Validate audit config against Metamist before proceeding
    metamist = MetamistDataAccess()
    audit = await metamist.validate_metamist_enums(
        AuditConfig.from_cli_args(config_args)
    )

    audit_logs = BucketAuditLogger(audit.dataset, 'upload_bucket_audit')
    gcs = GCSDataAccess(audit.dataset)
    reporter = Reporter(gcs, audit_logs, audit.results_folder)
    orchestrator = AuditOrchestrator(
        metamist_data_access=metamist,
        gcs_data_access=gcs,
        analyzer=AuditAnalyzer(),
        reporter=reporter,
        audit_logs=audit_logs,
    )
    # Run audit
    await orchestrator.run_audit(audit)


def audit_upload_bucket(config_args: SimpleNamespace):
    """
    Synchronous entry point for upload bucket audit.

    Args:
        config_args: Audit arguments for configuration
    """
    asyncio.run(audit_upload_bucket_async(config_args))


@click.command()
@click.option(
    '--dataset',
    '-d',
    help='Metamist dataset',
)
@click.option(
    '--sequencing-types',
    '-s',
    multiple=True,
    required=True,
    help='"all", or any of the enum sequencing types',
)
@click.option(
    '--sequencing-technologies',
    '-t',
    multiple=True,
    required=True,
    help='"all", or any of the enum sequencing technologies',
)
@click.option(
    '--sequencing-platforms',
    '-p',
    multiple=True,
    default=('all',),
    help='"all", or any of the enum sequencing platforms',
)
@click.option(
    '--analysis-types',
    '-a',
    multiple=True,
    default=('CRAM',),
    help='Specify the analysis types to check for SG completions',
)
@click.option(
    '--file-types',
    '-f',
    multiple=True,
    required=True,
    help='Find fastq, bam, cram, gvcf, vcf, all_reads (fastq + bam + cram), or all sequence file types',
)
@click.option(
    '--excluded-prefixes',
    '-e',
    multiple=True,
    help='Exclude files with these prefixes from the audit',
)
@click.option(
    '--results-folder',
    '-r',
    help='Name of the results directory, overwriting default timestamp',
)
def main(
    dataset: str,
    sequencing_types: tuple[str],
    sequencing_technologies: tuple[str],
    sequencing_platforms: tuple[str],
    analysis_types: tuple[str],
    file_types: tuple[str],
    excluded_prefixes: tuple[str] | None = None,
    results_folder: str | None = None,
):
    """Run upload bucket audit for a Metamist dataset."""
    config_args = SimpleNamespace(
        dataset=dataset,
        sequencing_types=sequencing_types,
        sequencing_technologies=sequencing_technologies,
        sequencing_platforms=sequencing_platforms,
        analysis_types=analysis_types,
        file_types=file_types,
        excluded_prefixes=excluded_prefixes,
        results_folder=results_folder,
    )

    # Run audit
    audit_upload_bucket(config_args)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
