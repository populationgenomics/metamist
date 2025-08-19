"""CLI entry point for upload bucket audit."""

import asyncio
import logging
from typing import Optional
import click
from cpg_utils.config import config_retrieve

from ..models import AuditConfig, FileType
from ..adapters import GraphQLClient, StorageClient
from ..repositories import MetamistRepository, GCSRepository
from ..services import AuditAnalyzer, ReportGenerator


class AuditOrchestrator:
    """Orchestrates the audit process with injected dependencies."""
    
    def __init__(
        self,
        metamist_repo: MetamistRepository,
        gcs_repo: GCSRepository,
        analyzer: AuditAnalyzer,
        report_writer: ReportGenerator,
        logger: logging.Logger
    ):
        """
        Initialize the orchestrator.
        
        Args:
            metamist_repo: Repository for Metamist data
            gcs_repo: Repository for GCS data
            analyzer: Audit analysis service
            report_writer: Report generation service
            logger: Logger instance
        """
        self.metamist_repo = metamist_repo
        self.gcs_repo = gcs_repo
        self.analyzer = analyzer
        self.report_writer = report_writer
        self.logger = logger
    
    async def run_audit(self, config: AuditConfig) -> None:
        """
        Run the complete audit process.
        
        Args:
            config: Audit configuration
        """
        self._log_initialization(config)
        
        # 1. Fetch sequencing groups from Metamist
        self.logger.info('Getting sequencing groups'.center(50, '~'))
        sgs = await self.metamist_repo.get_sequencing_groups(
            config.dataset,
            list(config.sequencing_types),
            list(config.sequencing_technologies),
            list(config.sequencing_platforms)
        )
        self.logger.info(f'Found {len(sgs)} sequencing groups')
        self._log_sg_summary(sgs)
        
        # 2. Fetch analyses for sequencing groups
        self.logger.info('Getting analyses'.center(50, '~'))
        sg_ids = [sg.id for sg in sgs]
        analyses = await self.metamist_repo.get_analyses_for_sequencing_groups(
            config.dataset,
            sg_ids,
            list(config.analysis_types)
        )
        self.logger.info(f'Found {len(analyses)} analyses')
        
        # 3. Get bucket name and list files
        self.logger.info('Scanning upload bucket'.center(50, '~'))
        bucket_name = self.gcs_repo.get_bucket_name(config.dataset, 'upload')
        self.logger.info(f'Bucket: gs://{bucket_name}')
        
        bucket_files = self.gcs_repo.list_files_in_bucket(
            bucket_name,
            list(config.file_types),
            config.excluded_prefixes
        )
        self.logger.info(f'Found {len(bucket_files)} files in bucket')
        
        # 4. Validate CRAM files exist
        if analyses:
            self.logger.info('Validating analysis existence'.center(50, '~'))
            cram_paths = [
                str(a.output_file.filepath) 
                for a in analyses 
                if a.is_cram and a.output_file
            ]
            existing_crams = self.gcs_repo.find_files_in_prefixes(
                cram_paths,
                ('.cram',)
            )
            self.logger.info(f'Validated {len(existing_crams)} CRAM files')
        
        # 5. Run analysis
        self.logger.info('Analyzing audit data'.center(50, '~'))
        result = self.analyzer.analyze_sequencing_groups(
            sgs,
            bucket_files,
            analyses,
            list(config.excluded_sequencing_groups)
        )
        
        # 6. Log summary
        self._log_summary(result)
        
        # 7. Write reports
        self.logger.info('Writing reports'.center(50, '~'))
        self.report_writer.write_reports(
            result,
            bucket_name,
            config
        )
        
        self.logger.info('Upload bucket audit complete')
    
    def _log_initialization(self, config: AuditConfig):
        """Log audit initialization details."""
        self.logger.info('Initializing audit'.center(50, '~'))
        self.logger.info('')
        self.logger.info(f'Dataset:                    {config.dataset}')
        self.logger.info(f'Sequencing Types:           {", ".join(sorted(config.sequencing_types))}')
        self.logger.info(f'Sequencing Technologies:    {", ".join(sorted(config.sequencing_technologies))}')
        self.logger.info(f'Sequencing Platforms:       {", ".join(sorted(config.sequencing_platforms))}')
        self.logger.info(f'Analysis Types:             {", ".join(sorted(config.analysis_types))}')
        self.logger.info(f'File Types:                 {", ".join(sorted(ft.name for ft in config.file_types))}')
        if config.excluded_prefixes:
            self.logger.info(f'Excluded Prefixes:          {", ".join(sorted(config.excluded_prefixes))}')
        self.logger.info('')
    
    def _log_sg_summary(self, sgs):
        """Log summary of sequencing groups by type/technology."""
        from collections import defaultdict
        
        counts = defaultdict(int)
        for sg in sgs:
            key = f"{sg.technology}|{sg.type}|{sg.platform}"
            counts[key] += 1
        
        for key, count in sorted(counts.items()):
            tech, typ, platform = key.split('|')
            self.logger.info(f'  {count:5} ({tech}, {typ}, {platform})')
        self.logger.info('')
    
    def _log_summary(self, result):
        """Log audit result summary."""
        stats = self.report_writer.generate_summary_statistics(result)
        
        self.logger.info('Audit Summary'.center(50, '~'))
        self.logger.info(f'Files to delete: {stats["files_to_delete"]} '
                        f'({stats["files_to_delete_size_gb"]:.2f} GB)')
        self.logger.info(f'Moved files: {stats["moved_files"]}')
        self.logger.info(f'Files to ingest: {stats["files_to_ingest"]} '
                        f'({stats["files_to_ingest_size_gb"]:.2f} GB)')
        self.logger.info(f'Unaligned SGs: {stats["unaligned_sgs"]}')
        self.logger.info('')


def setup_logger(dataset: str) -> logging.Logger:
    """Set up logger for audit."""
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(dataset)s :: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    handler.setFormatter(formatter)
    logger = logging.getLogger('audit')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    
    return logging.LoggerAdapter(logger, {'dataset': dataset})


async def validate_enum_values(
    graphql_client: GraphQLClient,
    config: AuditConfig
) -> AuditConfig:
    """
    Validate enum values against Metamist API.
    
    Args:
        graphql_client: GraphQL client
        config: Audit configuration
        
    Returns:
        Validated configuration
    """
    # Validate sequencing types
    if 'all' in config.sequencing_types:
        valid_types = await graphql_client.get_enum_values('sequencing_type')
        config = AuditConfig(
            dataset=config.dataset,
            sequencing_types=tuple(valid_types),
            sequencing_technologies=config.sequencing_technologies,
            sequencing_platforms=config.sequencing_platforms,
            analysis_types=config.analysis_types,
            file_types=config.file_types,
            excluded_prefixes=config.excluded_prefixes,
            excluded_sequencing_groups=config.excluded_sequencing_groups
        )
    
    # Validate sequencing technologies
    if 'all' in config.sequencing_technologies:
        valid_techs = await graphql_client.get_enum_values('sequencing_technology')
        config = AuditConfig(
            dataset=config.dataset,
            sequencing_types=config.sequencing_types,
            sequencing_technologies=tuple(valid_techs),
            sequencing_platforms=config.sequencing_platforms,
            analysis_types=config.analysis_types,
            file_types=config.file_types,
            excluded_prefixes=config.excluded_prefixes,
            excluded_sequencing_groups=config.excluded_sequencing_groups
        )
    
    # Validate sequencing platforms
    if 'all' in config.sequencing_platforms:
        valid_platforms = await graphql_client.get_enum_values('sequencing_platform')
        config = AuditConfig(
            dataset=config.dataset,
            sequencing_types=config.sequencing_types,
            sequencing_technologies=config.sequencing_technologies,
            sequencing_platforms=tuple(valid_platforms),
            analysis_types=config.analysis_types,
            file_types=config.file_types,
            excluded_prefixes=config.excluded_prefixes,
            excluded_sequencing_groups=config.excluded_sequencing_groups
        )
    
    # Validate analysis types
    if 'all' in config.analysis_types:
        valid_analysis_types = await graphql_client.get_enum_values('analysis_type')
        config = AuditConfig(
            dataset=config.dataset,
            sequencing_types=config.sequencing_types,
            sequencing_technologies=config.sequencing_technologies,
            sequencing_platforms=config.sequencing_platforms,
            analysis_types=tuple(valid_analysis_types),
            file_types=config.file_types,
            excluded_prefixes=config.excluded_prefixes,
            excluded_sequencing_groups=config.excluded_sequencing_groups
        )
    
    return config


async def audit_upload_bucket_async(config: AuditConfig):
    """
    Async entry point for upload bucket audit.
    
    Args:
        config: Audit configuration
    """
    # Set up logger
    logger = setup_logger(config.dataset)
    
    # Get GCP project
    gcp_project = config_retrieve(['workflow', config.dataset, 'gcp_project'])
    if not gcp_project:
        raise ValueError('GCP project is required')
    
    # Get excluded sequencing groups from config if not provided
    if not config.excluded_sequencing_groups:
        excluded_sgs = config_retrieve(
            ['metamist', 'audit', 'excluded_sequencing_groups'],
            default=[]
        )
        config = AuditConfig(
            dataset=config.dataset,
            sequencing_types=config.sequencing_types,
            sequencing_technologies=config.sequencing_technologies,
            sequencing_platforms=config.sequencing_platforms,
            analysis_types=config.analysis_types,
            file_types=config.file_types,
            excluded_prefixes=config.excluded_prefixes,
            excluded_sequencing_groups=tuple(excluded_sgs)
        )
    
    # Create adapters
    graphql_client = GraphQLClient()
    storage_client = StorageClient(project=gcp_project)
    
    # Validate enum values
    config = await validate_enum_values(graphql_client, config)
    
    # Create repositories
    metamist_repo = MetamistRepository(graphql_client)
    gcs_repo = GCSRepository(storage_client)
    
    # Create services
    analyzer = AuditAnalyzer(logger)
    report_writer = ReportGenerator(storage_client.client, logger)
    
    # Create orchestrator
    orchestrator = AuditOrchestrator(
        metamist_repo,
        gcs_repo,
        analyzer,
        report_writer,
        logger
    )
    
    # Run audit
    await orchestrator.run_audit(config)


def audit_upload_bucket(config: AuditConfig):
    """
    Synchronous entry point for upload bucket audit.
    
    Args:
        config: Audit configuration
    """
    asyncio.run(audit_upload_bucket_async(config))


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
    required=False,
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
def main(
    dataset: str,
    sequencing_types: tuple[str],
    sequencing_technologies: tuple[str],
    sequencing_platforms: tuple[str],
    analysis_types: tuple[str],
    file_types: tuple[str],
    excluded_prefixes: Optional[tuple[str]] = None,
):
    """Run upload bucket audit for a Metamist dataset."""
    # Create configuration from CLI args
    from types import SimpleNamespace
    args = SimpleNamespace(
        dataset=dataset,
        sequencing_types=sequencing_types,
        sequencing_technologies=sequencing_technologies,
        sequencing_platforms=sequencing_platforms,
        analysis_types=analysis_types,
        file_types=file_types,
        excluded_prefixes=excluded_prefixes
    )
    
    config = AuditConfig.from_cli_args(args)
    
    # Run audit
    audit_upload_bucket(config)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
