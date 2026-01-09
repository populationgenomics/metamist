"""CLI entry point for upload bucket audit."""

import asyncio
import click

from types import SimpleNamespace

from metamist.audit.services import AuditOrchestrator


async def audit_upload_bucket_async(config_args: SimpleNamespace):
    """
    Async entry point for upload bucket audit.

    Args:
        config_args: Audit configuration
    """
    orchestrator = AuditOrchestrator(config_args=config_args)
    # Run audit
    await orchestrator.run_audit()


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
