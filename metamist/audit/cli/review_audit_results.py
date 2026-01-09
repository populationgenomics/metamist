"""CLI entry point for reviewing audit results."""

import click

from metamist.audit.services import AuditReviewOrchestrator


@click.command()
@click.option('--dataset', '-d', required=True, help='Dataset name for logging context')
@click.option(
    '--results-folder', '-r', help='Path to the folder containing audit results files'
)
@click.option(
    '--action', '-a', default='DELETE', help='Action to perform on reviewed files.'
)
@click.option(
    '--comment', '-c', required=True, help='Comment to add to reviewed files.'
)
@click.option(
    '--filter',
    '-f',
    'filter_expressions',
    multiple=True,
    help='Filter expression like "SG Type==exome" or "File Path contains 2025-06-01"',
)
def main(
    dataset: str,
    results_folder: str,
    action: str,
    comment: str,
    filter_expressions: tuple = (),
):
    """Main function to review objects."""
    orchestrator = AuditReviewOrchestrator(
        dataset=dataset,
        results_folder=results_folder,
    )
    orchestrator.review_audit_report(
        action=action, comment=comment, filter_expressions=list(filter_expressions)
    )


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
