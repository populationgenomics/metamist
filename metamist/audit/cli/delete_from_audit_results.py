"""CLI entry point for deleting files in audit results."""

import click

from metamist.audit.services import AuditDeleteOrchestrator


@click.command()
@click.option('--dataset', '-d', required=True, help='Dataset name')
@click.option('--results-folder', '-r', required=True, help='Audit results folder')
@click.option(
    '--report-name',
    '-n',
    help="The report to delete files from. e.g. 'files_to_delete'.",
    default='files_to_delete',
)
@click.option('--dry-run', is_flag=True, help='If set, will not delete objects.')
def main(
    dataset: str,
    results_folder: str,
    report_name: str = 'files_to_delete',
    dry_run: bool = False,
):
    """
    Reads the report at location:
        gs://cpg-dataset-main-analysis/audit_results/{results_folder}/{report_name}.csv

    If the report is 'files_to_delete', it will delete the files listed in the report.

    For any other report, files must be marked for deletion in the report, alongside a
    comment justifying the deletion.

    Deleting files will create or update an analysis record of type 'audit_deletions'.
    """
    delete_orchestrator = AuditDeleteOrchestrator(dataset, results_folder, report_name)
    delete_orchestrator.delete_from_audit_results(dry_run=dry_run)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
