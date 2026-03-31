import click

from metamist.audit.cli.delete_from_audit_results import (
    main as delete_from_audit_results_main,
)
from metamist.audit.cli.review_audit_results import main as review_audit_results_main
from metamist.audit.cli.upload_bucket_audit import main as upload_bucket_audit_main
from metamist.metamist_config import get_config
from metamist.parser.generic_metadata_parser import main as generic_metadata_parser_main
from metamist.parser.sample_file_map_parser import main as sample_file_map_parser_main


@click.group()
def cli():
    """Metamist CLI"""


@cli.group()
def auth():
    """Metamist Auth helpers"""


@auth.command('init')
def init_auth():
    """Initialize metamist auth by caching the Google identity token."""
    config = get_config()

    if config.env == 'local':
        click.echo(
            'Auth cannot be initialized for local environent. Choose a development or production environment before runing `auth init`.'
        )
        return

    # Get oauth desktop credentials from gcloud secret
    config.get_desktop_oauth_creds(do_secret_fetch=True)

    click.echo(f'Initialize user auth for environment {config.env}')
    token = config.get_google_identity_token()
    if token:
        click.echo('Successfully authenticated and cached token.')
    else:
        click.echo('Failed to authenticate.', err=True)


@cli.group()
def audit():
    """Audit commands"""


audit.add_command(upload_bucket_audit_main, name='upload-bucket')
audit.add_command(review_audit_results_main, name='review-results')
audit.add_command(delete_from_audit_results_main, name='delete-from-results')


@cli.group()
def parse():
    """Parser commands"""


parse.add_command(generic_metadata_parser_main, name='generic-metadata')
parse.add_command(sample_file_map_parser_main, name='sample-file-map')


if __name__ == '__main__':
    cli()
