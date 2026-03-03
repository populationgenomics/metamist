#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pygithub",
# ]
# ///
"""
Database Migration Runner for Cloud Run Job

Commands:
    status  - Show current migration status
    up      - Apply all pending migrations

Required environment variables:
    DATABASE_URL_SECRET  - PostgreSQL connection string (value injected from Secret Manager)
    GITHUB_REPOSITORY    - GitHub repository (e.g., populationgenomics/metamist)
    GITHUB_REF           - Git ref to fetch migrations from (e.g., main, refs/heads/dev)
    GITHUB_TOKEN_SECRET  - GitHub token for API access (value injected from Secret Manager)

Optional environment variables:
    MIGRATION_COMMAND    - Command to run: status, up, down (default: status)
    MIGRATIONS_PATH      - Path to migrations in repo (default: db/migrations)
"""

import os
import re
import subprocess
import sys
from pathlib import Path

from github import Auth, Github


MIGRATIONS_DIR = Path('/db/migrations')


def print_header(title: str) -> None:
    """Print a formatted header."""
    print('=' * 44)
    print(title)
    print('=' * 44)


def print_separator() -> None:
    """Print a separator line."""
    print('-' * 44)


def download_migrations_from_github(
    repository: str,
    ref: str,
    token: str,
    migrations_path: str = 'db/migrations',
) -> int:
    """
    Download migration files from GitHub using PyGithub.

    Args:
        repository: GitHub repository in format owner/repo
        ref: Git ref (branch, tag, or commit SHA)
        token: GitHub token for API access
        migrations_path: Path to migrations directory in the repository

    Returns:
        Number of migration files downloaded
    """
    print('Downloading migrations from GitHub...')
    print(f'Repository: {repository}')
    print(f'Ref: {ref}')
    print(f'Path: {migrations_path}')

    # Clean up ref (remove refs/heads/ prefix if present)
    if ref.startswith('refs/heads/'):
        ref = ref[len('refs/heads/') :]
    elif ref.startswith('refs/tags/'):
        ref = ref[len('refs/tags/') :]

    # Initialize GitHub client
    auth = Auth.Token(token)
    gh = Github(auth=auth)
    repo = gh.get_repo(repository)

    try:
        contents = repo.get_contents(migrations_path, ref=ref)
    except Exception as e:
        print(f'ERROR: Failed to fetch migrations: {e}')
        return 0

    if not isinstance(contents, list):
        print('ERROR: Expected a directory but got a file')
        return 0

    downloaded = 0
    for item in contents:
        if item.type == 'file' and item.name.endswith('.sql'):
            local_path = MIGRATIONS_DIR / item.name

            # Fetch and decode file content
            content = item.decoded_content.decode('utf-8')
            local_path.write_text(content)

            print(f'  {item.name}')
            downloaded += 1

    gh.close()
    print(f'\nDownloaded {downloaded} migration files')
    return downloaded


def run_dbmate(
    database_url: str, *args: str, check: bool = True
) -> subprocess.CompletedProcess:
    """
    Run a dbmate command.

    Args:
        database_url: Database connection URL
        *args: Arguments to pass to dbmate
        check: Whether to raise on non-zero exit code

    Returns:
        CompletedProcess result
    """
    cmd = [
        'dbmate',
        '--url',
        database_url,
        '--migrations-dir',
        str(MIGRATIONS_DIR),
        *args,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def get_migration_status(database_url: str) -> tuple[list[str], list[str]]:
    """
    Get the current migration status.

    Args:
        database_url: Database connection URL

    Returns:
        Tuple of (applied_migrations, pending_migrations)
    """
    result = run_dbmate(database_url, 'status', check=False)

    applied = []
    pending = []

    for line in result.stdout.splitlines():
        # Applied migrations: [X] migration_name
        if match := re.match(r'^\[X\]\s+(\S+)', line):
            applied.append(match.group(1))
        # Pending migrations: [ ] migration_name
        elif match := re.match(r'^\[ \]\s+(\S+)', line):
            pending.append(match.group(1))

    return applied, pending


def show_status(database_url: str) -> tuple[list[str], list[str]]:
    """
    Show current migration status and return the status data.

    Args:
        database_url: Database connection URL

    Returns:
        Tuple of (applied_migrations, pending_migrations)
    """
    print()
    print('Current migration status:')
    print_separator()

    result = run_dbmate(database_url, 'status', check=False)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(f'stderr: {result.stderr}')
    if result.returncode != 0:
        print(f'dbmate exited with code {result.returncode}')

    print_separator()

    return get_migration_status(database_url)


def cmd_status(database_url: str) -> int:
    """
    Handle the 'status' command.

    Args:
        database_url: Database connection URL

    Returns:
        Exit code
    """
    show_status(database_url)
    print()
    print('Status check complete. No changes made.')
    return 0


def cmd_up(database_url: str) -> int:
    """
    Handle the 'up' command - apply all pending migrations.

    Args:
        database_url: Database connection URL

    Returns:
        Exit code
    """
    _, pending = show_status(database_url)

    if not pending:
        print()
        print('No pending migrations. Database is up to date.')
        return 0

    print()
    print(f'Applying {len(pending)} pending migration(s)...')

    result = run_dbmate(database_url, 'up', check=False)

    if result.returncode != 0:
        print('ERROR: Migration failed!')
        print(result.stderr)
        return 1

    print(result.stdout)
    print()
    print('Migrations applied successfully.')
    show_status(database_url)
    return 0


def main() -> int:
    """
    Main entrypoint.

    Returns:
        Exit code
    """
    # Get required environment variables
    database_url_secret = os.environ.get('DATABASE_URL_SECRET')
    if not database_url_secret:
        print('ERROR: DATABASE_URL_SECRET environment variable is required')
        return 1

    github_repository = os.environ.get('GITHUB_REPOSITORY')
    if not github_repository:
        print('ERROR: GITHUB_REPOSITORY environment variable is required')
        return 1

    github_ref = os.environ.get('GITHUB_REF')
    if not github_ref:
        print('ERROR: GITHUB_REF environment variable is required')
        return 1

    github_token_secret = os.environ.get('GITHUB_TOKEN_SECRET')
    if not github_token_secret:
        print('ERROR: GITHUB_TOKEN_SECRET environment variable is required')
        return 1

    # Get optional environment variables
    migration_command = os.environ.get('MIGRATION_COMMAND', 'status')
    migrations_path = os.environ.get('MIGRATIONS_PATH', 'db/migrations')

    # Print header
    print_header('Database Migration Runner')
    print(f'Command: {migration_command}')
    print(f'Repository: {github_repository}')
    print(f'Ref: {github_ref}')
    print()

    # Ensure migrations directory exists
    MIGRATIONS_DIR.mkdir(parents=True, exist_ok=True)

    # Use secret values directly from environment
    # (Cloud Run injects them from Secret Manager)
    database_url = database_url_secret
    github_token = github_token_secret
    print()

    # Download migrations from GitHub
    downloaded = download_migrations_from_github(
        repository=github_repository,
        ref=github_ref,
        token=github_token,
        migrations_path=migrations_path,
    )
    if downloaded == 0:
        print('ERROR: No migration files found!')
        return 1

    # Execute the command
    if migration_command == 'status':
        exit_code = cmd_status(database_url)
    elif migration_command == 'up':
        exit_code = cmd_up(database_url)
    else:
        print(f"ERROR: Unknown command '{migration_command}'")
        print('Supported commands: status, up, down')
        return 1

    print()
    print('Done.')
    return exit_code


if __name__ == '__main__':
    sys.exit(main())
