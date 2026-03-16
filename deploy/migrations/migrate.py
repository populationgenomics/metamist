#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pygithub",
#     "fastapi",
#     "uvicorn",
#     "pydantic",
# ]
# ///

import os
import subprocess
from pathlib import Path
from typing import Literal

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from github import Auth, Github
from pydantic import BaseModel


LISTEN_PORT = 8080
MIGRATIONS_DIR = Path('/db/migrations')

app = FastAPI(title='Metamist db migrations')


class MigrationRequest(BaseModel):
    """Request structure for triggering migration"""

    command: Literal['status', 'up']
    github_ref: str
    migrations_path: str = 'db/migrations'


class MigrationResponse(BaseModel):
    """Response structure for migration results"""

    success: bool
    github_repository: str | None = None
    github_ref: str
    dbmate_output: str | None = None
    error_message: str | None = None


def download_migrations_from_github(
    repository: str,
    ref: str,
    token: str,
    migrations_path: str = 'db/migrations',
) -> int:
    """Get migration files from github"""

    # Extract the actual branch/tag name
    clean_ref = ref
    if ref.startswith('refs/heads/'):
        clean_ref = ref.removeprefix('refs/heads/')
    elif ref.startswith('refs/tags/'):
        clean_ref = ref.removeprefix('refs/tags/')

    # Validate against ALLOWED_BRANCH if set
    allowed_branch = os.environ.get('ALLOWED_BRANCH')
    if allowed_branch and clean_ref != allowed_branch:
        raise ValueError(
            f'Migration on ref "{ref}" is not allowed. '
            f'This service is restricted to the "{allowed_branch}" branch.'
        )

    auth = Auth.Token(token)
    gh = Github(auth=auth)
    repo = gh.get_repo(repository)

    contents = repo.get_contents(migrations_path, ref=ref)

    if not isinstance(contents, list):
        raise ValueError(f'Expected a directory at {migrations_path}, but got a file.')

    downloaded = 0
    for item in contents:
        if item.type == 'file' and item.name.endswith('.sql'):
            local_path = MIGRATIONS_DIR / item.name
            content = item.decoded_content.decode('utf-8')
            local_path.write_text(content)
            downloaded += 1

    gh.close()
    return downloaded


def run_dbmate(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    """Execute a dbmate command"""
    cmd = [
        'dbmate',
        '--url',
        database_url,
        '--migrations-dir',
        str(MIGRATIONS_DIR),
        *args,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, check=True)


def run_migrations(
    github_ref: str,
    migrations_path: str,
    migration_command: Literal['status', 'up'],
):
    """Run the migrations for the given repository and ref"""
    database_url_secret = os.environ.get('DATABASE_URL_SECRET')
    if not database_url_secret:
        raise ValueError('DATABASE_URL_SECRET environment variable is required')

    github_token_secret = os.environ.get('GITHUB_TOKEN_SECRET')
    if not github_token_secret:
        raise ValueError('GITHUB_TOKEN_SECRET environment variable is required')

    github_repository = os.environ.get('ALLOWED_REPOSITORY')
    if not github_repository:
        raise ValueError('ALLOWED_REPOSITORY environment variable is required')

    MIGRATIONS_DIR.mkdir(parents=True, exist_ok=True)

    downloaded = download_migrations_from_github(
        repository=github_repository,
        ref=github_ref,
        token=github_token_secret,
        migrations_path=migrations_path,
    )

    if downloaded == 0:
        raise ValueError(
            f'No migration files found in {migrations_path} for {github_repository}@{github_ref}'
        )

    dbmate_output: str = ''
    error_message: str | None = None
    if migration_command not in ['status', 'up']:
        raise ValueError(f'Unsupported migration command: {migration_command}')
    try:
        cmd_result = run_dbmate(database_url_secret, migration_command)
        dbmate_output = (cmd_result.stdout or '') + '\n' + (cmd_result.stderr or '')
        success = True
    except subprocess.CalledProcessError as e:
        dbmate_output = (e.stdout or '') + '\n' + (e.stderr or '')
        success = False
        error_message = str(e)

    response = MigrationResponse(
        success=success,
        github_repository=github_repository,
        github_ref=github_ref,
        dbmate_output=dbmate_output,
        error_message=error_message,
    )

    return response


@app.post('/migrate', response_model=MigrationResponse)
def migrate(request: MigrationRequest):
    """Migration route handler"""

    response: MigrationResponse | None = None
    status_code = 200
    github_repository = os.environ.get('ALLOWED_REPOSITORY')

    try:
        response = run_migrations(
            github_ref=request.github_ref,
            migrations_path=request.migrations_path,
            migration_command=request.command,
        )

    except ValueError as ve:
        response = MigrationResponse(
            success=False,
            github_repository=github_repository,
            github_ref=request.github_ref,
            error_message=str(ve),
        )
        status_code = 400

    except Exception as e:  # noqa: BLE001 - we want to catch everything here
        error_message = f'Migration failed: {str(e)}'
        response = MigrationResponse(
            success=False,
            github_repository=github_repository,
            github_ref=request.github_ref,
            error_message=error_message,
        )
        status_code = 500

    return JSONResponse(status_code=status_code, content=response.model_dump())


if __name__ == '__main__':
    port = int(os.environ.get('PORT', LISTEN_PORT))
    uvicorn.run(app, host='0.0.0.0', port=port)
