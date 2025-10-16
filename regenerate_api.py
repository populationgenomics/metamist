#!/usr/bin/env python3
# pylint: disable=logging-not-lazy,subprocess-popen-preexec-fn,consider-using-with
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from functools import lru_cache

from api.server import _VERSION, app
from api.utils.openapi import get_openapi_3_0_schema

OPENAPI_COMMAND = os.getenv('OPENAPI_COMMAND', 'openapi-generator-cli').split(' ')
MODULE_NAME = 'metamist'

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

STATIC_DIR = 'web/src/static'
OUTPUT_DOCS_DIR = os.path.join(STATIC_DIR, 'sm_docs')
MODULE_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), MODULE_NAME)


def _get_openapi_version():
    # two different versions of openapi
    # require two different ways to get the version

    version_cmds = ['--version', 'version']

    has_timeout = False
    for version_cmd in version_cmds:
        command = [*OPENAPI_COMMAND, version_cmd]
        try:
            return subprocess.check_output(command, stderr=subprocess.PIPE, timeout=10)

        except subprocess.TimeoutExpired:
            has_timeout = True
            # sometimes a timeout means that it's waiting for stdin because openapi
            # is misconfigured, so try the next command and then tell the user
            continue

        except subprocess.CalledProcessError:
            continue

    if has_timeout:
        _command = ' '.join([*OPENAPI_COMMAND, version_cmds[-1]])
        raise ValueError(
            'Could not get version of openapi as the command timed out, this might '
            f'mean openapi is misconfigured. Try running "{_command}" in your terminal.'
        )

    raise ValueError('Could not get version of openapi')


def check_openapi_version():
    """
    Check compatible OpenAPI version
    """
    out = _get_openapi_version().decode().split('\n', maxsplit=1)[0].strip()

    version_match = re.search(pattern=r'\d+\.\d+\.\d+', string=out)
    if not version_match:
        raise ValueError(f'Could not detect version of openapi-generator from {out!r}')

    version = version_match.group()
    major = version.split('.')[0]
    if int(major) != 5:
        raise ValueError(
            f'openapi-generator must be version 5.x.x, received: {version}'
        )
    logger.info(f'Got openapi version: {version}')


@lru_cache
def get_openapi_schema() -> dict:
    """Get the OpenAPI schema (3.0) as a dictionary"""
    return get_openapi_3_0_schema(app, _VERSION)


def generate_api_and_copy(
    output_type, output_copyer, extra_commands: list[str] | None = None
):
    """
    Use OpenApiGenerator to generate the installable API
    """
    with open('deploy/python/version.txt', encoding='utf-8') as f:
        version = f.read().strip()

    # write to temporary file with extension .json
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json') as f:
        schema = get_openapi_schema()
        json.dump(schema, f)
        # flush anything in memory to disk
        f.flush()

        tmpdir = tempfile.mkdtemp()
        command = [
            *OPENAPI_COMMAND,
            'generate',
            *('-i', f.name),
            *('-g', output_type),
            *('-o', tmpdir),
            *('--package-name', MODULE_NAME),
            *(extra_commands or []),
            *('--artifact-version', version),
            '--skip-validate-spec',
        ]
        # quotes commands by calling repr on each element
        jcom = ' '.join(map(repr, command))
        logger.info('Generating with command: ' + jcom)
        # 5 attempts
        n_attempts = 1
        succeeded = False
        for i in range(n_attempts, 0, -1):
            try:
                stdout = subprocess.check_output(command)
                logger.info('Generated API: ' + str(stdout.decode()))
                succeeded = True
                break

            except subprocess.CalledProcessError as e:
                logger.warning(
                    f'openapi generation failed, trying {i-1} more times: {e}'
                )
                time.sleep(2)

        if not succeeded:
            raise RuntimeError(
                f'openapi generation failed after trying {n_attempts} time(s)'
            )

        output_copyer(tmpdir)
        shutil.rmtree(tmpdir)


def generate_schema_file():
    """
    Generate schema file and place in the metamist/graphql/ directory
    """
    command = ['strawberry', 'export-schema', 'api.graphql.schema:schema']
    schema = subprocess.check_output(command).decode()

    with open(os.path.join(MODULE_DIR, 'graphql/schema.graphql'), 'w+') as f:
        f.write(schema)


def copy_typescript_files_from(tmpdir):
    """Copy typescript files to web/src/sm-api/"""
    files_to_ignore = {
        'README.md',
        '.gitignore',
        '.npmignore',
        '.openapi-generator',
        '.openapi-generator-ignore',
        'git_push.sh',
    }

    dir_to_copy_to = 'web/src/sm-api/'  # should be relative to this script
    dir_to_copy_from = tmpdir

    if not os.path.exists(dir_to_copy_to):
        os.makedirs(dir_to_copy_to)
    if not os.path.exists(dir_to_copy_from):
        raise FileNotFoundError(
            f"Directory to copy from doesn't exist ({dir_to_copy_from})"
        )

    # remove everything from dir_to_copy_to except those in files_to_ignore
    logger.info('Removing files from dest directory ' + dir_to_copy_to)
    for file_to_remove in os.listdir(dir_to_copy_to):
        if file_to_remove in files_to_ignore:
            continue
        path_to_remove = os.path.join(dir_to_copy_to, file_to_remove)
        if os.path.isdir(path_to_remove):
            shutil.rmtree(path_to_remove)
        else:
            os.remove(path_to_remove)

    files_to_copy = os.listdir(dir_to_copy_from)
    logger.info(f'Copying {len(files_to_copy)} files / directories to {dir_to_copy_to}')
    for file_to_copy in files_to_copy:
        if file_to_copy in files_to_ignore:
            continue

        path_to_copy = os.path.join(dir_to_copy_from, file_to_copy)
        output_path = os.path.join(dir_to_copy_to, file_to_copy)
        if os.path.isdir(path_to_copy):
            shutil.copytree(path_to_copy, output_path)
        else:
            shutil.copy(path_to_copy, output_path)


def copy_python_files_from(tmpdir):
    """
    Copy a selection of API files generated from openapi-generator:

        FROM:   $tmpdir/metamist
        TO:     ./metamist

    This clears the ./metamist folder except for 'files_to_ignore'.
    """

    files_to_ignore = {'README.md', 'parser', 'graphql', 'audit'}

    dir_to_copy_to = MODULE_DIR  # should be relative to this script
    dir_to_copy_from = os.path.join(tmpdir, MODULE_NAME)

    if not os.path.exists(dir_to_copy_to):
        raise FileNotFoundError(
            f"Directory to copy to doesn't exist ({dir_to_copy_to})"
        )
    if not os.path.exists(dir_to_copy_from):
        raise FileNotFoundError(
            f"Directory to copy from doesn't exist ({dir_to_copy_from})"
        )

    # remove everything from dir_to_copy_to except those in files_to_ignore
    logger.info('Removing files from dest directory ' + dir_to_copy_to)
    for file_to_remove in os.listdir(dir_to_copy_to):
        if file_to_remove in files_to_ignore:
            continue
        path_to_remove = os.path.join(dir_to_copy_to, file_to_remove)
        if os.path.isdir(path_to_remove):
            shutil.rmtree(path_to_remove)
        else:
            os.remove(path_to_remove)

    files_to_copy = os.listdir(dir_to_copy_from)
    logger.info(f'Copying {len(files_to_copy)} files / directories to {dir_to_copy_to}')
    for file_to_copy in files_to_copy:
        if file_to_copy in files_to_ignore:
            continue

        path_to_copy = os.path.join(dir_to_copy_from, file_to_copy)
        output_path = os.path.join(dir_to_copy_to, file_to_copy)
        if os.path.isdir(path_to_copy):
            shutil.copytree(path_to_copy, output_path)
        else:
            shutil.copy(path_to_copy, output_path)

    docs_dir = os.path.join(tmpdir, 'docs')
    if os.path.exists(OUTPUT_DOCS_DIR):
        shutil.rmtree(OUTPUT_DOCS_DIR)
    if not os.path.exists(STATIC_DIR):
        os.makedirs(STATIC_DIR)
    shutil.copytree(docs_dir, OUTPUT_DOCS_DIR)
    shutil.copy(
        os.path.join(tmpdir, 'README.md'), os.path.join(OUTPUT_DOCS_DIR, 'README.md')
    )


def main():
    """
    Generates installable python API using:
        - Start API server (if applicable);
        - Call openapi-generator to generate python API to temp folder;
        - Empty the 'metamist' folder (except for some files);
        - Copy relevant files to 'metamist' in CWD;
        - Stop the server (if applicable)

    """
    # check openapi version first, because it seems to be fairly sketchy
    check_openapi_version()
    # Generate the installable Python API
    generate_api_and_copy(
        'python',
        copy_python_files_from,
        ['--template-dir', 'openapi-templates'],
    )

    # Generate the Typescript API for React application
    generate_api_and_copy(
        'typescript-axios',
        copy_typescript_files_from,
    )

    # Generate the GraphQL schema
    generate_schema_file()

    # Copy resources and README
    shutil.copy(
        './resources/muck-the-duck.svg',
        os.path.join('web/src', 'muck-the-duck.svg'),
    )
    shutil.copy(
        'README.md',
        os.path.join(OUTPUT_DOCS_DIR, 'index.md'),
    )


if __name__ == '__main__':
    main()
