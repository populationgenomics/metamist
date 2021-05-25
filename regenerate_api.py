# pylint: disable=logging-not-lazy
import os
import tempfile
import shutil
import time
import logging
import subprocess
from typing import Optional

DOCKER_IMAGE = os.getenv('SM_DOCKER', 'docker.io/michaelfranklin/sample-meta:dev')
PACKAGE_NAME = 'sample_metadata'

logging.basicConfig(level='DEBUG')
logger = logging.getLogger(__name__)


def start_server(with_docker=False) -> Optional[subprocess.Popen]:
    """Start the API server, and return a process when it's started"""

    command = ['python', '-m', 'api.server']

    if with_docker:
        command = [
            'docker',
            'run',
            '-eSM_HOST=0.0.0.0',
            '-eSM_IGNORE_GCP_CREDENTIALS_ERROR=1',
            '-p5000:5000',
            DOCKER_IMAGE,
            *command,
        ]

    logger.info('Starting API server with: ' + ' '.join(command))

    _process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    for c in iter(_process.stdout.readline, 'b'):
        line = None
        if c:
            line = c.decode('utf-8').rstrip()
            if line is not None:
                logger.info('API: ' + line)
                if 'Running on http' in line:
                    # server has been started
                    return _process

        rc = _process.poll()
        if rc is not None:
            # the process exited early
            logger.error(f'Server exited with rc={rc}')
            if with_docker:
                logger.warning(
                    "If you're receiving a 'port is already allocated' message, "
                    "run 'docker ps' and make sure the container isn't already running."
                )
            raise SystemExit(1)

    return None


def generate_api_and_copy():
    """Get JSON from server"""
    tmpdir = tempfile.mkdtemp()
    command = [
        'openapi-generator',
        'generate',
        '-i',
        'http://localhost:5000/api/schema.json',
        '-g',
        'python',
        '-o',
        tmpdir,
        '--package-name',
        'sample_metadata',
        '--skip-validate-spec',
    ]
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
            logger.warning(f'openapi generation failed, trying {i-1} more times: {e}')
            time.sleep(2)

    if not succeeded:
        return

    copy_files_from(tmpdir)

    shutil.rmtree(tmpdir)


def copy_files_from(tmpdir):
    """
    Copy a selection of API files generated from openapi-generator:

        FROM:   $tmpdir/sample_metadata
        TO:     ./sample_metadata

    This clears the ./sample_metadata folder except for 'files_to_ignore'.
    """

    files_to_ignore = {'configuration.py'}

    dir_to_copy_to = 'sample_metadata'  # should be relative to this script
    dir_to_copy_from = os.path.join(tmpdir, 'sample_metadata')

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


if __name__ == '__main__':
    process = start_server(with_docker=bool(os.getenv('SM_USE_DOCKER')))
    try:
        generate_api_and_copy()
    # pylint: disable=broad-except
    except Exception as e:
        logger.error(str(e))

    logger.info('Killing docker container')
    process.kill()
