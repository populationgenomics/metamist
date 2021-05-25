# pylint: disable=logging-not-lazy
import os
import tempfile
import shutil
import time
import logging
import subprocess
from typing import Optional

DOCKER_IMAGE = os.getenv('SM_DOCKER', 'docker.io/michaelfranklin/sample-meta:dev')

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

    # TODO: copy files from here
    print('LISTDIR', os.listdir(tmpdir))

    shutil.rmtree(tmpdir)


if __name__ == '__main__':
    process = start_server(with_docker=bool(os.getenv('SM_USE_DOCKER')))
    generate_api_and_copy()
    process.kill()
