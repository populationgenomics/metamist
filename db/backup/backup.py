#!/usr/bin/python3
# pylint: disable=broad-exception-caught,broad-exception-raised
""" Daily back up function for databases within a local
MariaDB instance """

import json
import os
import subprocess
from datetime import datetime
from typing import Literal

from google.cloud import logging, secretmanager, storage

STORAGE_CLIENT = storage.Client()
LOGGING_CLIENT = logging.Client()
SECRET_CLIENT = secretmanager.SecretManagerServiceClient()

SECRET_PROJECT = 'sample-metadata'
SECRET_NAME = 'mariadb-backup-user-credentials'


def read_db_credentials() -> dict[Literal['username', 'password'], str]:
    """Get database credentials from Secret Manager."""
    try:
        # noinspection PyTypeChecker
        secret_path = SECRET_CLIENT.secret_version_path(
            SECRET_PROJECT, SECRET_NAME, 'latest'
        )
        response = SECRET_CLIENT.access_secret_version(request={'name': secret_path})
        return json.loads(response.payload.data.decode('UTF-8'))
    except Exception as e:
        # Fail gracefully if there's no secret version yet.
        raise Exception(f'Could not access database credentials: {e}') from e


def perform_backup():
    """Completes a backup of the databases within a local mariadb instance
    and uploads this backup to GCS."""

    # Logging: Any log with `severity >= ERROR` get's logged to #software-alerts

    log_name = 'backup_log'
    logger = LOGGING_CLIENT.logger(log_name)

    # Get timestamp
    timestamp_str = (
        datetime.utcnow().isoformat(timespec='seconds').replace(':', '-') + 'Z'
    )

    # Set up tmp dir
    tmp_dir = f'backup_{timestamp_str}'
    subprocess.run(['mkdir', tmp_dir], check=True)
    # grant permissions, so that mariadb can read ib_logfile0
    subprocess.run(['sudo', 'chmod', '-R', '770', tmp_dir], check=True)

    credentials = read_db_credentials()
    db_username = credentials['username']
    db_password = credentials['password']

    # Export SQL Data
    try:
        text = f'Performed mariabackup to pull data {timestamp_str}.'
        logger.log_text(text, severity='INFO')
        subprocess.run(
            [
                'mariabackup',
                '--backup',
                f'--target-dir={tmp_dir}/',
                f'--user={db_username}',
            ],
            check=True,
            stderr=subprocess.DEVNULL,
            # pass the password with stdin to avoid it being visible in the process list
            env={'MYSQL_PWD': db_password, **os.environ},
        )

    except subprocess.CalledProcessError as e:
        text = f'Failed to export backup {timestamp_str}: {e}\n {e.stderr}'
        logger.log_text(text, severity='ERROR')
        return
    except Exception as e:
        text = f'Failed to export backup {timestamp_str}: {e}'
        logger.log_text(text, severity='ERROR')
        return

    # mariabackup creates awkward permissions for the output files,
    # so we'll grant appropriate permissions for tmp_dir to later remove it
    subprocess.run(['sudo', 'chmod', '-R', '770', tmp_dir], check=True)

    # tar the archive to make it easier to upload to GCS
    tar_archive_path = f'{tmp_dir}.tar.gz'
    subprocess.run(['tar', '-czf', tar_archive_path, tmp_dir], check=True)

    # Upload tar file to GCS
    # GCS Client Library does not support moving directories
    try:
        subprocess.run(
            ['gsutil', '-m', 'mv', tar_archive_path, 'gs://cpg-sm-backups'], check=True
        )
    except subprocess.CalledProcessError as e:
        text = (
            f'Failed to upload backup {timestamp_str} to GCS. '
            f'GSUtil failed to upload with error: {e.stderr}'
        )
        logger.log_text(text, severity='ERROR')

    # Cleans up empty directories after the move
    subprocess.run(['rm', '-rf', tmp_dir, tar_archive_path], check=True)

    # Validates file exists and was uploaded to GCS
    bucket = STORAGE_CLIENT.get_bucket('cpg-sm-backups')
    files_in_gcs = list(STORAGE_CLIENT.list_blobs(bucket, prefix=tar_archive_path))

    if len(files_in_gcs) > 0:
        text = f'Successfully backed up: {timestamp_str}'
        logger.log_text(text, severity='INFO')
    else:
        text = (
            f'Failed to upload backup to GCS: {timestamp_str}. '
            f'Could not find the file gs://{bucket.name}/{tar_archive_path}'
        )
        logger.log_text(text, severity='ERROR')
        raise Exception(text)


if __name__ == '__main__':
    perform_backup()
