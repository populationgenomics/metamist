#!/usr/bin/python3.7
# pylint: disable=broad-exception-caught,broad-exception-raised
""" Daily back up function for databases within a local
MariaDB instance """

from datetime import datetime
import subprocess
from google.cloud import storage
from google.cloud import logging


def perform_backup():
    """Completes a backup of the databases within a local mariadb instance
    and uploads this backup to GCS."""

    # Logging: Any log with `severity >= ERROR` get's logged to #software-alerts
    logging_client = logging.Client()
    log_name = 'backup_log'
    logger = logging_client.logger(log_name)

    # Get timestamp
    timestamp_str = (
        datetime.utcnow().isoformat(timespec='seconds').replace(':', '-') + 'Z'
    )

    # Set up tmp dir
    tmp_dir = f'backup_{timestamp_str}'
    subprocess.run(['mkdir', tmp_dir], check=True)
    # grant permissions, so that mariadb can read ib_logfile0
    subprocess.run(['sudo', 'chmod', '-R', '777', tmp_dir], check=True)

    # Export SQL Data
    try:
        text = f'Performed mariabackup to pull data {timestamp_str}.'
        logger.log_text(text, severity='INFO')
        subprocess.run(
            [
                'mariabackup',
                '--backup',
                f'--target-dir={tmp_dir}/',
                '--user=root',
            ],
            check=True,
            stderr=subprocess.DEVNULL,
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
    subprocess.run(['sudo', 'chmod', '-R', '777', tmp_dir], check=True)

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
    client = storage.Client()
    bucket = client.get_bucket('cpg-sm-backups')
    files_in_gcs = list(client.list_blobs(bucket, prefix=tar_archive_path))

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
