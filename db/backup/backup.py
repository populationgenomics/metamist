#!/usr/bin/python3.7

""" Daily back up function for databases within a local
MariaDB instance """

from datetime import datetime
import subprocess
from google.cloud import storage
from google.cloud import logging
import pytz


def perform_backup():
    """Completes a backup of the databases within a local mariadb instance
    and uploads this backup to GCS."""

    # Logging
    logging_client = logging.Client()
    log_name = 'backup_log'
    logger = logging_client.logger(log_name)

    # Get timestamp
    utc_now = pytz.utc.localize(datetime.utcnow())
    timestamp_str = utc_now.strftime('%d_%m_%Y_%H-%M-%S')

    # Export SQL Data
    tmp_dir = f'backup_{timestamp_str}'
    try:
        subprocess.run(
            [
                'sudo',
                'mariabackup',
                '--backup',
                f'--target-dir={tmp_dir}/',
                '--user=root',
            ],
            check=True,
        )
        text = f'Performed mysqldump to pull data {timestamp_str} UTC.'
        logger.log_text(text, severity='INFO')

    except subprocess.SubprocessError:
        text = f'Failed to export backup {timestamp_str} UTC.'
        logger.log_text(text, severity='ERROR')
        return

    # Upload file to GCS
    # GCS Client Library does not support moving directories
    subprocess.run(
        ['sudo', 'gsutil', '-m', 'mv', tmp_dir, 'gs://cpg-sm-backups'], check=True
    )
    # Clean up after the move
    subprocess.run(['sudo', 'rm', '-r', tmp_dir], check=True)

    # Validates file exists and was uploaded to GCS
    client = storage.Client()
    bucket = client.get_bucket('cpg-sm-backups')
    files_in_gcs = list(client.list_blobs(bucket, prefix=tmp_dir))

    if len(files_in_gcs) > 0:
        text = f'Successful backup {timestamp_str} UTC'
        logger.log_text(text, severity='INFO')
    else:
        text = f'Failed to upload backup to GCS. {timestamp_str} UTC. \
        Could not find the file within {bucket.name}'
        logger.log_text(text, severity='ERROR')


if __name__ == '__main__':
    perform_backup()
