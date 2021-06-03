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
    try:
        backup_sql = subprocess.check_output(
            ['sudo', 'mysqldump', '--lock-tables', '--all-databases']
        )
        text = f'Performed mysqldump to pull data {timestamp_str} UTC.'
        logger.log_text(text, severity='INFO')

    except subprocess.SubprocessError:
        text = f'Failed to export backup {timestamp_str} UTC.'
        logger.log_text(text, severity='ERROR')
        return

    # Write backup to file
    filepath = f'backups/backup_{timestamp_str}.sql'
    with open(filepath, 'xb') as f:
        f.write(backup_sql)

    # Connect to the GCS client
    client = storage.Client()
    bucket = client.get_bucket('sm-dev-sm')

    # Upload file to GCS
    blob = bucket.blob(filepath)
    blob.upload_from_filename(filepath)

    # Validates file exists and was uploaded to GCS
    file_exists = storage.Blob(bucket=bucket, name=filepath).exists(client)
    if file_exists:
        text = f'Successful backup {timestamp_str} UTC'
        logger.log_text(text, severity='INFO')
    else:
        text = f'Failed to upload backup to GCS. {timestamp_str} UTC. \
Could not find the file within {bucket.name}'
        logger.log_text(text, severity='ERROR')


if __name__ == '__main__':
    perform_backup()
