#!/usr/bin/python3.7

""" Daily back up function for databases within a local
MariaDB instance """

from datetime import datetime
from typing import List
import subprocess
from google.cloud import storage
from google.cloud import logging
import mysql.connector
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

    databases = get_dbs()

    # Export SQL Data
    try:
        backup_sql = subprocess.check_output(
            ['sudo', 'mysqldump', '--lock-tables', '--databases'] + databases
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
    stats = storage.Blob(bucket=bucket, name=filepath).exists(client)
    if stats:
        text = f'Successful backup {timestamp_str} UTC'
        logger.log_text(text, severity='INFO')
    else:
        text = f'Failed to upload backup to GCS. {timestamp_str} UTC. \
Could not find the file within {bucket.name}'
        logger.log_text(text, severity='ERROR')


def get_dbs():
    """ Obtain a list of databases that exist on a local mariadb instance """
    conn = mysql.connector.connect(user='root', host='localhost')
    cursor = conn.cursor()
    cursor.execute('show databases')

    databases: List[str] = []
    for db in cursor:
        databases.append(db[0])

    databases.remove('information_schema')
    databases.remove('mysql')
    databases.remove('performance_schema')

    return databases


if __name__ == '__main__':
    perform_backup()
