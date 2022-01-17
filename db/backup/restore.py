""" A script to restore the database instance to the latest
backup """

import os
import subprocess
from google.cloud import storage

BACKUP_BUCKET = 'cpg-sm-backups'
LOCAL_BACKUP_FOLDER = 'latest_backup'


def restore(backup_folder=None):
    """Restore the database"""
    print('Starting Restoration')
    if backup_folder is None:
        backup_folder = pull_latest_backup(BACKUP_BUCKET)

    subprocess.run(['mkdir', LOCAL_BACKUP_FOLDER], check=True)
    subprocess.run(
        [
            'gsutil',
            '-m',
            'cp',
            '-r',
            f'gs://{BACKUP_BUCKET}/{backup_folder}',
            LOCAL_BACKUP_FOLDER,
        ],
        check=True,
    )
    # Prepare the backup for restoration
    print('Preparing Backup')
    subprocess.run(
        [
            'mariabackup',
            '--prepare',
            f'--target-dir={LOCAL_BACKUP_FOLDER}/{backup_folder}',
        ],
        check=True,
    )
    # Stop the MariaDB server
    subprocess.run(['sudo', 'systemctl', 'stop', 'mariadb'], check=True)

    if os.path.isdir('/var/lib/mysql/'):
        raise RuntimeError(
            'Restore cannot be performed unless /var/lib/mysql is empty.'
        )

    # Restore
    subprocess.run(
        [
            'sudo',
            'mariabackup',
            '--copy-back',
            f'--target-dir={LOCAL_BACKUP_FOLDER}/{backup_folder}',
        ],
        check=True,
    )
    # Fix permissions
    subprocess.run(
        ['sudo', 'chown', '-R', 'mysql:mysql', '/var/lib'],
        check=True,
    )
    # Start the MariaDB server
    subprocess.run(['sudo', 'systemctl', 'start', 'mariadb'], check=True)

    print('Database Restored')
    subprocess.run(['rm', '-r', LOCAL_BACKUP_FOLDER], check=True)


def pull_latest_backup(backup_bucket):
    """Assumes no changes to metadata since initial upload"""
    print('Pulling Latest Backup from GCS')
    storage_client = storage.Client()
    blobs = [(blob, blob.updated) for blob in storage_client.list_blobs(backup_bucket)]
    latest = sorted(blobs, key=lambda tup: tup[1])[-1][0]
    full_path = os.path.dirname(latest.name)
    backup_folder = os.path.dirname(full_path)
    return backup_folder


if __name__ == '__main__':
    restore()
