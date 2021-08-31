""" A script to test that tests the validity of a database backup
in the event of recovery.
NOTE: DO NOT RUN THIS SCRIPT ON A PRODUCTION SERVER.
It will drop the local mysql database after each run.

"""

import os
import unittest
import subprocess
from typing import Tuple, Optional
from collections import namedtuple
from google.cloud import storage
import mysql.connector
from parameterized import parameterized


BACKUP_BUCKET = 'cpg-sm-backups'
LOCAL_BACKUP_FOLDER = 'latest_backup'
DATABASE = 'sm_production'

ConnectionDetails = namedtuple('ConnectionDetails', 'address user')

TABLES = [
    ('analysis'),
    ('family'),
    ('participant'),
    ('project '),
    ('sample'),
    ('sample_sequencing'),
    ('analysis_sample'),
    ('participant_phenotypes'),
    ('family_participant'),
]

FIELDS = [
    ('analysis', ('id',)),
    ('family', ('id',)),
    ('participant', ('id',)),
    ('project ', ('id',)),
    ('sample', ('id',)),
    ('sample_sequencing', ('id',)),
    ('analysis_sample', ('analysis_id', 'sample_id')),
    ('participant_phenotypes', ('participant_id', 'hpo_term', 'description')),
    ('family_participant', ('family_id', 'participant_id')),
]


class TestDatabaseBackup(unittest.TestCase):
    """ Testing validity of DB backup"""

    @classmethod
    def setUpClass(cls):
        """ Pull the backup file, and restore the database."""
        backup_folder = pull_latest_backup()
        get_timestamp(backup_folder)
        # Pull the latest backup
        subprocess.run(['mkdir', LOCAL_BACKUP_FOLDER], check=True)
        subprocess.run(
            [
                'gsutil',
                '-m',
                'cp',
                '-r',
                f'gs://cpg-sm-backups/{backup_folder}',
                LOCAL_BACKUP_FOLDER,
            ],
            check=True,
        )
        # Prepare the backup for restoration
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
            print('Backup cannot be performed unless /var/lib/mysql is empty.')
        else:
            print('Performing backup')
        # Perform backup
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

    def setUp(self):
        self.prod_connection = ConnectionDetails('10.152.0.2', 'backup')
        self.local_connection = ConnectionDetails('localhost', 'root')
        backup_folder = pull_latest_backup()
        self.timestamp = get_timestamp(backup_folder)

    def test_database_exists(self):
        """Validates that the db in the production
        database matches those produced by the restored db"""
        backup_databases = get_databases(self.local_connection)
        prod_databases = get_databases(self.prod_connection)
        self.assertEqual(backup_databases, prod_databases)

    @parameterized.expand(TABLES)
    def test_row_number(self, table):
        """ Tests that the count of rows for each table matches """

        restored_count = get_results(
            self.local_connection,
            f'SELECT COUNT(*) FROM {table};',
        )
        prod_count = get_results(
            self.local_connection,
            f"SELECT COUNT(*) FROM {table} FOR SYSTEM_TIME AS OF TIMESTAMP'{self.timestamp}'",
        )

        self.assertEqual(restored_count, prod_count)

    @parameterized.expand(TABLES)
    def test_rows_top(self, table):
        """ Validates the top 10 rows in each table match """
        restored_results = get_results(
            self.local_connection,
            f'SELECT * FROM {table} LIMIT 10;',
        )
        prod_results = get_results(
            self.prod_connection,
            f"SELECT * FROM {table} FOR SYSTEM_TIME AS OF TIMESTAMP'{self.timestamp}' LIMIT 10;",
        )
        self.assertEqual(restored_results, prod_results)

    @parameterized.expand(FIELDS)
    def test_random_rows_ids(self, table, id_fields: Tuple[str]):
        """Pulls 10 random rows for testing.
        Operates on tables with a unique id field"""

        restored_random_results = get_results(
            self.local_connection,
            f'SELECT * FROM {table} ORDER BY RAND() LIMIT 10',
        )

        for row in restored_random_results:
            ids = row[: len(id_fields)]
            wheres_str = ' AND '.join(f'{field} = %s' for field in id_fields)

            _query = f"""\
SELECT *
FROM {table}
FOR SYSTEM_TIME AS OF TIMESTAMP'{self.timestamp}'
WHERE {wheres_str};"""

            prod_random_results = get_results(
                self.prod_connection,
                _query,
                ids,
            )
            self.assertEqual(row, prod_random_results[0])

    @classmethod
    def tearDownClass(cls):
        """ Delete test database following testing """
        subprocess.run(['rm', '-r', LOCAL_BACKUP_FOLDER], check=True)
        subprocess.run(['sudo', 'rm', '-r', '/var/lib/mysql'], check=True)


def pull_latest_backup():
    """ Assumes no changes to metadata since initial upload """
    storage_client = storage.Client()
    blobs = [(blob, blob.updated) for blob in storage_client.list_blobs(BACKUP_BUCKET)]
    latest = sorted(blobs, key=lambda tup: tup[1])[-1][0]
    full_path = os.path.dirname(latest.name)
    backup_folder = os.path.dirname(full_path)
    return backup_folder


def get_timestamp(folder: str):
    """Returns timestamp in format YYYY-MM-DD HH:MM:SS """
    timestamp = f'{folder[13:17]}-{folder[10:12]}-{folder[7:9]} {folder[18:20]}:{folder[21:23]}:{folder[24:26]}'
    return timestamp


def get_databases(connection: ConnectionDetails):
    """ Returns a list of the databases """
    conn = mysql.connector.connect(user=connection.user, host=connection.address)
    cursor = conn.cursor()
    cursor.execute('show databases')
    databases = list(cursor)
    return databases


def get_results(
    connection: ConnectionDetails, query: str, values: Optional[Tuple] = None
):
    """ Returns the results from a provided query at a given connection """
    conn = mysql.connector.connect(
        user=connection.user, host=connection.address, database=DATABASE
    )
    cursor = conn.cursor()
    cursor.execute(query, values)
    results = list(cursor)
    return results
