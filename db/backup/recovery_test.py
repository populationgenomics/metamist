""" A script to test that tests the validity of a database backup
in the event of recovery.
NOTE: DO NOT RUN THIS SCRIPT ON A PRODUCTION SERVER.
It will drop the local mysql database after each run.

"""

import os
import unittest
import subprocess
from collections import namedtuple
from google.cloud import storage
import mysql.connector


BACKUP_BUCKET = 'cpg-sm-backups'
LOCAL_BACKUP_FOLDER = 'latest_backup'
DATABASE = 'sm_production'

ConnectionDetails = namedtuple('ConnectionDetails', 'address user')


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

    def test_row_number(self):
        """ Tests that the count of rows for each table matches """
        tables = get_results('show tables;', self.local_connection)
        for table in tables:
            if table[0] == 'DATABASECHANGELOG' or table[0] == 'DATABASECHANGELOGLOCK':
                continue
            restored_count = get_results(
                f'SELECT COUNT(*) FROM {table[0]};', self.local_connection
            )
            prod_count = get_results(
                f"SELECT COUNT(*) FROM {table[0]} FOR SYSTEM_TIME AS OF TIMESTAMP'{self.timestamp}'",
                self.prod_connection,
            )

            self.assertEqual(restored_count, prod_count)

    def test_rows_top(self):
        """ Validates the top 10 rows in each table match """
        tables = get_results('show tables;', self.local_connection)
        for table in tables:
            if table[0] == 'DATABASECHANGELOG' or table[0] == 'DATABASECHANGELOGLOCK':
                continue
            restored_results = get_results(
                f'SELECT * FROM {table[0]} LIMIT 10;', self.local_connection
            )
            prod_results = get_results(
                f"SELECT * FROM {table[0]} FOR SYSTEM_TIME AS OF TIMESTAMP'{self.timestamp}' LIMIT 10;",
                self.prod_connection,
            )

            self.assertEqual(restored_results, prod_results)

    def test_random_rows_ids(self):
        """Pulls 10 random rows for testing.
        Operates on tables with a unique id field"""

        # Tables with unique id fields.
        tables = [
            'analysis',
            'family',
            'participant',
            'project ',
            'sample',
            'sample_sequencing',
        ]

        for table in tables:

            restored_random_results = get_results(
                f'SELECT * FROM {table} ORDER BY RAND() LIMIT 10', self.local_connection
            )
            sample_ids = []
            for sample in restored_random_results:
                sample_ids.append(sample[0])

            if len(sample_ids) == 0:
                continue

            sample_string = ','.join([str(elem) for elem in sample_ids])
            prod_random_results = get_results(
                f"SELECT * FROM {table} FOR SYSTEM_TIME AS OF TIMESTAMP'{self.timestamp}' WHERE id in ({sample_string}) ;",
                self.prod_connection,
            )
            sorted_local = sorted(restored_random_results, key=lambda tup: tup[0])
            sorted_prod = sorted(prod_random_results, key=lambda tup: tup[0])

            self.assertEqual(sorted_local, sorted_prod)

    def test_random_rows_analysis_sample(self):
        """ Test 10 random rows in the analysis_sample table """
        restored_random_results = get_results(
            f'SELECT * FROM analysis_sample ORDER BY RAND() LIMIT 10',
            self.local_connection,
        )
        for sample in restored_random_results:
            analysis_id = sample[0]
            sample_id = sample[1]

            prod_random_results = get_results(
                f"SELECT * FROM analysis_sample FOR SYSTEM_TIME AS OF TIMESTAMP'{self.timestamp}' WHERE analysis_id={analysis_id} and sample_id={sample_id} ;",
                self.prod_connection,
            )
            self.assertEqual(sample, prod_random_results[0])

    def test_random_rows_family_participant(self):
        """ Test 10 random rows in the family_participant table """
        restored_random_results = get_results(
            f'SELECT * FROM family_participant ORDER BY RAND() LIMIT 10',
            self.local_connection,
        )
        for entry in restored_random_results:
            family_id = entry[0]
            participant_id = entry[1]

            prod_random_results = get_results(
                f"SELECT * FROM family_participant FOR SYSTEM_TIME AS OF TIMESTAMP'{self.timestamp}' WHERE family_id={family_id} and participant_id={participant_id} ;",
                self.prod_connection,
            )
            self.assertEqual(entry, prod_random_results[0])

    def test_random_rows_participant_phenotypes(self):
        """ Test 10 random rows in the participant_phenotypes table """
        restored_random_results = get_results(
            f'SELECT * FROM participant_phenotypes ORDER BY RAND() LIMIT 10',
            self.local_connection,
        )
        for entry in restored_random_results:
            participant_id = entry[0]
            hpo_term = entry[1]

            prod_random_results = get_results(
                f"SELECT * FROM participant_phenotypes FOR SYSTEM_TIME AS OF TIMESTAMP'{self.timestamp}' WHERE participant_id={participant_id} and hpo_term={hpo_term} ;",
                self.prod_connection,
            )
            self.assertEqual(entry, prod_random_results[0])

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


def get_results(query: str, connection: ConnectionDetails):
    """ Returns the results from a provided query at a given connection """
    conn = mysql.connector.connect(user=connection.user, host=connection.address)
    cursor = conn.cursor()
    cursor.execute(f'use {DATABASE}')
    cursor.execute(query)
    results = list(cursor)
    return results
