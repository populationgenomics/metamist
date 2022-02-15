""" A script to test that tests the validity of a database backup
in the event of recovery.
NOTE: DO NOT RUN THIS SCRIPT ON A PRODUCTION SERVER.
It will drop the local mysql database after each run.

"""

import unittest
import subprocess
import json
import os
from typing import Tuple, Optional
from collections import namedtuple
import google.cloud.secretmanager
import mysql.connector
from parameterized import parameterized
from restore import pull_latest_backup, restore


BACKUP_BUCKET = 'cpg-sm-backups'
LOCAL_BACKUP_FOLDER = 'latest_backup'

ConnectionDetails = namedtuple('ConnectionDetails', 'address user password')

FIELDS = [
    ('analysis', ('id',)),
    ('family', ('id',)),
    ('participant', ('id',)),
    ('project ', ('id',)),
    ('sample', ('id',)),
    ('sample_sequencing', ('id',)),
    ('analysis_sample', ('analysis_id', 'sample_id')),
    ('participant_phenotypes', ('participant_id', 'hpo_term', 'description')),
    ('family_participant', ('participant_id',)),
]

TABLES = [field[0] for field in FIELDS]

secret_manager = google.cloud.secretmanager.SecretManagerServiceClient()

SECRET_NAME = 'projects/sample-metadata/secrets/db-validate-backup/versions/latest'
config_str = secret_manager.access_secret_version(
    request={'name': SECRET_NAME}
).payload.data.decode('UTF-8')
config = json.loads(config_str)

DATABASE = config['dbname']

PROD_HOST = config['p_host']
PROD_USER = config['p_username']
PROD_PASSWORD = config['p_password']

LOCAL_USER = os.environ.get('local_username', 'root')
LOCAL_HOST = os.environ.get('local_host', 'localhost')
LOCAL_PASSWORD = os.environ.get('local_password', '')


class TestDatabaseBackup(unittest.TestCase):
    """Testing validity of DB backup"""

    @classmethod
    def setUpClass(cls):
        """Pull the backup file, and restore the database."""

        backup_folder = pull_latest_backup(BACKUP_BUCKET)
        restore(backup_folder)

    def setUp(self):
        backup_folder = pull_latest_backup(BACKUP_BUCKET)
        self.timestamp = get_timestamp(backup_folder)

        self.local_conn = mysql.connector.connect(
            user=LOCAL_USER,
            host=LOCAL_HOST,
            password=LOCAL_PASSWORD,
            database=DATABASE,
        )
        self.prod_conn = mysql.connector.connect(
            host=PROD_HOST,
            user=PROD_USER,
            password=PROD_PASSWORD,
            database=DATABASE,
        )

    def test_database_exists(self):
        """Validates that the db in the production
        database matches those produced by the restored db"""
        backup_databases = get_results(self.local_conn, 'show databases;')
        prod_databases = get_results(self.prod_conn, 'show databases;')
        self.assertEqual(backup_databases, prod_databases)

    @parameterized.expand(TABLES)
    def test_row_number(self, table):
        """Tests that the count of rows for each table matches"""

        restored_count = get_results(
            self.local_conn,
            f'SELECT COUNT(*) FROM {table};',
        )
        prod_count = get_results(
            self.prod_conn,
            f"SELECT COUNT(*) FROM {table} FOR SYSTEM_TIME AS OF TIMESTAMP'{self.timestamp}'",
        )

        self.assertEqual(restored_count, prod_count)

    @parameterized.expand(TABLES)
    def test_rows_top(self, table):
        """Validates the top 10 rows in each table match"""
        restored_results = get_results(
            self.local_conn,
            f'SELECT * FROM {table} LIMIT 10;',
        )
        prod_results = get_results(
            self.prod_conn,
            f"SELECT * FROM {table} FOR SYSTEM_TIME AS OF TIMESTAMP'{self.timestamp}' LIMIT 10;",
        )
        self.assertEqual(restored_results, prod_results)

    @parameterized.expand(FIELDS)
    def test_random_rows_ids(self, table, id_fields: Tuple[str]):
        """Pulls 10 random rows for testing.
        Operates on tables with a unique id field"""

        id_fields_str = ', '.join(id_fields) + f', {table}.*'

        restored_random_results = get_results(
            self.local_conn,
            f'SELECT {id_fields_str} FROM {table} ORDER BY RAND() LIMIT 10',
        )

        for row in restored_random_results:
            ids = row[: len(id_fields)]
            wheres_str = ' AND '.join(f'{field} = %s' for field in id_fields)

            _query = f"""\
SELECT {id_fields_str}
FROM {table}
FOR SYSTEM_TIME AS OF TIMESTAMP'{self.timestamp}'
WHERE {wheres_str};"""

            prod_random_results = get_results(
                self.prod_conn,
                _query,
                ids,
            )
            self.assertEqual(row, prod_random_results[0])

    @classmethod
    def tearDownClass(cls):
        """Delete test database following testing"""
        subprocess.run(['rm', '-r', LOCAL_BACKUP_FOLDER], check=True)
        subprocess.run(['sudo', 'rm', '-r', '/var/lib/mysql'], check=True)


def get_timestamp(folder: str):
    """Returns timestamp in format YYYY-MM-DD HH:MM:SS"""
    timestamp = f'{folder[13:17]}-{folder[10:12]}-{folder[7:9]} {folder[18:20]}:{folder[21:23]}:{folder[24:26]}'
    return timestamp


def get_results(conn, query: str, values: Optional[Tuple] = None):
    """Returns the results from a provided query at a given connection"""
    cursor = conn.cursor()
    cursor.execute(query, values)
    results = list(cursor)
    return results
