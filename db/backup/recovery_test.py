""" A script to test that tests the validity of a database backup
in the event of recovery.
NOTE: DO NOT RUN THIS SCRIPT ON A PRODUCTION SERVER
At present, it will drop the production database.

"""

import unittest


class TestDatabaseBackup(unittest.TestCase):
    """ Testing validity of DB backup """

    @classmethod
    def setUpCass(cls):  # pylint: disable-msg=C0103
        """ Pull the backup file, and restore the database."""

    def test_database_exists(self):
        """Validates that the db in the production
        database matches those produced by the restored db"""

    def test_row_number(self):
        """ Test that the number of rows in each table is equal """

    def test_rows_top(self):
        """ Validates the top 10 rows in each table match """

    def test_rows_bottom(self):
        """ Validates the bottom 10 rows in each table match """

    def test_random_rows(self):
        """ Pulls 10 random rows for testing """

    @classmethod
    def tearDownClass(cls):
        """ Delete test database following testing """
