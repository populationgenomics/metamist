"""
Includes unittest specific implementation to create a TestDatabaseContainer (singleton instance)
before running any tests (and removed at the end).
The created TestDatabaseContainer is then treated a shared resource and used in the test classes.
This set-up improve test runtime by reusing the test database container whose creation (time) is expensive.

Adapted from -  https://stackoverflow.com/questions/75097271/python-unittest-starttestrun-to-execute-setup-only-once-before-all-tests
"""

import unittest

from test.testdb_container import TestDatabaseContainer


def startTestRun(self):
    db_container = TestDatabaseContainer()
    db_container.start()


def stopTestRun(self):
    db_container = TestDatabaseContainer()
    db_container.teardown()


setattr(unittest.TestResult, 'startTestRun', startTestRun)
setattr(unittest.TestResult, 'stopTestRun', stopTestRun)
