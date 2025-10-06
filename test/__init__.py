import unittest

from test.testDbContainer import TestDatabaseContainer


def startTestRun(self):
    db_container = TestDatabaseContainer()
    db_container.start()


def stopTestRun(self):
    db_container = TestDatabaseContainer()
    db_container.teardown()


setattr(unittest.TestResult, 'startTestRun', startTestRun)
setattr(unittest.TestResult, 'stopTestRun', stopTestRun)
