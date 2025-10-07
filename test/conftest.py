"""
Includes pytest specific implementation to create a TestDatabaseContainer (singleton instance)
before running any tests (and removed at the end).
The created TestDatabaseContainer is then treated a shared resource and used in the test classes.
This set-up improve test runtime by reusing the test database container whose creation (time) is expensive.

Adapted from - https://stackoverflow.com/questions/17801300/how-to-run-a-method-before-all-tests-in-all-classes
"""
import pytest

from test.testdb_container import TestDatabaseContainer

@pytest.fixture(scope="session", autouse=True)
def setup_before_all_tests():
    db_container = TestDatabaseContainer()
    db_container.start()
    yield
    db_container.teardown()