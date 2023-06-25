# pylint: disable=invalid-overridden-method

import asyncio
import logging
import os
import socket
import subprocess
import unittest
from functools import wraps

from typing import Dict

import nest_asyncio
from pymysql import IntegrityError
from testcontainers.mysql import MySqlContainer

from api.graphql.loaders import get_context  # type: ignore
from api.graphql.schema import schema  # type: ignore
from api.settings import set_all_access
from db.python.connect import (
    ConnectionStringDatabaseConfiguration,
    Connection,
    SMConnections,
    TABLES_ORDERED_BY_FK_DEPS,
)
from db.python.tables.project import ProjectPermissionsTable

# use this to determine where the db directory is relatively,
# as pycharm runs in "test/" folder, and GH runs them in git root
am_i_in_test_environment = os.getcwd().endswith('test')

# This monkey patches the asyncio event loop and allows it to be re-entrant
# (you can call run_until_complete while run_until_complete is already on the stack)
# Source: https://stackoverflow.com/a/56434301
nest_asyncio.apply()


for lname in (
    'asyncio',
    'urllib3',
    'docker',
    'databases',
    'testcontainers.core.container',
    'testcontainers.core.waiting_utils',
):
    logging.getLogger(lname).setLevel(logging.WARNING)


def find_free_port():
    """Find free port to run tests on"""
    s = socket.socket()
    s.bind(('', 0))  # Bind to a free port provided by the host.
    return s.getsockname()[1]  # Return the port number assigned.


loop = asyncio.new_event_loop()


def run_as_sync(f):
    """
    Decorate your async function to run is synchronously
    This runs on the testing event loop
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        return loop.run_until_complete(f(*args, **kwargs))

    return wrapper


class DbTest(unittest.TestCase):
    """Base class for database integration tests"""

    # store connections here, so they can be created PER-CLASS
    # and don't get recreated per test.
    dbs: Dict[str, MySqlContainer] = {}
    connections: Dict[str, Connection] = {}

    @classmethod
    def setUpClass(cls) -> None:
        @run_as_sync
        async def setup():
            """
            This starts a mariadb container, applies liquibase schema and inserts a project
            MAYBE, the best way in the future would be to only ever create ONE connection
            between all Test classes, and create a new database per test, new set of a connections,
            but that certainly has a host of its own problems.

            Then you can destroy the database within tearDownClass as all tests have been completed.
            """
            logger = logging.getLogger()
            try:
                set_all_access(True)
                db = MySqlContainer('mariadb:10.8.3')
                port_to_expose = find_free_port()
                # override the default port to map the container to
                db.with_bind_ports(db.port_to_expose, port_to_expose)
                logger.disabled = True
                db.start()
                logger.disabled = False
                cls.dbs[cls.__name__] = db

                db_prefix = 'db'
                if am_i_in_test_environment:
                    db_prefix = '../db'

                con_string = db.get_connection_url()
                con_string = 'mysql://' + con_string.split('://', maxsplit=1)[1]
                lcon_string = f'jdbc:mariadb://{db.get_container_host_ip()}:{port_to_expose}/{db.MYSQL_DATABASE}'
                # apply the liquibase schema
                command = [
                    'liquibase',
                    *('--changeLogFile', db_prefix + '/project.xml'),
                    *('--defaultsFile', db_prefix + '/liquibase.properties'),
                    *('--url', lcon_string),
                    *('--driver', 'org.mariadb.jdbc.Driver'),
                    *('--classpath', db_prefix + '/mariadb-java-client-3.0.3.jar'),
                    *('--username', db.MYSQL_USER),
                    *('--password', db.MYSQL_PASSWORD),
                    'update',
                ]
                subprocess.check_output(command, stderr=subprocess.STDOUT)

                sm_db = SMConnections.make_connection(
                    ConnectionStringDatabaseConfiguration(con_string)
                )
                await sm_db.connect()
                connection = Connection(
                    connection=sm_db,
                    project=1,
                    author='testuser',
                )
                cls.connections[cls.__name__] = connection

                ppt = ProjectPermissionsTable(
                    connection.connection, allow_full_access=True
                )
                cls.project_name = 'test'
                cls.project_id = await ppt.create_project(
                    project_name=cls.project_name,
                    dataset_name=cls.project_name,
                    create_test_project=False,
                    author='testuser',
                    read_group_name='None',
                    write_group_name='None',
                    check_permissions=False,
                )

            except subprocess.CalledProcessError as e:
                logging.exception(e)
                if e.stderr:
                    logging.error(e.stderr.decode().replace('\\n', '\n'))
                if e.stdout:
                    logging.error(e.stdout.decode().replace('\\n', '\n'))
                cls.tearDownClass()
                raise e
            except Exception as e:
                logging.error(f'FAILED WITH {e}')
                logging.exception(e)
                cls.tearDownClass()

                raise

        return setup()

    @classmethod
    def tearDownClass(cls) -> None:
        db = cls.dbs.get(cls.__name__)
        if db:
            db.exec(f'DROP DATABASE {db.MYSQL_DATABASE};')
            db.stop()

    def setUp(self) -> None:
        self.project_id = 1
        self.project_name = 'test'
        self.connection = self.connections[self.__class__.__name__]

    @run_as_sync
    async def run_graphql_query(self, query, variables=None):
        """Run SYNC graphql query on internal database"""
        return await self.run_graphql_query_async(query, variables=variables)

    async def run_graphql_query_async(self, query, variables=None):
        """Run ASYNC graphql query on internal database"""
        if not isinstance(query, str):
            # if the query was wrapped in a gql() call, unwrap it
            try:
                query = query.loc.source.body
            except KeyError as exc:
                # it obviously wasn't of type document
                raise ValueError(
                    f'Invalid test query (type: {type(query)}): {query}'
                ) from exc

        value = await schema.execute(
            query,
            variable_values=variables,
            context_value=await get_context(
                connection=self.connection,
                request=None,   # pylint: disable
            ),
        )
        if value.errors:
            raise value.errors[0]
        return value.data


class DbIsolatedTest(DbTest):
    """
    Database integration tests that performs clean-up at the start of each test
    """

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()

        ignore = {'DATABASECHANGELOG', 'DATABASECHANGELOGLOCK', 'project'}
        for table in TABLES_ORDERED_BY_FK_DEPS:
            if table in ignore:
                continue
            try:
                await self.connection.connection.execute(
                    f'DELETE FROM {table} WHERE 1;'
                )
                await self.connection.connection.execute(f'DELETE HISTORY FROM {table}')
            except IntegrityError as e:
                raise IntegrityError(f'Could not delete {table}') from e
