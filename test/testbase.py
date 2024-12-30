# pylint: disable=invalid-overridden-method,no-member

import asyncio
import dataclasses
import logging
import os
import socket
import subprocess
import unittest
from functools import wraps
from unittest.mock import patch

import databases.core
import nest_asyncio
from google.auth.credentials import AnonymousCredentials
from google.cloud.storage import Client
from pymysql import IntegrityError
from testcontainers.mysql import MySqlContainer

from api.graphql.loaders import get_context  # type: ignore
from api.graphql.schema import schema  # type: ignore
from db.python.connect import (
    TABLES_ORDERED_BY_FK_DEPS,
    Connection,
    CredentialedDatabaseConfiguration,
    SMConnections,
)
from db.python.tables.project import ProjectPermissionsTable
from models.models.project import Project, ProjectId, ProjectMemberUpdate

TEST_PROJECT_NAME = 'test'

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


loop = asyncio.new_event_loop()


def find_free_port():
    """Find free port to run tests on"""
    s = socket.socket()
    s.bind(('', 0))  # Bind to a free port provided by the host.
    free_port_number = s.getsockname()[1]  # Return the port number assigned.
    s.close()  # free the port so we can immediately use
    return free_port_number


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
    dbs: MySqlContainer | None = None
    connections: dict[str, databases.Database] = {}

    author: str
    project_id: ProjectId
    project_name: str
    project_id_map: dict[ProjectId, Project]
    project_name_map: dict[str, Project]

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
            # Set environment to test
            os.environ['SM_ENVIRONMENT'] = 'test'
            logger = logging.getLogger()
            try:
                db = cls.dbs
                if not cls.dbs:
                    db = MySqlContainer('mariadb:11.2.2', password='test')

                    port_to_expose = find_free_port()

                    # override the default port to map the container to
                    db.with_bind_ports(db.port, port_to_expose)
                    logger.disabled = True
                    db.start()
                    logger.disabled = False
                    cls.dbs = db

                db_prefix = 'db'
                if am_i_in_test_environment:
                    db_prefix = '../db'

                if not db:
                    raise ValueError('No database container found')

                # create the database
                db_name = str(cls.__name__) + 'Db'

                _root_connection = SMConnections.make_connection(
                    CredentialedDatabaseConfiguration(
                        host=db.get_container_host_ip(),
                        port=str(port_to_expose),
                        username='root',
                        password=db.password,
                        dbname=db.dbname,
                    ),
                )

                # create the database for each test class, and give permissions
                await _root_connection.connect()
                await _root_connection.execute(f'CREATE DATABASE {db_name};')
                await _root_connection.execute(
                    f"GRANT ALL PRIVILEGES ON `{db_name}`.* TO {db.username}@'%';"
                )
                await _root_connection.execute('FLUSH PRIVILEGES;')
                await _root_connection.disconnect()

                # mfranklin -> future dancoates: if you work out how to copy the
                #       database instead of running liquibase, that would be great
                lcon_string = f'jdbc:mariadb://{db.get_container_host_ip()}:{port_to_expose}/{db_name}'
                # apply the liquibase schema
                command = [
                    'liquibase',
                    *('--changeLogFile', db_prefix + '/project.xml'),
                    *('--defaultsFile', db_prefix + '/liquibase.properties'),
                    *('--url', lcon_string),
                    *('--driver', 'org.mariadb.jdbc.Driver'),
                    *(
                        '--classpath',
                        db_prefix + '/mariadb-java-client-3.0.3.jar',
                    ),
                    *('--username', db.username),
                    *('--password', db.password),
                    'update',
                ]
                subprocess.check_output(command, stderr=subprocess.STDOUT)

                cls.author = 'testuser'

                sm_db = SMConnections.make_connection(
                    CredentialedDatabaseConfiguration(
                        host=db.get_container_host_ip(),
                        port=str(port_to_expose),
                        username='root',
                        password=db.password,
                        dbname=db_name,
                    )
                )
                await sm_db.connect()

                cls.connections[cls.__name__] = sm_db
                formed_connection = Connection(
                    connection=sm_db,
                    author=cls.author,
                    project_id_map={},
                    project_name_map={},
                    on_behalf_of=None,
                    ar_guid=None,
                    project=None,
                )

                # Add the test user to the admin groups
                await formed_connection.connection.execute(
                    f"""
                    INSERT INTO group_member(group_id, member)
                    SELECT id, '{cls.author}'
                    FROM `group` WHERE name IN('project-creators', 'members-admin')
                """
                )

                ppt = ProjectPermissionsTable(
                    connection=formed_connection,
                )
                cls.project_name = TEST_PROJECT_NAME
                cls.project_id = await ppt.create_project(
                    project_name=cls.project_name,
                    dataset_name=cls.project_name,
                    author=cls.author,
                )

                _, initial_project_name_map = await ppt.get_projects_accessible_by_user(
                    user=cls.author
                )
                created_project = initial_project_name_map.get(cls.project_name)
                assert created_project

                await ppt.set_project_members(
                    project=created_project,
                    members=[
                        ProjectMemberUpdate(
                            member=cls.author, roles=['reader', 'writer']
                        )
                    ],
                )

                # Get the new project map now that project membership is updated
                (
                    project_id_map,
                    project_name_map,
                ) = await ppt.get_projects_accessible_by_user(user=cls.author)

                cls.project_id_map = project_id_map
                cls.project_name_map = project_name_map

                # formed_connection.project = cls.project_id

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
        # remove from active_tests
        @run_as_sync
        async def tearDown():
            connection = cls.connections.pop(cls.__name__, None)
            if connection:
                await connection.disconnect()

            if len(cls.connections) == 0 and cls.dbs:
                cls.dbs.stop()

        tearDown()

    def setUp(self) -> None:
        self._connection = self.connections[self.__class__.__name__]
        # create a connection on each test so we can generate a new
        # audit_log ID for each test
        self.connection = Connection(
            connection=self._connection,
            project=self.project_id_map.get(self.project_id),
            author=self.author,
            project_id_map=self.project_id_map,
            project_name_map=self.project_name_map,
            ar_guid=None,
            on_behalf_of=None,
        )

        # Patch get_gcs_client for all tests
        patcher = patch(
            'models.models.output_file.get_gcs_client',
            self.custom_get_gcs_client,
        )
        self.addCleanup(patcher.stop)
        self.mock_get_gcs_client = patcher.start()

    def custom_get_gcs_client(self):
        """Create the custom client instance with the desired configuration"""
        return Client(
            credentials=AnonymousCredentials(),
            project='test',
            client_options={'api_endpoint': 'http://localhost:4443'},
        )

    @run_as_sync
    async def run_graphql_query(self, query, variables=None):
        """Run SYNC graphql query on internal database"""
        return await self.run_graphql_query_async(query, variables=variables)

    async def run_graphql_query_async(self, query, variables=None) -> dict:
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
                request=None,  # pylint: disable
            ),
        )
        if value.errors:
            raise value.errors[0]

        if value.data is None:
            raise ValueError(f'No data returned from query: {query}')

        return value.data

    async def audit_log_id(self):
        """Get audit_log_id for the test"""
        return await self.connection.audit_log_id()

    def assertDataclassEqual(self, a, b):
        """Assert two dataclasses are equal"""

        def to_dict(obj):
            d = dataclasses.asdict(obj)
            for k, v in d.items():
                if dataclasses.is_dataclass(v):
                    d[k] = to_dict(v)
            return d

        self.maxDiff = None
        self.assertDictEqual(to_dict(a), to_dict(b))


class DbIsolatedTest(DbTest):
    """
    Database integration tests that performs clean-up at the start of each test
    """

    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()
        # make sure it's clear, for whatever reason
        await self.clear_database()

    @run_as_sync
    async def tearDown(self) -> None:
        super().tearDown()
        # tear down on end to test any issues with deleting THAT test data
        await self.clear_database()

    async def clear_database(self):
        """Clear the database of all data, except for project + group tables"""
        ignore = {
            'DATABASECHANGELOG',
            'DATABASECHANGELOGLOCK',
            'project',
            'group',
            'project_member',
        }
        for table in TABLES_ORDERED_BY_FK_DEPS:
            if table in ignore:
                continue
            try:
                # mfranklin: Can't use truncate, despite what the docs say
                #   docs: https://mariadb.com/kb/en/truncate-table/
                #   error: System-versioned tables do not support TRUNCATE TABLE'
                #   ticket: https://jira.mariadb.org/browse/MDEV-28439
                #
                # so disable FK checks earlier to more easily delete all rows
                await self.connection.connection.execute(
                    f"""
                    SET GLOBAL FOREIGN_KEY_CHECKS = 0;
                    DELETE FROM `{table}` WHERE 1;
                    SET GLOBAL FOREIGN_KEY_CHECKS = 1;
                    """
                )
                await self.connection.connection.execute(
                    f'DELETE HISTORY FROM `{table}`'
                )
            except IntegrityError as e:
                raise IntegrityError(f'Could not delete {table}') from e
