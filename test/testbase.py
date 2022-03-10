import asyncio
import os
import socket
import subprocess
import unittest
from functools import wraps

from typing import Dict

from testcontainers.mysql import MariaDbContainer

from db.python.connect import (
    ConnectionStringDatabaseConfiguration,
    Connection,
    SMConnections,
)
from db.python.tables.project import ProjectPermissionsTable

am_i_in_test_environment = os.getcwd().endswith('test')


def find_free_port():
    """Find free port to run tests on"""
    s = socket.socket()
    s.bind(('', 0))  # Bind to a free port provided by the host.
    return s.getsockname()[1]  # Return the port number assigned.


loop = asyncio.new_event_loop()


def run_as_sync(f):
    """
    Run an async function, synchronously.
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        return loop.run_until_complete(f(*args, **kwargs))

    return wrapper


class DbTest(unittest.TestCase):
    """Base class for database integration tests"""

    # store connections here, so they can be created PER-CLASS
    # and don't get reused.
    dbs: Dict[str, MariaDbContainer] = {}
    connections: Dict[str, Connection] = {}

    @classmethod
    def setUpClass(cls) -> None:
        @run_as_sync
        async def setup():
            """
            This starts a mariadb container, applies liquibase schema and inserts a project
            MAYBE, the best way in the future would be to only ever create ONE connection
            between all Test classes, and create a new database per test, new set of a connections

            Then on tearDownClass, only tear down the instance once all tests have been completed.
            """
            try:
                db = MariaDbContainer('mariadb:latest')
                port_to_expose = find_free_port()
                db.with_bind_ports(db.port_to_expose, port_to_expose)
                db.start()
                cls.dbs[cls.__name__] = db

                db_prefix = 'db'
                if am_i_in_test_environment:
                    db_prefix = '../db'
                    # db_prefix = os.path.join(*os.getcwd().split('/')[:-1], 'db')

                con_string = db.get_connection_url()
                con_string = 'mysql://' + con_string.split('://', maxsplit=1)[1]
                lcon_string = f'jdbc:mariadb://{db.get_container_host_ip()}:{port_to_expose}/{db.MYSQL_DATABASE}'
                command = [
                    'liquibase',
                    *('--changeLogFile', db_prefix + '/project.xml'),
                    *('--defaultsFile', db_prefix + '/liquibase.properties'),
                    *('--url', lcon_string),
                    *('--driver', 'org.mariadb.jdbc.Driver'),
                    *('--classpath', db_prefix + '/mariadb-java-client-2.7.2.jar'),
                    *('--username', db.MYSQL_USER),
                    *('--password', db.MYSQL_PASSWORD),
                    'update',
                ]
                subprocess.check_output(command)

                sm_db = SMConnections.make_connection(
                    ConnectionStringDatabaseConfiguration(con_string)
                )
                await sm_db.connect()
                connection = Connection(
                    connection=sm_db,
                    project=0,
                    author='testuser',
                )
                cls.connections[cls.__name__] = connection

                ppt = ProjectPermissionsTable(connection.connection)
                await ppt.create_project(
                    project_name='test',
                    dataset_name='test',
                    create_test_project=False,
                    gcp_project_id='None',
                    author='testuser',
                    read_secret_name='None',
                    write_secret_name='None',
                    check_permissions=False,
                )

            except subprocess.CalledProcessError as e:
                print(e)
                if e.stderr:
                    print(e.stderr.decode().replace('\\n', '\n'))
                if e.stdout:
                    print(e.stdout.decode().replace('\\n', '\n'))
                cls.tearDownClass()
                raise e
            except Exception as e:
                print(f'FAILED WITH {e}')
                print(e)
                cls.tearDownClass()

                raise

        return setup()

    @classmethod
    def tearDownClass(cls) -> None:
        db = cls.dbs[cls.__name__]
        db.exec(f'DROP DATABASE {db.MYSQL_DATABASE};')
        db.stop()

    def setUp(self) -> None:
        self.connection = self.connections[self.__class__.__name__]
