"""
Code for connecting to Postgres database
"""
import os
import json
import logging
from typing import Dict, List

import asyncio
import databases


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def to_db_json(val):
    """Convert val to json for DB"""
    # return psycopg2.extras.Json(val)
    return json.dumps(val)


class Connection:
    """Stores a DB connection, project and author"""

    def __init__(
        self,
        connection: databases.Database,
        project: str,
        author: str,
    ):
        self.connection: databases.Database = connection
        self.project: str = project
        self.author: str = author


class ProjectDoesNotExist(Exception):
    """Custom error for ProjectDoesNotExist"""

    def __init__(self, project_id, *args: object) -> None:
        super().__init__(
            f'Project with id "{project_id}" does not exist, '
            'or you do not have the appropriate permissions',
            *args,
        )


class NotFoundError(Exception):
    """Custom error when you can't find something"""


class DatabaseConfiguration:
    """Class to hold information about a MySqlConfiguration"""

    def __init__(
        self,
        project,
        dbname,
        host=None,
        port=None,
        username=None,
        password=None,
        is_admin=False,
    ):
        self.project = project
        self.dbname = dbname
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.is_admin = is_admin

    @staticmethod
    def dev_config():
        """Dev config for local database with name 'sm_dev'"""
        # consider pulling from env variables
        return [
            DatabaseConfiguration(
                project='dev',
                dbname='sm_dev',
                username='root',
                password='',
            ),
            DatabaseConfiguration(
                project='dev2',
                dbname='sm_dev2',
                username='root',
                password='',
            ),
            DatabaseConfiguration(
                project=None,
                dbname='sm_admin',
                username='root',
                password='',
                is_admin=True,
            ),
        ]


class SMConnections:
    """Contains useful functions for connecting to the database"""

    _connected = False
    _connections: Dict[str, databases.Database] = {}
    _admin_db: databases.Database = None

    @staticmethod
    def get_admin_db():
        """Get administrator database, most likely for getting sample-map"""
        return SMConnections._admin_db

    @staticmethod
    def _get_connections(force_reconnect=False):
        if not SMConnections._connections or force_reconnect:
            configs = DatabaseConfiguration.dev_config()
            if os.getenv('SM_ENVIRONMENT') == 'PRODUCTION':
                logger.info('Using production mysql configurations')
                configs = (
                    SMConnections._load_database_configurations_from_secret_manager()
                )

            project_configs = [c for c in configs if not c.is_admin]
            admin_config = [c for c in configs if c.is_admin][0]
            SMConnections._admin_db = SMConnections.make_connection(admin_config)

            try:
                SMConnections._connections = {
                    config.project: SMConnections.make_connection(config)
                    for config in project_configs
                }
                logger.info('Created connections to databases')
            except Exception as exp:
                logger.error(f"Couldn't create mysql connection: {exp}")
                raise exp

        return SMConnections._connections

    @staticmethod
    def make_connection(config: DatabaseConfiguration):
        """Create connection from dbname"""

        # the connection string will prepare pooling automatically
        return databases.Database(
            SMConnections.prepare_connection_string(
                host=config.host,
                database=config.dbname,
                username=config.username,
                password=config.password,
                port=config.port,
            )
        )

    @staticmethod
    def prepare_connection_string(
        host,
        database,
        username,
        password=None,
        port=None,
        min_pool_size=5,
        max_pool_size=20,
    ):
        """Prepares the connection string for mysql / mariadb"""

        _host = host or 'localhost'
        u_p = username

        if password:
            u_p += f':{password}'
        if port:
            _host += f':{host}'

        options = {'min_size': min_pool_size, 'max_size': max_pool_size}
        _options = '&'.join(f'{k}={v}' for k, v in options.items())

        url = f'mysql://{u_p}@{_host}/{database}?{_options}'

        return url

    @staticmethod
    async def connect():
        """Create connections to database"""
        if SMConnections._connected:
            return False

        connections = SMConnections._get_connections()
        await SMConnections._admin_db.connect()
        for dbname, db in connections.items():
            logger.info(f'Connecting to {dbname}')
            await db.connect()

        return True

    @staticmethod
    async def disconnect():
        """Disconnect from database"""
        if not SMConnections._connected:
            return False

        await asyncio.gather(
            [v.disconnect() for v in SMConnections._get_connections().values()]
        )
        return True

    @staticmethod
    def get_connection_for_project(project, author):
        """Get a db connection from a project and user"""
        # maybe it makes sense to perform permission checks here too
        logger.debug(f'Authenticate the connection with "{author}"')

        conn = SMConnections._get_connections().get(project)

        if conn is None:
            raise ProjectDoesNotExist(project)

        return Connection(connection=conn, author=author, project=project)

    @staticmethod
    def _read_secret(name: str) -> str:
        """Reads the latest version of the given secret from Google's Secret Manager."""
        # pylint: disable=import-outside-toplevel,no-name-in-module
        from google.cloud import secretmanager

        secret_manager = secretmanager.SecretManagerServiceClient()

        secret_name = f'projects/sample-metadata/secrets/{name}/versions/latest'
        response = secret_manager.access_secret_version(request={'name': secret_name})
        return response.payload.data.decode('UTF-8')

    @staticmethod
    def _load_database_configurations_from_secret_manager() -> List[
        DatabaseConfiguration
    ]:
        configs_dicts = json.loads(SMConnections._read_secret('databases'))
        configs = [DatabaseConfiguration(**config) for config in configs_dicts]
        return configs


class DbBase:
    """Base class for table subclasses"""

    @classmethod
    def from_project(cls, project, author):
        """Create the Db object from a project with user details"""
        return cls(
            connection=SMConnections.get_connection_for_project(project, author),
        )

    def __init__(self, connection: Connection):
        if connection is None:
            raise Exception(
                f'No connection was provided to the table "{self.__class__.__name__}"'
            )
        if not isinstance(connection, Connection):
            raise Exception(
                f'Expected connection type Connection, received {type(connection)}, did you mean to call self._connection?'
            )

        self._connection = connection
        self.connection: databases.Database = connection.connection
        self.author = connection.author
        self.project = connection.project

        if self.author is None:
            raise Exception('Must provide author to {self.__class__.__name__}')

    # piped from the connection
