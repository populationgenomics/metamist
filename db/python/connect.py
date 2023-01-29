# pylint: disable=unused-import
#
"""
Code for connecting to Postgres database
"""
import abc
import json
import logging
import os
from typing import Optional

import databases

from db.python.tables.project import ProjectPermissionsTable

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

TABLES_ORDERED_BY_FK_DEPS = [
    'project',
    'analysis',
    'participant',
    'sample',
    'analysis_sample',
    'sample_sequencing',
    'sample_sequencing_eid',
    'family',
    'family_participant',
    'participant_phenotypes',
][::-1]


class Connection:
    """Stores a DB connection, project and author"""

    def __init__(
        self,
        connection: databases.Database,
        project: Optional[int],
        author: str,
    ):
        self.connection: databases.Database = connection
        self.project: Optional[int] = project
        self.author: str = author

    def assert_requires_project(self):
        """Assert the project is set, or return an exception"""
        if self.project is None:
            raise Exception(
                'An internal error has occurred when passing the project context, '
                'please send this stacktrace to your system administrator'
            )


class NotFoundError(Exception):
    """Custom error when you can't find something"""


class DatabaseConfiguration(abc.ABC):
    """Base class for DatabaseConfiguration"""

    @abc.abstractmethod
    def get_connection_string(self):
        """Get connection string"""
        raise NotImplementedError


class ConnectionStringDatabaseConfiguration(DatabaseConfiguration):
    """Database Configuration that takes a literal DatabaseConfiguration"""

    def __init__(self, connection_string):
        self.connection_string = connection_string

    def get_connection_string(self):
        return self.connection_string


class CredentialedDatabaseConfiguration(DatabaseConfiguration):
    """Class to hold information about a MySqlConfiguration"""

    def __init__(
        self,
        dbname,
        host=None,
        port=None,
        username=None,
        password=None,
    ):
        self.dbname = dbname
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    @staticmethod
    def dev_config() -> 'CredentialedDatabaseConfiguration':
        """Dev config for local database with name 'sm_dev'"""
        # consider pulling from env variables
        return CredentialedDatabaseConfiguration(
            dbname=os.environ.get('SM_DEV_DB_NAME', 'sm_dev'),
            username=os.environ.get('SM_DEV_DB_USER', 'root'),
            password=os.environ.get('SM_DEV_DB_PASSWORD', ''),
            host=os.environ.get('SM_DEV_DB_HOST', '127.0.0.1'),
            port=os.environ.get('SM_DEV_DB_PORT', '3306'),
        )

    def get_connection_string(self):
        """Prepares the connection string for mysql / mariadb"""

        _host = self.host or 'localhost'
        u_p = self.username

        if self.password:
            u_p += f':{self.password}'
        if self.port:
            _host += f':{self.port}'

        options = {}  # {'min_size': self.min_pool_size, 'max_size': self.max_pool_size}
        _options = '&'.join(f'{k}={v}' for k, v in options.items())

        url = f'mysql://{u_p}@{_host}/{self.dbname}?{_options}'

        return url


class SMConnections:
    """Contains useful functions for connecting to the database"""

    # _connected = False
    # _connections: Dict[str, databases.Database] = {}
    # _admin_db: databases.Database = None

    _credentials: Optional[DatabaseConfiguration] = None

    @staticmethod
    def _get_config():
        if SMConnections._credentials:
            return SMConnections._credentials

        config = CredentialedDatabaseConfiguration.dev_config()
        creds_from_env = os.getenv('SM_DBCREDS')
        if creds_from_env is not None:
            config = CredentialedDatabaseConfiguration(**json.loads(creds_from_env))
            logger.info(f'Using supplied SM DB CREDS: {config.host}')

        SMConnections._credentials = config

        return SMConnections._credentials

    @staticmethod
    def make_connection(config: DatabaseConfiguration):
        """Create connection from dbname"""
        # the connection string will prepare pooling automatically
        return databases.Database(config.get_connection_string())

    @staticmethod
    async def connect():
        """Create connections to database"""

        # this will now not connect, new connection will be made on every request
        return False

    @staticmethod
    async def disconnect():
        """Disconnect from database"""

        return False

    @staticmethod
    async def _get_made_connection():
        credentials = SMConnections._get_config()

        if credentials is None:
            raise Exception(
                'The server has been misconfigured, please '
                'contact your system administrator'
            )

        conn = SMConnections.make_connection(credentials)
        await conn.connect()
        return conn

    @staticmethod
    async def get_connection(*, author: str, project_name: str, readonly: bool):
        """Get a db connection from a project and user"""
        # maybe it makes sense to perform permission checks here too
        logger.debug(f'Authenticate connection to {project_name} with {author!r}')

        conn = await SMConnections._get_made_connection()
        pt = ProjectPermissionsTable(connection=conn)

        project_id = await pt.get_project_id_from_name_and_user(
            user=author, project_name=project_name, readonly=readonly
        )

        return Connection(connection=conn, author=author, project=project_id)

    @staticmethod
    async def get_connection_no_project(author: str):
        """Get a db connection from a project and user"""
        # maybe it makes sense to perform permission checks here too
        logger.debug(f'Authenticate no-project connection with {author!r}')

        conn = await SMConnections._get_made_connection()

        # we don't authenticate project-less connection, but rely on the
        # the endpoint to validate the resources

        return Connection(connection=conn, author=author, project=None)


class DbBase:
    """Base class for table subclasses"""

    @classmethod
    async def from_project(cls, project, author, readonly: bool):
        """Create the Db object from a project with user details"""
        return cls(
            connection=await SMConnections.get_connection(
                project_name=project, author=author, readonly=readonly
            ),
        )

    def __init__(self, connection: Connection):
        if connection is None:
            raise Exception(
                f'No connection was provided to the table {self.__class__.__name__!r}'
            )
        if not isinstance(connection, Connection):
            raise Exception(
                f'Expected connection type Connection, received {type(connection)}, '
                f'did you mean to call self._connection?'
            )

        self._connection = connection
        self.connection: databases.Database = connection.connection
        self.author = connection.author
        self.project = connection.project

        if self.author is None:
            raise Exception(f'Must provide author to {self.__class__.__name__}')

    # piped from the connection

    @staticmethod
    def escape_like_term(query: str):
        """
        Escape meaningful keys when using LIKE with a user supplied input
        """
        return query.replace('%', '\\%').replace('_', '\\_')
