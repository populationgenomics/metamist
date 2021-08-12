"""
Code for connecting to Postgres database
"""
import json
import logging
import os
from typing import Optional

# import asyncio
import databases

from db.python.tables.project import ProjectTable

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


class ProjectDoesNotExist(Exception):
    """Custom error for ProjectDoesNotExist"""

    def __init__(self, project_id, *args: object) -> None:
        super().__init__(
            f'Project with id "{project_id}" does not exist, '
            'or you do not have the appropriate permissions',
            *args,
        )


class Forbidden(Exception):
    """Not allowed access to a project (or not allowed project-less access)"""


class NotFoundError(Exception):
    """Custom error when you can't find something"""


class DatabaseConfiguration:
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
    def dev_config() -> 'DatabaseConfiguration':
        """Dev config for local database with name 'sm_dev'"""
        # consider pulling from env variables
        return DatabaseConfiguration(
            dbname=os.environ.get('SM_DEV_DB_NAME', 'sm_dev'),
            username=os.environ.get('SM_DEV_DB_USER', 'root'),
            password=os.environ.get('SM_DEV_DB_PASSWORD', ''),
            host=os.environ.get('SM_DEV_DB_HOST', '127.0.0.1'),
            port=os.environ.get('SM_DEV_DB_PORT', '3306'),
        )


class SMConnections:
    """Contains useful functions for connecting to the database"""

    # _connected = False
    # _connections: Dict[str, databases.Database] = {}
    # _admin_db: databases.Database = None

    _credentials: DatabaseConfiguration = None

    @staticmethod
    def _get_config():
        if SMConnections._credentials:
            return SMConnections._credentials

        config = DatabaseConfiguration.dev_config()
        if os.getenv('SM_ENVIRONMENT') == 'PRODUCTION':
            logger.info('Using production mysql configurations')
            config = SMConnections._load_database_configurations_from_secret_manager()

        SMConnections._credentials = config

        return SMConnections._credentials

    # @staticmethod
    # def _get_connections(force_reconnect=False):
    #     if not SMConnections._connections or force_reconnect:
    #
    #
    #         # SMConnections._admin_db = SMConnections.make_connection(admin_config)
    #
    #         try:
    #             SMConnections._connections = {
    #                 config.project: SMConnections.make_connection(config)
    #                 for config in project_configs
    #             }
    #             logger.info('Created connections to databases')
    #         except Exception as exp:
    #             logger.error(f"Couldn't create mysql connection: {exp}")
    #             raise exp
    #
    #     return SMConnections._connections

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
        # min_pool_size=5,
        # max_pool_size=20,
    ):
        """Prepares the connection string for mysql / mariadb"""

        _host = host or 'localhost'
        u_p = username

        if password:
            u_p += f':{password}'
        if port:
            _host += f':{port}'

        options = {}  # {'min_size': min_pool_size, 'max_size': max_pool_size}
        _options = '&'.join(f'{k}={v}' for k, v in options.items())

        url = f'mysql://{u_p}@{_host}/{database}?{_options}'

        return url

    @staticmethod
    async def connect():
        """Create connections to database"""

        # if SMConnections._connected:
        #     return False
        #
        # connections = SMConnections._get_connections()
        # await SMConnections._admin_db.connect()
        # for dbname, db in connections.items():
        #     logger.info(f'Connecting to {dbname}')
        #     await db.connect()
        #
        # return True

        # this will now not connect, new connection will be made on every request
        return False

    @staticmethod
    async def disconnect():
        """Disconnect from database"""
        # if not SMConnections._connected:
        #     return False
        #
        # await asyncio.gather(
        #     [v.disconnect() for v in SMConnections._get_connections().values()]
        # )
        # return True

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
    async def get_connection(*, author: str, project_name: Optional[str]):
        """Get a db connection from a project and user"""
        # maybe it makes sense to perform permission checks here too
        logger.debug(f'Authenticate connection to {project_name} with "{author}"')

        conn = await SMConnections._get_made_connection()
        pt = ProjectTable(connection=conn)

        project_id = await pt.get_project_id_from_name_and_user(
            user=author, project=project_name
        )
        if project_id is False:
            raise ProjectDoesNotExist(project_name)

        return Connection(connection=conn, author=author, project=project_id)

    @staticmethod
    async def get_connection_no_project(author: str):
        """Get a db connection from a project and user"""
        # maybe it makes sense to perform permission checks here too
        logger.debug(f'Authenticate no-project connection with "{author}"')

        conn = await SMConnections._get_made_connection()
        pt = ProjectTable(connection=conn)

        has_access = pt.get_project_id_from_name_and_user(user=author, project=None)
        if not has_access:
            raise Forbidden

        return Connection(connection=conn, author=author, project=None)

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
    def _load_database_configurations_from_secret_manager() -> DatabaseConfiguration:
        config_dict = json.loads(SMConnections._read_secret('databases'))
        config = DatabaseConfiguration(**config_dict)
        return config


class DbBase:
    """Base class for table subclasses"""

    @classmethod
    async def from_project(cls, project, author):
        """Create the Db object from a project with user details"""
        return cls(
            connection=await SMConnections.get_connection(
                project_name=project, author=author
            ),
        )

    def __init__(self, connection: Connection):
        if connection is None:
            raise Exception(
                f'No connection was provided to the table "{self.__class__.__name__}"'
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
