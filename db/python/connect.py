# pylint: disable=unused-import,too-many-instance-attributes
# flake8: noqa
"""
Code for connecting to Postgres database
"""
import abc
import asyncio
import json
import logging
import os

import databases

from api.settings import LOG_DATABASE_QUERIES
from db.python.utils import InternalError

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

TABLES_ORDERED_BY_FK_DEPS = [
    'project',
    'group',
    'analysis',
    'participant',
    'sample',
    'sequencing_group',
    'assay',
    'sequencing_group_assay',
    'analysis_sequencing_group',
    'analysis_sample',
    'assay_external_id',
    'sequencing_group_external_id',
    'family',
    'family_participant',
    'participant_phenotypes',
    'group_member',
    'cohort_template',
    'cohort',
    'cohort_sequencing_group',
    'analysis_cohort',
    'analysis_runner',
][::-1]


class Connection:
    """Stores a DB connection, project and author"""

    def __init__(
        self,
        connection: databases.Database,
        project: int | None,
        author: str,
        on_behalf_of: str | None,
        readonly: bool,
        ar_guid: str | None,
        meta: dict[str, str] | None = None,
    ):
        self.connection: databases.Database = connection
        self.project: int | None = project
        self.author: str = author
        self.on_behalf_of: str | None = on_behalf_of
        self.readonly: bool = readonly
        self.ar_guid: str | None = ar_guid
        self.meta = meta

        self._audit_log_id: int | None = None
        self._audit_log_lock = asyncio.Lock()

    async def audit_log_id(self):
        """Get audit_log ID for write operations, cached per connection"""
        if self.readonly:
            raise InternalError(
                'Trying to get a audit_log ID, but not a write connection'
            )

        async with self._audit_log_lock:
            if not self._audit_log_id:

                # make this import here, otherwise we'd have a circular import
                from db.python.tables.audit_log import (  # pylint: disable=import-outside-toplevel,R0401
                    AuditLogTable,
                )

                at = AuditLogTable(self)
                self._audit_log_id = await at.create_audit_log(
                    author=self.author,
                    on_behalf_of=self.on_behalf_of,
                    ar_guid=self.ar_guid,
                    comment=None,
                    project=self.project,
                    meta=self.meta,
                )

        return self._audit_log_id

    def assert_requires_project(self):
        """Assert the project is set, or return an exception"""
        if self.project is None:
            raise InternalError(
                'An internal error has occurred when passing the project context, '
                'please send this stacktrace to your system administrator'
            )


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

        options: dict[str, str | int] = (
            {}
        )  # {'min_size': self.min_pool_size, 'max_size': self.max_pool_size}
        _options = '&'.join(f'{k}={v}' for k, v in options.items())

        url = f'mysql://{u_p}@{_host}/{self.dbname}?{_options}'

        return url


class SMConnections:
    """Contains useful functions for connecting to the database"""

    _credentials: DatabaseConfiguration | None = None

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
        return databases.Database(
            config.get_connection_string(), echo=LOG_DATABASE_QUERIES
        )

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
    async def get_made_connection():
        """
        Makes a new connection to the database on each call
        """
        credentials = SMConnections._get_config()

        if credentials is None:
            raise InternalError(
                'The server has been misconfigured, please '
                'contact your system administrator'
            )

        conn = SMConnections.make_connection(credentials)
        await conn.connect()

        return conn

    @staticmethod
    async def get_connection_no_project(
        author: str, ar_guid: str, meta: dict[str, str]
    ):
        """Get a db connection from a project and user"""
        # maybe it makes sense to perform permission checks here too
        logger.debug(f'Authenticate no-project connection with {author!r}')

        conn = await SMConnections.get_made_connection()

        # we don't authenticate project-less connection, but rely on the
        # the endpoint to validate the resources

        return Connection(
            connection=conn,
            author=author,
            project=None,
            on_behalf_of=None,
            ar_guid=ar_guid,
            readonly=False,
            meta=meta,
        )
