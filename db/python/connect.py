"""
Code for connecting to Postgres database
"""
import os
import json
import logging
from typing import Dict, List
from contextlib import contextmanager

import mysql.connector as mysql


logger = logging.getLogger(__name__)


def to_db_json(val):
    """Convert val to json for DB"""
    # return psycopg2.extras.Json(val)
    return json.dumps(val)


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

    def __init__(self, dbname, host=None, port=None, username=None, password=None):
        self.dbname = dbname
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    @staticmethod
    def dev_config():
        """Dev config for local database with name 'sm_dev'"""
        # consider pulling from env variables
        return DatabaseConfiguration(dbname='sm_dev', username='root')


class SMConnections:
    """Contains useful functions for connecting to the database"""

    _connections: Dict = {}

    @staticmethod
    def _get_connections(force_reconnect=False):
        if not SMConnections._connections or force_reconnect:
            configs = [DatabaseConfiguration.dev_config()]
            if os.getenv('SM_ENVIRONMENT') == 'PRODUCTION':
                logger.info('Using production mysql configurations')
                configs = (
                    SMConnections._load_database_configurations_from_secret_manager()
                )

            SMConnections._connections = {
                config.dbname: SMConnections.make_connection(config)
                for config in configs
            }

        return SMConnections._connections

    @staticmethod
    def make_connection(config: DatabaseConfiguration):
        """Create connection from dbname"""
        d = {
            'host': config.host,
            'user': config.username,
            'password': config.password,
            'database': config.dbname,
        }
        # filter empty keys
        d = {k: v for k, v in d.items() if v is not None}
        return mysql.connect(**d, autocommit=True)

    @staticmethod
    def get_connection_for_project(project, user):
        """Get a db connection from a project and user"""
        # maybe it makes sense to perform permission checks here too
        logger.debug(f'Authenticate the connection with "{user}"')

        conn = SMConnections._get_connections().get(project)

        if conn is None:
            raise ProjectDoesNotExist(project)

        if isinstance(conn.autocommit, str) and conn.autocommit.startswith('Traceback'):
            logger.warning('Reconnecting to mariadb')
            if SMConnections._connections:
                SMConnections._connections[project] = SMConnections.make_connection(
                    project
                )

        return conn

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
            author=author,
        )

    def __init__(self, connection, author):
        self._connection = connection
        self.author = author

        if connection is None:
            raise Exception(
                f'No connection was provided to the table "{self.__class__.__name__}"'
            )

        if author is None:
            raise Exception('Must provide author to {self.__class__.__name__}')

    @contextmanager
    def get_cursor(self, auto_commit=False):
        """
        Use like:

            with self.get_cursor() as cursor:
                cursor.execute('<query>')
                records = cursor.fetch_all()
        """
        with self._connection.cursor() as cur:
            yield cur

            if auto_commit:
                self.commit()

    @contextmanager
    def transaction(self):
        """Create a transaction by yielding a connection"""
        with self._connection as c:
            yield c

    def commit(self):
        """Commit a set of changes"""
        self._connection.commit()
