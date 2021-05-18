"""
Code for connecting to Postgres database
"""
import logging

from contextlib import contextmanager
from typing import Dict

# import psycopg2
import json
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


class SMConnections:
    """Contains useful functions for connecting to the database"""

    _connections: Dict = {}
    _databases = ['sm_dev']

    @staticmethod
    def _get_connections(force_reconnect=False):
        if not SMConnections._connections or force_reconnect:
            SMConnections._connections = {
                dbname: SMConnections.make_connection(dbname)
                for dbname in SMConnections._databases
            }

        return SMConnections._connections

    @staticmethod
    def make_connection(dbname):
        """Create connection from dbname"""
        return mysql.connect(
            host='localhost',
            user='liquibase',
            password='CPG_pass123',
            database=dbname,
            autocommit=True,
        )

        # return psycopg2.connect(f'dbname={dbname}')

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
