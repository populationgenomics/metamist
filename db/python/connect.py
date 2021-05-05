"""
Code for connecting to Postgres database
"""
import logging

from contextlib import contextmanager
from typing import Dict, Optional

import psycopg2


logger = logging.getLogger(__name__)


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

    _connections: Optional[Dict] = None
    _databases = ['sm_dev']

    @staticmethod
    def _get_connections(force_reconnect=False):
        if SMConnections._connections is None or force_reconnect:
            SMConnections._connections = {
                dbname: psycopg2.connect(f'dbname={dbname}')
                for dbname in SMConnections._databases
            }

        return SMConnections._connections

    @staticmethod
    def get_connection_for_project(project, user):
        """Get a db connection from a project and user"""
        # maybe it makes sense to perform permission checks here too
        logger.debug(f'Authenticate the connection with "{user}"')

        conn = SMConnections._get_connections().get(project)
        if conn is None:
            raise ProjectDoesNotExist(project)
        return conn


class DbBase:
    """Base class for table subclasses"""

    @classmethod
    def from_project(cls, project, user):
        """Create the Db object from a project with user details"""
        return cls(connection=SMConnections.get_connection_for_project(project, user))

    def __init__(self, connection=None):
        self._connection = connection

        if connection is None:
            raise Exception(
                f'No connection was provided to the table "{self.__class__.__name__}"'
            )

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
