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
    def _get_connection_for_project(project, user):
        # maybe it makes sense to perform permission checks here too
        logger.debug(f'Authenticate the connection with "{user}"')

        conn = SMConnections._get_connections().get(project)
        if conn is None:
            raise ProjectDoesNotExist(project)
        return conn

    @staticmethod
    @contextmanager
    def get_cursor(project, user, auto_commit=False):
        """
        Use like:

            with SMConnections.get_cursor('project_id', user) as cursor:
                cursor.execute('<query>')
                records = cursor.fetch_all()
        """
        conn = SMConnections._get_connection_for_project(project, user)
        cur = conn.cursor()

        def commit():
            return conn.commit()

        yield cur, commit

        if auto_commit:
            cur.commit()


class DbBase:
    """Base class for table subclasses"""

    def __init__(self, cursor):

        self._cursor = cursor

        if cursor is None:
            raise Exception('No cursor was provided to the DbBase')
