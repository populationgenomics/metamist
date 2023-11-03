import databases
from db.python.connect import Connection
from db.python.utils import InternalError


class DbBase:
    """Base class for table subclasses"""

    def __init__(self, connection: Connection):
        if connection is None:
            raise InternalError(
                f'No connection was provided to the table {self.__class__.__name__!r}'
            )
        if not isinstance(connection, Connection):
            raise InternalError(
                f'Expected connection type Connection, received {type(connection)}, '
                f'did you mean to call self._connection?'
            )

        self._connection = connection
        self.connection: databases.Database = connection.connection
        self.author = connection.author
        self.project = connection.project

        if self.author is None:
            raise InternalError(f'Must provide author to {self.__class__.__name__}')

    @property
    def changelog_id(self):
        """
        Get changelog ID (or fail otherwise)
        """
        return self._connection.changelog_id

    # piped from the connection

    @staticmethod
    def escape_like_term(query: str):
        """
        Escape meaningful keys when using LIKE with a user supplied input
        """
        return query.replace('%', '\\%').replace('_', '\\_')
