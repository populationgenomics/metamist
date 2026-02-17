from collections import defaultdict

from psycopg import sql

from db.python.connect import Connection
from db.python.utils import InternalError
from models.models.audit_log import AuditLogInternal


class DbBase:
    """Base class for table subclasses"""

    connection: Connection

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

        self.connection = connection
        self.author = connection.author
        self.project = connection.project
        self.project_id = connection.project_id

        if self.author is None:
            raise InternalError(f'Must provide author to {self.__class__.__name__}')

    async def audit_log_id(self):
        """
        Get audit_log ID (or fail otherwise)
        """
        return await self.connection.audit_log_id()

    # piped from the connection

    async def get_all_audit_logs_for_table(
        self, table: str, ids: list[int], id_field='id'
    ) -> dict[int, list[AuditLogInternal]]:
        """
        Get all audit logs for values from a table
        """
        history_table = sql.Identifier(f'{table}_history')

        new_query = t"""
        WITH historical_rows AS (
        SELECT {id_field:i}, audit_log_id
        FROM {table:i}
        WHERE {id_field:i} = ANY({ids:s})

        UNION ALL

        SELECT {id_field:i}, audit_log_id
        FROM {history_table:i}
        WHERE {id_field:i} = ANY({ids:s}))

        SELECT hr.{id_field:i} as table_id, al.id as id, al.author as author, al.on_behalf_of as on_behalf_of,
        al.timestamp as timestamp, al.ar_guid as ar_guid, al.comment as comment, al.auth_project as auth_project,
        al.meta as meta FROM historical_rows hr
        INNER JOIN audit_log al ON al.id = hr.audit_log_id
        """

        rows = await (await self.connection.pg_connection.execute(new_query)).fetchall()
        by_id = defaultdict(list)
        for r in rows:
            id_value = r.pop('table_id')
            by_id[id_value].append(AuditLogInternal(**r))

        return by_id
