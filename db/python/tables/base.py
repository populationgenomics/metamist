from collections import defaultdict

import databases

from db.python.connect import Connection
from db.python.utils import InternalError
from models.models.audit_log import AuditLogInternal


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

    async def audit_log_id(self):
        """
        Get audit_log ID (or fail otherwise)
        """
        return await self._connection.audit_log_id()

    # piped from the connection

    async def get_all_audit_logs_for_table(
        self, table: str, ids: list[int], id_field='id'
    ) -> dict[int, list[AuditLogInternal]]:
        """
        Get all audit logs for values from a table
        """
        _query = f"""
        SELECT
            t.{id_field} as table_id,
            al.id as id,
            al.author as author,
            al.on_behalf_of as on_behalf_of,
            al.timestamp as timestamp,
            al.ar_guid as ar_guid,
            al.comment as comment,
            al.auth_project as auth_project,
            al.meta as meta
        FROM {table} FOR SYSTEM_TIME ALL t
        INNER JOIN audit_log al
        ON al.id = t.audit_log_id
        WHERE t.{id_field} in :ids
        """.strip()
        rows = await self.connection.fetch_all(_query, {'ids': ids})
        by_id = defaultdict(list)
        for r in rows:
            row = dict(r)
            id_value = row.pop('table_id')
            by_id[id_value].append(AuditLogInternal.from_db(row))

        return by_id
