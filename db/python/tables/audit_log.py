from typing import Any

from psycopg.types.json import Jsonb

from db.python.tables.base import DbBase
from models.models.audit_log import AuditLogInternal
from models.models.project import ProjectId


class AuditLogTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    table_name = 'audit_log'

    async def get_projects_for_ids(self, audit_log_ids: list[int]) -> set[ProjectId]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = t"""
            SELECT DISTINCT auth_project
            FROM audit_log
            WHERE id = ANY({audit_log_ids})
        """
        if len(audit_log_ids) == 0:
            raise ValueError('Received no audit log IDs')
        rows = await (await self.connection.pg_connection.execute(_query)).fetchall()
        return {r['project'] for r in rows}

    async def get_audit_logs_for_ids(
        self, audit_log_ids: list[int]
    ) -> list[AuditLogInternal]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = t"""
            SELECT id, timestamp, author, on_behalf_of, ar_guid, comment, auth_project
            FROM audit_log
            WHERE id = ANY({audit_log_ids})
        """
        if len(audit_log_ids) == 0:
            raise ValueError('Received no audit log IDs')

        async with self.connection.pg_connection.cursor(
            class_row=AuditLogInternal
        ) as cur:
            audit_log_rows = await (await cur.execute(_query)).fetchall()

        return audit_log_rows

    async def create_audit_log(
        self,
        author: str,
        on_behalf_of: str | None,
        ar_guid: str | None,
        comment: str | None,
        project: ProjectId | None,
        meta: dict[str, Any] | None = None,
    ) -> int:
        """
        Create a new audit log entry
        """

        meta_param = Jsonb(meta or {})
        _query = t"""
        INSERT INTO audit_log
            (author, on_behalf_of, ar_guid, comment, auth_project, meta)
        VALUES
            ({author}, {on_behalf_of}, {ar_guid}, {comment}, {project}, {meta_param})
        RETURNING id
        """
        res = await self.connection.pg_connection.execute(_query)
        row: dict[str, int] | None = await res.fetchone()
        assert row
        return row['id']
