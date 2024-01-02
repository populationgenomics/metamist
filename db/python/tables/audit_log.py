from typing import Any

from db.python.tables.base import DbBase
from db.python.utils import to_db_json
from models.models.audit_log import AuditLogInternal
from models.models.project import ProjectId


class AuditLogTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    table_name = 'audit_log'

    async def get_projects_for_ids(self, audit_log_ids: list[int]) -> set[ProjectId]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = """
            SELECT DISTINCT auth_project
            FROM audit_log
            WHERE id in :audit_log_ids
        """
        if len(audit_log_ids) == 0:
            raise ValueError('Received no audit log IDs')
        rows = await self.connection.fetch_all(_query, {'audit_log_ids': audit_log_ids})
        return {r['project'] for r in rows}

    async def get_audit_logs_for_ids(
        self, audit_log_ids: list[int]
    ) -> list[AuditLogInternal]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = """
            SELECT id, timestamp, author, on_behalf_of, ar_guid, comment, auth_project
            FROM audit_log
            WHERE id in :audit_log_ids
        """
        if len(audit_log_ids) == 0:
            raise ValueError('Received no audit log IDs')
        rows = await self.connection.fetch_all(_query, {'audit_log_ids': audit_log_ids})
        return [AuditLogInternal.from_db(dict(r)) for r in rows]

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

        _query = """
        INSERT INTO audit_log
            (author, on_behalf_of, ar_guid, comment, auth_project, meta)
        VALUES
            (:author, :on_behalf_of, :ar_guid, :comment, :project, :meta)
        RETURNING id
        """
        audit_log_id = await self.connection.fetch_val(
            _query,
            {
                'author': author,
                'on_behalf_of': on_behalf_of,
                'ar_guid': ar_guid,
                'comment': comment,
                'project': project,
                'meta': to_db_json(meta or {}),
            },
        )

        return audit_log_id
