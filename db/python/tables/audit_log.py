from db.python.tables.base import DbBase
from models.models.audit_log import AuditLogInternal


class AuditLogTable(DbBase):
    """
    Capture Analysis table operations and queries
    """

    table_name = 'audit_log'

    async def get_audit_logs_by_ids(
        self, audit_log_ids: list[int]
    ) -> list[AuditLogInternal]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = """
            SELECT id, timestamp, author, on_behalf_of, ar_guid, comment 
            FROM audit_log
            WHERE id in :audit_log_ids
        """
        if len(audit_log_ids) == 0:
            raise ValueError('Received no audit log IDs')
        rows = await self.connection.fetch_all(_query, {'audit_log_ids': audit_log_ids})
        return [AuditLogInternal.from_db(r) for r in rows]
