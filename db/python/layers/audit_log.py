from db.python.layers.base import BaseLayer, Connection
from db.python.tables.audit_log import AuditLogTable
from models.models.audit_log import AuditLogId, AuditLogInternal
from models.models.project import ReadAccessRoles


class AuditLogLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.alayer: AuditLogTable = AuditLogTable(connection)

    # GET
    async def get_for_ids(self, ids: list[AuditLogId]) -> list[AuditLogInternal]:
        """Query for samples"""
        if not ids:
            return []

        logs = await self.alayer.get_audit_logs_for_ids(ids)
        projects = {log.auth_project for log in logs}
        self.connection.check_access_to_projects_for_ids(
            project_ids=projects, allowed_roles=ReadAccessRoles
        )

        return logs

    # don't put the create here, I don't want it to be public
