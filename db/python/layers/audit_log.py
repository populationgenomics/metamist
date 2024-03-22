from db.python.layers.base import BaseLayer, Connection
from db.python.tables.audit_log import AuditLogTable
from models.models.audit_log import AuditLogId, AuditLogInternal
from models.models.group import ReadAccessRoles


class AuditLogLayer(BaseLayer):
    """Layer for more complex sample logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self.alayer: AuditLogTable = AuditLogTable(connection)

    # GET
    async def get_for_ids(
        self, ids: list[AuditLogId], check_project_id: bool = True
    ) -> list[AuditLogInternal]:
        """Query for samples"""
        if not ids:
            return []

        logs = await self.alayer.get_audit_logs_for_ids(ids)
        if check_project_id:
            projects = {log.auth_project for log in logs}
            await self.ptable.check_access_to_project_ids(
                user=self.author, project_ids=projects, allowed_roles=ReadAccessRoles
            )

        return logs

    # don't put the create here, I don't want it to be public
