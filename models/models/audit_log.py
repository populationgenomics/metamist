import datetime

from models.base import SMBase
from models.models.project import ProjectId

AuditLogId = int


class AuditLogInternal(SMBase):
    """
    Model for audit_log
    """

    id: AuditLogId
    timestamp: datetime.datetime
    author: str
    auth_project: ProjectId
    on_behalf_of: str | None
    ar_guid: str | None
    comment: str | None

    @staticmethod
    def from_db(d: dict):
        """Take DB mapping object, and return SampleSequencing"""
        return AuditLogInternal(**d)
