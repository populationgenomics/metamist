import datetime

from db.python.utils import from_db_json
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
    meta: dict | None

    @staticmethod
    def from_db(d: dict):
        """Take DB mapping object, and return SampleSequencing"""
        meta = d.pop('meta', None)
        return AuditLogInternal(meta=from_db_json(meta) or {}, **d)
