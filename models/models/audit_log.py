import datetime

from models.base import SMBase, parse_sql_dict
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
        meta = parse_sql_dict(d.pop('meta', {}))

        return AuditLogInternal(meta=meta, **d)
