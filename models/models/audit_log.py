import datetime
import json
from typing import Any

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
    def from_db(d: dict[str, Any]):
        """Take DB mapping object, and return SampleSequencing"""
        meta = {}
        if 'meta' in d:
            meta = json.loads(d.pop('meta') or {})

        return AuditLogInternal(meta=meta, **d)
