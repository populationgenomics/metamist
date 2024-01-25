import datetime

import orjson

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
        meta = {}
        if 'meta' in d:
            meta = orjson.loads(d.pop('meta'))  # pylint: disable=maybe-no-member

        return AuditLogInternal(meta=meta, **d)
