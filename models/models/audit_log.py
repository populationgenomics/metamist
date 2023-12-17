import datetime

from models.base import SMBase


class AuditLogInternal(SMBase):
    """
    Model for audit_log
    """

    id: int
    timestamp: datetime.datetime
    author: str
    on_behalf_of: str | None
    ar_guid: str | None
    comment: str | None

    @staticmethod
    def from_db(d: dict):
        """Take DB mapping object, and return SampleSequencing"""
        return AuditLogInternal(**d)
