import datetime

import strawberry

from models.models.audit_log import AuditLogInternal


@strawberry.type
class GraphQLAuditLog:
    """AuditLog GraphQL model"""

    id: int
    author: str
    timestamp: datetime.datetime
    ar_guid: str | None
    comment: str | None
    meta: strawberry.scalars.JSON

    @staticmethod
    def from_internal(audit_log: AuditLogInternal) -> 'GraphQLAuditLog':
        """From internal model"""
        return GraphQLAuditLog(
            id=audit_log.id,
            author=audit_log.author,
            timestamp=audit_log.timestamp,
            ar_guid=audit_log.ar_guid,
            comment=audit_log.comment,
            meta=audit_log.meta,
        )
