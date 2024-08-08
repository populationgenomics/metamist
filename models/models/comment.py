import datetime
from enum import StrEnum
from typing import Any

from models.base import SMBase

CommentStatus = StrEnum(
    'CommentStatus',
    ['active', 'deleted'],
)

CommentEntityType = StrEnum(
    'CommentEntityType',
    ['project', 'sample', 'assay', 'participant', 'sequencing_group'],
)


class CommentVersionInternal(SMBase):
    content: str
    author: str
    status: CommentStatus
    timestamp: datetime.datetime


class CommentInternal(SMBase):
    """Model for Cohort"""

    id: int
    parent_id: int | None
    content: str
    author: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    entity_type: CommentEntityType
    entity_id: int
    versions: list[CommentVersionInternal]
    thread: list['CommentInternal']
    status: CommentStatus

    def add_comment_to_thread(self, comment: 'CommentInternal'):
        self.thread.append(comment)

    @staticmethod
    def from_db_versions(versions: list[dict[str, Any]]):
        """
        Convert from a list of ordered comment versions to a
        single comment instance with version
        """

        first_version = versions[0]
        last_version = versions[-1]

        history = [
            CommentVersionInternal(
                author=v.get('author'),
                timestamp=v.get('timestamp'),
                status=v.get('status'),
                content=v.get('content'),
            )
            for v in versions[1:]
        ]

        return CommentInternal(
            id=first_version.get('comment_id'),
            parent_id=last_version.get('parent_id'),
            entity_type=first_version.get('entity_type'),
            entity_id=first_version.get('entity_id'),
            content=last_version.get('content'),
            author=first_version.get('author'),
            created_at=first_version.get('timestamp'),
            updated_at=last_version.get('timestamp'),
            thread=[],
            versions=history,
            status=last_version.get('status'),
        )
