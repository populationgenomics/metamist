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
    ['project', 'family', 'sample', 'assay', 'participant', 'sequencing_group'],
)


# Timestamps in the db come back without a timezone attached but should be sent through
# as UTC time, so add the timezone here so it is sent through to the client
def assume_utc(time: datetime.datetime | None):
    """Update timestamp to use UTC as timezone"""
    if time is None:
        return None
    return time.replace(tzinfo=datetime.timezone.utc)


class CommentVersionInternal(SMBase):
    """Model for comment versions"""

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
    requested_entity_id: int
    comment_entity_type: CommentEntityType
    comment_entity_id: int
    versions: list[CommentVersionInternal]
    thread: list['CommentInternal']
    status: CommentStatus

    def add_comment_to_thread(self, comment: 'CommentInternal'):
        """Append the provided comment to this comment's thread"""
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
                timestamp=assume_utc(v.get('timestamp')),
                status=v.get('status'),
                content=v.get('content'),
            )
            for v in versions[0:-1]
        ]

        return CommentInternal(
            id=first_version.get('comment_id'),
            parent_id=last_version.get('parent_id'),
            requested_entity_id=first_version.get('requested_entity_id'),
            comment_entity_type=first_version.get('comment_entity_type'),
            comment_entity_id=first_version.get('comment_entity_id'),
            content=last_version.get('content'),
            author=first_version.get('author'),
            created_at=assume_utc(first_version.get('timestamp')),
            updated_at=assume_utc(last_version.get('timestamp')),
            thread=[],
            versions=history,
            status=last_version.get('status'),
        )


class DiscussionInternal(SMBase):
    """
    Model for a comment discussion containing comments made directly on the entity, as
    well as comments made on related entities
    """

    direct_comments: list[CommentInternal]
    related_comments: list[CommentInternal]

    @staticmethod
    def from_flat_comments(
        comments: list[CommentInternal],
        requested_entity_type: CommentEntityType,
        requested_entity_id: int,
    ):
        """
        Convert a flat list of comments into two groups, direct comments and related
        comments.
        """
        direct_comments: list[CommentInternal] = []
        related_comments: list[CommentInternal] = []

        for comment in comments:
            if (
                comment.comment_entity_id == requested_entity_id
                and comment.comment_entity_type == requested_entity_type
            ):
                direct_comments.append(comment)
            else:
                related_comments.append(comment)

        return DiscussionInternal(
            direct_comments=direct_comments, related_comments=related_comments
        )
