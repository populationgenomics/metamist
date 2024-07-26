import datetime

from models.base import SMBase


class CommentInternal(SMBase):
    """Model for Cohort"""

    id: int
    parent_id: int | None
    content: str
    author: str
    timestamp: datetime.datetime

    @staticmethod
    def from_db(d: dict):
        """
        Convert from db keys, mainly converting id to id_
        """

        return CommentInternal(
            id=d.get('comment_id'),
            parent_id=d.get('parent_id'),
            content=d.get('content'),
            author=d.get('author'),
            timestamp=d.get('timestamp'),
        )
