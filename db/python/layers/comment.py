from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.tables.comment import CommentTable
from models.models.comment import CommentEntityType, DiscussionInternal


class CommentLayer(BaseLayer):
    """Layer for cohort logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)

        self.ct = CommentTable(connection)

    async def get_discussion_for_entity_ids(
        self, entity: CommentEntityType, entity_ids: list[int]
    ) -> list[DiscussionInternal | None]:
        """Query Cohorts"""
        return await self.ct.get_discussion_for_entity_ids(entity, entity_ids)
