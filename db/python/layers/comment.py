from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.tables.comment import CommentTable
from db.python.utils import get_logger
from models.models.comment import CommentInternal

logger = get_logger()


class CommentLayer(BaseLayer):
    """Layer for cohort logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)

        self.ct = CommentTable(connection)

    async def query(self, entity: str, entity_id: int) -> list[CommentInternal]:
        """Query Cohorts"""
        rows = await self.ct.query(entity, entity_id)
        return rows
