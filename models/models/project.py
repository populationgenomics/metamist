from typing import Optional

from db.python.utils import from_db_json
from models.base import SMBase

ProjectId = int


class Project(SMBase):
    """Row for project in 'project' table"""

    id: Optional[ProjectId] = None
    name: Optional[str] = None
    dataset: Optional[str] = None
    meta: Optional[dict] = None
    read_group_id: Optional[int] = None
    write_group_id: Optional[int] = None

    @staticmethod
    def from_db(kwargs):
        """From DB row, with db keys"""
        kwargs = dict(kwargs)
        kwargs['meta'] = from_db_json(kwargs.get('meta')) or {}
        return Project(**kwargs)
