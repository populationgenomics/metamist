import json
from typing import Optional

from models.base import SMBase


class Project(SMBase):
    """Row for project in 'project' table"""

    id: Optional[int] = None
    name: Optional[str] = None
    dataset: Optional[str] = None
    meta: Optional[dict] = None
    read_group_id: Optional[int] = None
    write_group_id: Optional[int] = None

    @staticmethod
    def from_db(kwargs):
        """From DB row, with db keys"""
        kwargs = dict(kwargs)
        kwargs['meta'] = json.loads(kwargs['meta']) if kwargs.get('meta') else {}
        return Project(**kwargs)
