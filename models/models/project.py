from typing import Optional
import json

from models.base import SMBase


class Project(SMBase):
    """Row for project in 'project' table"""

    id: Optional[int] = None
    name: Optional[str] = None
    dataset: Optional[str] = None
    meta: Optional[dict] = None
    read_group_name: Optional[str] = None
    write_group_name: Optional[str] = None

    @staticmethod
    def from_db(kwargs):
        """From DB row, with db keys"""
        kwargs = dict(kwargs)
        kwargs['meta'] = json.loads(kwargs['meta']) if kwargs.get('meta') else {}
        return Project(**kwargs)
