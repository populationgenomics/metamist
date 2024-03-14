import json
from typing import Optional

from models.base import SMBase

ProjectId = int


class Project(SMBase):
    """Row for project in 'project' table"""

    id: ProjectId
    name: str
    dataset: Optional[str] = None
    meta: Optional[dict] = None

    @staticmethod
    def from_db(kwargs):
        """From DB row, with db keys"""
        kwargs = dict(kwargs)
        kwargs['meta'] = json.loads(kwargs['meta']) if kwargs.get('meta') else {}
        return Project(**kwargs)
