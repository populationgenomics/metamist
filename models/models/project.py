from typing import Optional

from models.base import SMBase, parse_sql_dict

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
        kwargs['meta'] = parse_sql_dict(kwargs.get('meta')) or {}
        return Project(**kwargs)
