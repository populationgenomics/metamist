from enum import Enum

from models.base import SMBase

GroupProjectRole = Enum('GroupProjectRole', ['read', 'contribute', 'write'])


class Group(SMBase):
    """Row for project in 'project' table"""

    id: int
    name: str
    role: GroupProjectRole

    @staticmethod
    def from_db(kwargs):
        """From DB row, with db keys"""
        kwargs = dict(kwargs)
        return Group(**kwargs)
