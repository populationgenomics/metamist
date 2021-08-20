from models.base import SMBase


class ProjectRow(SMBase):
    """Row for project in 'project' table"""

    id: int = None
    name: str = None
    gcp_id: str = None
    dataset: str = None

    @staticmethod
    def from_db(kwargs):
        """From DB row, with db keys"""
        kwargs = dict(kwargs)
        return ProjectRow(**kwargs)
