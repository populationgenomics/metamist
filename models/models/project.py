from models.base import SMBase


class ProjectRow(SMBase):
    """Row for project in 'project' table """

    def __init__(self, id_: int, name: str, gcp_id: str, dataset: str):
        self.id = id_
        self.name = name
        self.gcp_id = gcp_id
        self.dataset = dataset

    @staticmethod
    def from_db(kwargs):
        """From DB row, with db keys"""
        kwargs = dict(kwargs)
        return ProjectRow(id_=kwargs.pop('id'), **kwargs)
