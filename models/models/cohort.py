from models.base import SMBase


class Cohort(SMBase):
    """ Model for Cohort """
    id: str
    project: str
    description: str

    @staticmethod
    def from_db(d: dict):
        """
        Convert from db keys, mainly converting id to id_
        """
        _id = d.pop('id', None)
        project = d.pop('project', None)
        description = d.pop('description', None)
        return Cohort(id=_id, project=project, description=description)
