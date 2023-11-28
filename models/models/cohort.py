from models.base import SMBase
from models.models.sequencing_group import SequencingGroup, SequencingGroupExternalId


class Cohort(SMBase):
    """Model for Cohort"""

    id: str
    name: str
    author: str
    project: str
    description: str
    derived_from: int | None
    sequencing_groups: list[SequencingGroup | SequencingGroupExternalId]

    @staticmethod
    def from_db(d: dict):
        """
        Convert from db keys, mainly converting id to id_
        """
        _id = d.pop('id', None)
        project = d.pop('project', None)
        description = d.pop('description', None)
        name = d.pop('name', None)
        author = d.pop('author', None)
        derived_from = d.pop('derived_from', None)
        sequencing_groups = d.pop('sequencing_groups', [])

        return Cohort(
            id=_id,
            name=name,
            author=author,
            project=project,
            description=description,
            derived_from=derived_from,
            sequencing_groups=sequencing_groups,
        )