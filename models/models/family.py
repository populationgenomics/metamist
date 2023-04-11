from typing import Optional

from pydantic import BaseModel


class Family(BaseModel):
    """Family model"""

    id: int
    external_id: str
    project: int
    description: Optional[str] = None
    coded_phenotype: Optional[str] = None

    @staticmethod
    def from_db(d):
        """From DB fields"""
        return Family(**d)


class PedRowInternal:
    """Class for capturing a row in a pedigree"""

    def __init__(
        self,
        family_id: int,
        participant_id: int,
        paternal_id: int | None,
        maternal_id: int | None,
        affected: int | None,
        notes: str | None,
    ):
        self.family_id = family_id
        self.participant_id = participant_id
        self.paternal_id = paternal_id
        self.maternal_id = maternal_id
        self.affected = affected
        self.notes = notes
