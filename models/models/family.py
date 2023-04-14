from typing import Optional

from pydantic import BaseModel

class FamilyInternal(BaseModel):
    """Family model"""

    id: int
    external_id: str
    project: int
    description: Optional[str] = None
    coded_phenotype: Optional[str] = None

    @staticmethod
    def from_db(d):
        """From DB fields"""
        return FamilyInternal(**d)

    def to_external(self):
        """Convert to external model"""
        return Family(
            id=self.id,
            external_id=self.external_id,
            project=self.project,
            description=self.description,
            coded_phenotype=self.coded_phenotype,
        )

class Family(BaseModel):
    """Family model"""

    id: int | None
    external_id: str
    project: int
    description: Optional[str] = None
    coded_phenotype: Optional[str] = None

    @staticmethod
    def from_db(d):
        """From DB fields"""
        return Family(**d)

    def to_internal(self):
        """Convert to internal model"""
        return FamilyInternal(
            id=self.id,
            external_id=self.external_id,
            project=self.project,
            description=self.description,
            coded_phenotype=self.coded_phenotype,
        )


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
