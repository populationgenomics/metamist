from typing import Optional

from pydantic import BaseModel


class Family(BaseModel):
    """Family model"""

    id: int
    external_id: str
    project: int
    description: str | None = None
    coded_phenotype: str | None = None

    @staticmethod
    def from_db(d):
        """From DB fields"""
        return Family(**d)


class PedigreeRow(BaseModel):
    """
    Formed pedigree row
    """
    family_id: int | str | None
    individual_id: int | str | None
    paternal_id: int | str | None
    maternal_id: int | str | None
    sex: int | None
    affected: int | None = None
    notes: str | None = None
