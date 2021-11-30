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
