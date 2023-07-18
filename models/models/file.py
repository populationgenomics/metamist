from enum import Enum

from pydantic import BaseModel


class FileType(Enum):
    """Type of File that we record"""

    FILE = 'file'
    DIRECTORY = 'directory'


class File(BaseModel):
    """Model to represent File"""

    id: int
    type: FileType
    path: str
    size: int  # in bytes
    checksum: str | None
    exists: bool

    @staticmethod
    def from_db(**kwargs) -> 'File':
        """From DB fields"""
        return File(**kwargs)
