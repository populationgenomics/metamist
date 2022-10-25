from enum import Enum
from typing import Optional

from pydantic import BaseModel

class FileType(Enum):
    FILE = 'file'
    DIRECTORY = 'directory'


class File(BaseModel):
    """Model to represent File"""

    id: int
    type: FileType
    path: str
    # in bytes
    size: int
    checksum: str | None
