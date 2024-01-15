from typing import Optional, Union, List
from models.base import SMBase



class Directory(SMBase):
    """Directory model for external use"""

    location: Optional[str]
    path: str
    basename: str
    listing: Optional[List[Union['File', 'Directory']]]
    writable: Optional[bool]

    def to_internal(self):
        """Convert to internal model"""
        return DirectoryInternal(
            location=self.location,
            path=self.path,
            basename=self.basename,
            listing=[item.to_internal() for item in self.listing or []],
            writable=self.writable,
        )


class DirectoryInternal(SMBase):
    """Directory model for internal use"""

    id: int
    location: Optional[str]
    path: str
    basename: str
    listing: Optional[List[Union['FileInternal', 'DirectoryInternal']]]
    writable: Optional[bool]

    def to_external(self):
        """Convert to external model"""
        return Directory(
            id=self.id,
            location=self.location,
            path=self.path,
            basename=self.basename,
            listing=[item.to_external() for item in self.listing or []],
            writable=self.writable,
        )    


class File(SMBase):
    """File model for external use"""

    path: str
    basename: str
    dirname: str
    nameroot: str
    nameext: Optional[str]
    checksum: Optional[str]
    size: int
    secondaryFiles: Optional[List[Union['File', Directory]]]

    def to_internal(self):
        """Convert to internal model"""
        return FileInternal(
            path=self.path,
            basename=self.basename,
            dirname=self.dirname,
            nameroot=self.nameroot,
            nameext=self.nameext,
            checksum=self.checksum,
            size=self.size,
            secondaryFiles=[f.to_internal() for f in self.secondaryFiles or []],
        )
    

class FileInternal(SMBase):
    """File model for internal use"""

    id: int
    path: str
    basename: str
    dirname: str
    nameroot: str
    nameext: str | None
    checksum: str | None
    size: int
    secondaryFiles: List[Union['FileInternal', DirectoryInternal]] | None

    def to_external(self):
        """Convert to external model"""
        return File(
            id=self.id,
            path=self.path,
            basename=self.basename,
            dirname=self.dirname,
            nameroot=self.nameroot,
            nameext=self.nameext,
            checksum=self.checksum,
            size=self.size,
            secondaryFiles=[f.to_external() for f in self.secondaryFiles or []],
        )