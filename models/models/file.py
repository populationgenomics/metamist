from collections import defaultdict
from typing import Any
from google.cloud import storage
from cloudpathlib import AnyPath, GSPath
from pydantic import BaseModel

from models.base import SMBase


class FileInternal(SMBase):
    """File model for internal use"""

    id: int
    path: str
    basename: str
    dirname: str
    nameroot: str
    nameext: str | None
    file_checksum: str | None
    size: int
    meta: str | None = None
    valid: bool = False
    secondary_files: list[dict[str, Any]] | None = None

    @staticmethod
    def from_db(**kwargs):
        """
        Convert from db keys, mainly converting id to id_
        """
        path = kwargs.get('path')
        basename = kwargs.get('basename')
        dirname = kwargs.get('dirname')
        nameroot = kwargs.get('nameroot')
        nameext = kwargs.get('nameext')
        file_checksum = kwargs.get('file_checksum')
        size = kwargs.get('size')
        meta = kwargs.get('meta')
        valid = kwargs.get('valid')

        return File(
            id=kwargs.pop('id'),
            path=path,
            basename=basename,
            dirname=dirname,
            nameroot=nameroot,
            nameext=nameext,
            file_checksum=file_checksum,
            size=size,
            meta=meta,
            valid=valid,
        )

    def to_external(self):
        """
        Convert to external model
        """
        return File(
            id=self.id,
            path=self.path,
            basename=self.basename,
            dirname=self.dirname,
            nameroot=self.nameroot,
            nameext=self.nameext,
            file_checksum=self.file_checksum,
            size=self.size,
            meta=self.meta,
            valid=self.valid,
        )

    @staticmethod
    async def validate_path(path: str) -> bool:
        """Validate file at given path"""
        try:
            file_obj = AnyPath(path)
            return isinstance(file_obj, GSPath) and file_obj.exists()  # pylint: disable=E1101
        except (FileNotFoundError, ValueError):
            return False

    @staticmethod
    def get_basename(path: str) -> str:
        """Get file basename for file at given path"""
        return AnyPath(path).name  # pylint: disable=E1101

    @staticmethod
    def get_dirname(path: str) -> str:
        """Get file dirname for file at given path"""
        return str(AnyPath(path).parent)  # pylint: disable=E1101

    @staticmethod
    def get_nameroot(path: str) -> str:
        """Get file nameroot for file at given path"""
        return AnyPath(path).stem  # pylint: disable=E1101

    @staticmethod
    def get_extension(path: str) -> str:
        """Get file extension for file at given path"""
        return AnyPath(path).suffix  # pylint: disable=E1101

    @staticmethod
    async def get_checksum(path: str) -> str:
        """Get checksum for file at given path"""
        if FileInternal.get_extension(path) == '.mt':
            return None

        # try:
        if isinstance(AnyPath(path), GSPath):
            # get crc32c checksum for gs file
            client = storage.Client()
            bucket = client.get_bucket(AnyPath(path).bucket)  # pylint: disable=E1101
            blob = bucket.get_blob(AnyPath(path).name)  # pylint: disable=E1101
            return blob.crc32c  # pylint: disable=E1101
            # except (google_exceptions.NotFound, FileNotFoundError, AnyPathTypeError):
        return None

    @staticmethod
    def get_size(path: str) -> int:
        """Get file size"""
        # try:
        return AnyPath(path).stat().st_size  # pylint: disable=E1101
        # except (google_exceptions.NotFound, cloudpathlib_exceptions.NoStatError, FileNotFoundError):
        #     return 0

    @staticmethod
    def reconstruct_json(data: list) -> list | dict:
        """_summary_

        Args:
            data (list): A tuple of (FileInternal, json_structure) if file_id is not None, else just the output string based on the
                analysis_outputs table

        Returns:
            list | dict: Should return the JSON structure based on the input data.
        """
        root: dict = {}
        for file in data:
            file_root: dict = defaultdict(dict)

            # Check if the file is a tuple or a string
            # If it's a tuple, it's a file object and a json structure
            if isinstance(file, tuple):
                file_obj, json_structure = file
                file_obj = file_obj.dict()
                fields = FileInternal.__fields__.keys()  # type:ignore[attr-defined]
                for field in fields:
                    file_root[field] = file_obj.get(field)

                if json_structure:
                    if isinstance(json_structure, str):
                        json_structure = json_structure.split('.')
                    # Split the path into components and parse the JSON content
                    path = json_structure
                    content = file_root

                    # Navigate down the tree to the correct position, creating dictionaries as needed
                    current = root
                    for key in path[:-1]:
                        current = current.setdefault(key, {})

                    if path[-1] in current:
                        current[path[-1]].update(content)
                    else:
                        current[path[-1]] = content
                else:
                    root.update(file_root)

            # If it's a string, it's just the output
            else:
                file_root['output'] = file
                if 'output' in root:
                    root['output'].append(file)
                else:
                    root['output'] = [file]
        return root


class File(BaseModel):
    """File model for external use"""

    id: int
    path: str
    basename: str
    dirname: str
    nameroot: str
    nameext: str | None
    file_checksum: str | None
    size: int
    meta: str | None = None
    valid: bool = False
    secondary_files: list[dict[str, Any]] | None = None

    def to_internal(self):
        """
        Convert to internal model
        """
        return FileInternal(
            id=self.id,
            path=self.path,
            basename=self.basename,
            dirname=self.dirname,
            nameroot=self.nameroot,
            nameext=self.nameext,
            file_checksum=self.file_checksum,
            size=self.size,
            meta=self.meta,
            valid=self.valid,
        )
