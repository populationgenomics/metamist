import hashlib
from typing import Generator
import json
from collections import defaultdict

from cloudpathlib.anypath import AnyPath
from pydantic import BaseModel

from models.base import SMBase


class FileInternal(SMBase):
    """File model for internal use"""

    id: int
    analysis_id: int
    path: str
    basename: str
    dirname: AnyPath
    nameroot: str
    nameext: str | None
    checksum: str | None
    size: int
    json_path: str | None = None
    secondary_files: list[str] = []

    @staticmethod
    def from_db(**kwargs):
        """
        Convert from db keys, mainly converting id to id_
        """
        analysis_id = kwargs.get('analysis_id')
        path = kwargs.get('path')
        basename = kwargs.get('basename')
        dirname = kwargs.get('dirname')
        nameroot = kwargs.get('nameroot')
        nameext = kwargs.get('nameext')
        checksum = kwargs.get('checksum')
        size = kwargs.get('size')
        json_path = kwargs.get('json_path')
        secondary_files = kwargs.get('secondary_files')

        return File(
            id=kwargs.pop('id'),
            analysis_id=analysis_id,
            path=path,
            basename=basename,
            dirname=dirname,
            nameroot=nameroot,
            nameext=nameext,
            checksum=checksum,
            size=size,
            json_path=json_path,
            secondary_files=secondary_files,
        )

    def to_external(self):
        """
        Convert to external model
        """
        return File(
            id=self.id,
            analysis_id=self.analysis_id,
            path=self.path,
            basename=self.basename,
            dirname=self.dirname,
            nameroot=self.nameroot,
            nameext=self.nameext,
            checksum=self.checksum,
            size=self.size,
            secondary_files=self.secondary_files,
        )

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
    def get_checksum(path: str) -> str:
        """Get checksum for file at given path"""
        try:
            return hashlib.sha256(AnyPath(path).read_bytes()).hexdigest()  # pylint: disable=E1101
        except FileNotFoundError:
            return None

    @staticmethod
    def get_size(path: str) -> int:
        """Get file size"""
        try:
            return AnyPath(path).stat().st_size  # pylint: disable=E1101
        except FileNotFoundError:
            return 0

    @staticmethod
    def find_files_from_dict(json_dict: dict, path=None) -> Generator[tuple[str, dict], None, None]:
        """Retrieve filepaths from a dict of outputs"""
        if not path:
            path = []

        if isinstance(json_dict, dict):
            for key, value in json_dict.items():
                if isinstance(value, dict):
                    # If the value is a dictionary, continue the recursion
                    yield from FileInternal.find_files_from_dict(value, path + [key])
                else:
                    # Found a leaf, yield the path and the value
                    yield path, json_dict
        else:
            # If the input is not a dictionary, just return the value
            yield path, json_dict

    @staticmethod
    def reconstruct_json(data) -> dict:
        """Reconstruct a JSON object from a list of paths and values"""
        root: dict = defaultdict(dict)
        for path_str, content_str in data:
            # Split the path into components and parse the JSON content
            path = path_str.split('.')

            try:
                content = json.loads(content_str)
            except json.JSONDecodeError:
                print(f'Error parsing JSON: {content_str}')

            # Navigate down the tree to the correct position, creating dictionaries as needed
            current = root
            for key in path[:-1]:
                current = current.setdefault(key, {})

            if path[-1] in current:
                current[path[-1]].update(content)
            else:
                current[path[-1]] = content

        return root


class File(BaseModel):
    """File model for external use"""

    id: int
    analysis_id: int
    path: str
    basename: str
    dirname: AnyPath
    nameroot: str
    nameext: str | None
    checksum: str | None
    size: int
    json_path: str | None = None
    secondary_files: list[str] = []

    def to_internal(self):
        """
        Convert to internal model
        """
        return FileInternal(
            id=self.id,
            analysis_id=self.analysis_id,
            path=self.path,
            basename=self.basename,
            dirname=self.dirname,
            nameroot=self.nameroot,
            nameext=self.nameext,
            checksum=self.checksum,
            size=self.size,
            json_path=self.json_path,
            secondary_files=self.secondary_files,
        )
