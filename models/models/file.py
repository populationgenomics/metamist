import json
from collections import defaultdict
from typing import Any, Generator, Union

import cloudpathlib.exceptions as cloudpathlib_exceptions
import google.api_core.exceptions as google_exceptions
from google.cloud import storage
from cloudpathlib import AnyPath, GSPath
from cloudpathlib.exceptions import AnyPathTypeError
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
    checksum: str | None
    size: int
    meta: str | None = None
    # exists: bool = False # make it optional
    # secondary_files: list[str] | None = []

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
        checksum = kwargs.get('checksum')
        size = kwargs.get('size')
        meta = kwargs.get('meta')
        # exists = kwargs.get('exists')
        # secondary_files = kwargs.get('secondary_files')

        return File(
            id=kwargs.pop('id'),
            path=path,
            basename=basename,
            dirname=dirname,
            nameroot=nameroot,
            nameext=nameext,
            checksum=checksum,
            size=size,
            meta=meta,
            # exists=exists,
            # secondary_files=secondary_files,
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
            checksum=self.checksum,
            size=self.size,
            meta=self.meta,
            # exists=self.exists,
            # secondary_files=self.secondary_files,
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

    # TODO use async
    @staticmethod
    def get_checksum(path: str) -> str:
        """Get checksum for file at given path"""
        if FileInternal.get_extension(path) == '.mt':
            return None

        # TODO replace md5 with crc32c
        if isinstance(AnyPath(path), GSPath):
            try:
                storage_client = storage.Client()
                bucket = storage_client.bucket(AnyPath(path).bucket)  # pylint: disable=E1101
                blob = bucket.blob(AnyPath(path).name)  # pylint: disable=E1101
                return blob.md5_hash
            except (google_exceptions.NotFound, FileNotFoundError, AnyPathTypeError):
                return None
        return None

    @staticmethod
    def get_size(path: str) -> int:
        """Get file size"""
        try:
            return AnyPath(path).stat().st_size  # pylint: disable=E1101
        except (google_exceptions.NotFound, cloudpathlib_exceptions.NoStatError, FileNotFoundError):
            return 0

    @staticmethod
    def find_files_from_dict(json_dict: Union[dict, str], json_path=None) -> Generator[tuple[Any, Any], None, None]:
        """Retrieve filepaths from a dict of outputs"""
        if isinstance(json_dict, str):
            try:
                json_dict = json.loads(json_dict)
            except json.JSONDecodeError:
                print(f'Error parsing JSON: {json_dict}')

        if not json_path:
            json_path = []

        if isinstance(json_dict, dict):
            for key, value in json_dict.items():
                if isinstance(value, dict):
                    # If the value is a dictionary, continue the recursion
                    yield from FileInternal.find_files_from_dict(value, json_path + [key])
                else:
                    # Found a leaf, yield the json_path and the value dict containing file_path
                    yield json_path, json_dict
        else:
            # If the input is not a dictionary, just return the value
            yield json_path, json_dict  # type: ignore[misc]

    @staticmethod
    def reconstruct_json(data: list) -> list | dict:
        """Reconstruct a JSON object from a list of paths and values"""

        # TODO change to dict once new input structure is in place
        root: list = []
        for file in data:
            file_root: dict = defaultdict(dict)
            fields = FileInternal.__fields__.keys()  # type:ignore[attr-defined]
            for field in fields:
                file_root[field] = getattr(file, field)

            # if file.json_structure:
            #     # Split the path into components and parse the JSON content
            #     path = file.json_path.split('.')
            #     try:
            #         content = json.loads(file_root)
            #     except json.JSONDecodeError:
            #         print(f'Error parsing JSON: {file_root}')

            #     # Navigate down the tree to the correct position, creating dictionaries as needed
            #     current = root
            #     for key in path[:-1]:
            #         current = current.setdefault(key, [])

            #     if path[-1] in current:
            #         current[path[-1]].append(content)
            #     else:
            #         current[path[-1]] = content
            # else:
            root.append(file_root)
        return root


class File(BaseModel):
    """File model for external use"""

    id: int
    path: str
    basename: str
    dirname: str
    nameroot: str
    nameext: str | None
    checksum: str | None
    size: int
    meta: str | None = None
    # exists: bool = False
    # secondary_files: list[str] | None = []

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
            checksum=self.checksum,
            size=self.size,
            meta=self.meta,
            # exists=self.exists,
            # secondary_files=self.secondary_files,
        )
