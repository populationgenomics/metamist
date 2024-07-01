from pathlib import Path
from typing import Any, Optional, TypeAlias

from cloudpathlib import AnyPath, CloudPath, GSPath
from google.cloud.storage import Client
from pydantic import BaseModel

from models.base import SMBase, parse_sql_bool

RecursiveDict: TypeAlias = dict[str, 'str | RecursiveDict']


class OutputFileInternal(SMBase):
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
        return OutputFileInternal(
            id=kwargs.pop('id'),
            path=kwargs.get('path'),
            basename=kwargs.get('basename'),
            dirname=kwargs.get('dirname'),
            nameroot=kwargs.get('nameroot'),
            nameext=kwargs.get('nameext'),
            file_checksum=kwargs.get('file_checksum'),
            size=kwargs.get('size'),
            meta=kwargs.get('meta'),
            valid=parse_sql_bool(kwargs.get('valid')),
        )

    def to_external(self):
        """
        Convert to external model
        """
        return OutputFile(
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
    async def get_file_info(
        file_obj: CloudPath | AnyPath | GSPath | Path, client: Client
    ) -> dict | None:
        """Get file info for file at given path"""
        try:
            file_checksum = None
            valid = False
            size = 0
            # try:
            if isinstance(file_obj, GSPath) and client:
                bucket = client.get_bucket(file_obj.bucket)  # pylint: disable=E1101
                blob = bucket.get_blob(file_obj.blob)  # pylint: disable=E1101
                if file_obj.suffix != '.mt' and blob:
                    file_checksum = blob.crc32c  # pylint: disable=E1101
                    valid = True
                    size = blob.size  # pylint: disable=E1101

            return {
                'basename': file_obj.name,  # pylint: disable=E1101
                'dirname': str(file_obj.parent),  # pylint: disable=E1101
                'nameroot': file_obj.stem,  # pylint: disable=E1101
                'nameext': file_obj.suffix,  # pylint: disable=E1101
                'checksum': file_checksum,
                'size': size,  # pylint: disable=E1101
                'valid': valid,
            }
        except (FileNotFoundError, ValueError):
            return None

    @staticmethod
    def reconstruct_json(data: list | str) -> str | RecursiveDict:
        """_summary_

        Args:
            data (list): A tuple of (OutputFileInternal, json_structure) if file_id is not None, else just the output string based on the
                analysis_outputs table

        Returns:
            dict: Should return the JSON structure based on the input data.
        """
        root: dict = {}
        if isinstance(data, str):
            return data
        for file in data:
            file_root: dict = {}

            # Check if the file is a tuple or a string
            # If it's a tuple, it's a file object and a json structure
            if isinstance(file, tuple):
                file_obj, json_structure = file
                file_obj = file_obj.dict()
                fields = (
                    OutputFileInternal.model_fields.keys()
                )  # type:ignore[attr-defined]
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


class OutputFile(BaseModel):
    """File model for external use"""

    id: int
    path: str
    basename: str
    dirname: str
    nameroot: str
    nameext: Optional[str]
    file_checksum: Optional[str]
    size: int
    meta: Optional[str] = None
    valid: bool = False
    secondary_files: list[dict[str, Any]] | None = None

    def to_internal(self):
        """
        Convert to internal model
        """
        return OutputFileInternal(
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
