from pathlib import Path
from typing import TypeAlias

from cloudpathlib import AnyPath, CloudPath, GSPath
from google.cloud.storage import Client
from pydantic import BaseModel

from models.base import SMBase, parse_sql_bool

RecursiveDict: TypeAlias = dict[str, 'str | RecursiveDict']


class OutputFileInternal(SMBase):
    """File model for internal use"""

    id: int
    parent_id: int | None = None
    path: str
    basename: str
    dirname: str
    nameroot: str
    nameext: str | None
    file_checksum: str | None
    size: int
    meta: dict | None = None
    valid: bool = False
    secondary_files: dict | None = {}

    @staticmethod
    def from_db(**kwargs):
        """
        Convert from db keys, mainly converting id to id_
        """
        return OutputFileInternal(
            id=kwargs.pop('id'),
            parent_id=kwargs.pop('parent_id', None),
            path=kwargs.get('path'),
            basename=kwargs.get('basename'),
            dirname=kwargs.get('dirname'),
            nameroot=kwargs.get('nameroot'),
            nameext=kwargs.get('nameext'),
            file_checksum=kwargs.get('file_checksum'),
            size=kwargs.get('size'),
            meta=kwargs.get('meta'),
            valid=parse_sql_bool(kwargs.get('valid')),
            secondary_files=kwargs.get('secondary_files', {}),
        )

    def to_external(self):
        """
        Convert to external model
        """
        return OutputFile(
            id=self.id,
            parent_id=self.parent_id,
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
                if '/' in file_obj.blob:
                    prefix = '/'.join(file_obj.blob.split('/')[:-1]) + '/'
                else:
                    prefix = ''
                delimiter = '/'
                blobs = client.list_blobs(
                    str(file_obj.bucket),
                    versions=False,
                    prefix=prefix,
                    delimiter=str(delimiter),
                )  # pylint: disable=E1101
                for blob in blobs:
                    if blob.name == file_obj.name:
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
    def reconstruct_json(data: list | str) -> str | dict:
        """Reconstruct JSON structure from provided data.

        Args:
            data (list | str): A tuple of (OutputFileInternal, json_structure) if file_id is not None,
                else just the output string based on the analysis_outputs table.

        Returns:
            dict: Should return the JSON structure based on the input data.
        """
        root: dict = {}
        if isinstance(data, str):
            return data

        def add_to_structure(current, path, content):
            """Helper function to add content to the correct place in the structure."""
            for key in path[:-1]:
                if key.isdigit():
                    key = int(key)
                    while len(current) <= key:
                        current.append({})
                    if current[key] is None:
                        current[key] = {}
                    current = current[key]
                else:
                    if key not in current or current[key] is None:
                        current[key] = {}
                    current = current[key]

            final_key = path[-1]
            if final_key.isdigit():
                final_key = int(final_key)
                while len(current) <= final_key:
                    current.append({})
                current[final_key] = content
            else:
                if final_key in current:
                    if isinstance(current[final_key], dict):
                        current[final_key].update(content)
                    else:
                        current[final_key] = content
                else:
                    current[final_key] = content

        for file in data:
            file_root: dict = {}

            # Check if the file is a tuple or a string
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
                    path = json_structure
                    add_to_structure(root, path, file_root)
                else:
                    root.update(file_root)
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
    parent_id: int | None = None
    path: str
    basename: str
    dirname: str
    nameroot: str
    nameext: str | None
    file_checksum: str | None
    size: int
    meta: dict | None = None
    valid: bool = False
    secondary_files: dict | None = {}

    def to_internal(self):
        """
        Convert to internal model
        """
        return OutputFileInternal(
            id=self.id,
            parent_id=self.parent_id,
            path=self.path,
            basename=self.basename,
            dirname=self.dirname,
            nameroot=self.nameroot,
            nameext=self.nameext,
            file_checksum=self.file_checksum,
            size=self.size,
            meta=self.meta,
            valid=self.valid,
            secondary_files=self.secondary_files,
        )
