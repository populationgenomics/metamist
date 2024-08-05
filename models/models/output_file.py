import os

# from pathlib import Path
import re
from typing import TypeAlias

from google.cloud.storage import Blob, Client
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
    async def list_blobs(
        bucket: str,
        prefix: str,
        delimiter: str,
        client: Client,
        versions: bool = False,
    ) -> list[Blob]:
        """
        List blobs in a bucket
        """
        blobs = client.list_blobs(
            bucket, prefix=prefix, delimiter=delimiter, versions=versions
        )
        return list(blobs)

    @staticmethod
    async def extract_bucket_params(
        path: str,
    ) -> dict:
        """Extracts bucket from output path"""
        # Define the regex pattern
        pattern = r'gs://([^/]+)(/.*)?'

        # Match the pattern
        match = re.match(pattern, path)

        bucket = None
        prefix = None
        basename = None
        dirname = None
        file_extension = None
        file_stem = None
        if match:
            bucket = match.group(1)
            path_after_bucket = match.group(2) or ''
            dirname = f'gs://{bucket}{os.path.dirname(path_after_bucket)}'
            basename = os.path.basename(path_after_bucket)
            file_extension = os.path.splitext(basename)[1] if basename else ''
            file_stem = os.path.splitext(basename)[0] if basename else ''
            prefix = (
                os.path.dirname(path_after_bucket) + '/'
                if path_after_bucket.count('/') > 1
                else ''
            )
            blob_name = f'{path_after_bucket.strip("/")}'
        else:
            return {}

        if not bucket:
            return {}

        return {
            'bucket': bucket,
            'prefix': prefix,
            'delimiter': '/',
            'dirname': dirname,
            'basename': basename,
            'file_extension': file_extension,
            'file_stem': file_stem,
            'blob_name': blob_name,
        }

    @staticmethod
    async def get_file_info(
        path: str,
        client: Client,
        blobs: list[Blob] | None = None,
    ) -> dict | None:
        """Get file info for file at given path"""
        try:
            file_checksum = None
            valid = False
            size = 0

            params = await OutputFileInternal.extract_bucket_params(
                path=path,
            )
            if not params:
                return None
            if path.startswith('gs://') and client:
                if not blobs and not isinstance(blobs, list):
                    blobs = await OutputFileInternal.list_blobs(
                        bucket=params['bucket'],
                        prefix=params['prefix'],
                        delimiter=params['delimiter'],
                        client=client,
                        versions=False,
                    )
                for blob in blobs:
                    if blob.name == params['blob_name']:
                        if params['file_extension'] != '.mt':
                            file_checksum = (
                                blob.crc32c
                            )  # pylint: disable=E1101
                            valid = True
                            size = blob.size  # pylint: disable=E1101

            return {
                'basename': params['basename'],  # pylint: disable=E1101
                'dirname': params['dirname'],  # pylint: disable=E1101
                'nameroot': params['file_stem'],  # pylint: disable=E1101
                'nameext': params['file_extension'],  # pylint: disable=E1101
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
