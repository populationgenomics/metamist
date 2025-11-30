import os

# from pathlib import Path
import re
from typing import TypeAlias

from google.cloud.storage import Blob, Client
from google.api_core.exceptions import NotFound
from pydantic import BaseModel

from api.settings import METAMIST_GCP_PROJECT
from models.base import SMBase, parse_sql_bool

RecursiveDict: TypeAlias = dict[str, 'str | RecursiveDict']

GCS_CLIENT = None


def get_gcs_client():
    """Return a GCS client"""
    global GCS_CLIENT  # pylint: disable=global-statement
    if GCS_CLIENT:
        return GCS_CLIENT

    if METAMIST_GCP_PROJECT:
        GCS_CLIENT = Client(project=METAMIST_GCP_PROJECT)

    if not GCS_CLIENT:
        GCS_CLIENT = Client()

    return GCS_CLIENT


class OutputFileInternal(SMBase):
    """File model for internal use"""

    id: int | None = None
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
            id=kwargs.pop('id', None),
            parent_id=kwargs.pop('parent_id', None),
            path=kwargs['path'],
            basename=kwargs['basename'],
            dirname=kwargs['dirname'],
            nameroot=kwargs['nameroot'],
            nameext=kwargs['nameext'],
            file_checksum=kwargs['file_checksum'],
            size=kwargs['size'],
            meta=kwargs['meta'],
            valid=parse_sql_bool(kwargs['valid']),
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
    def list_blobs(
        bucket: str,
        prefix: str | None,
        delimiter: str,
        client: Client,
        versions: bool = False,
    ) -> list[Blob]:
        """
        List blobs in a bucket
        """
        try:
            blobs = client.list_blobs(
                bucket, prefix=prefix, delimiter=delimiter, versions=versions
            )
            return list(blobs)
        except NotFound as e:
            print(f'Could not find bucket {bucket}: {e}')
            return []

    @staticmethod
    def extract_bucket_params(
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
            'path_after_bucket': path_after_bucket,
        }

    @staticmethod
    def get_file_info(
        path: str,
        client: Client | None = None,
        blobs: list[Blob] | None = None,
    ) -> 'OutputFileInternal | None':
        """Get file info for file at given path"""
        try:
            if not client:
                client = get_gcs_client()

            file_checksum = None
            valid = False
            size = 0

            params = OutputFileInternal.extract_bucket_params(
                path=path,
            )
            if not params:
                return None
            if path.startswith('gs://') and client:
                if not blobs and not isinstance(blobs, list):
                    blobs = OutputFileInternal.list_blobs(
                        bucket=params['bucket'],
                        prefix=params['blob_name'],
                        delimiter=params['delimiter'],
                        client=client,
                        versions=False,
                    )

                for blob in blobs:
                    if blob.name == params['blob_name']:
                        # .mt files present as folders on gcs so calculating checksums is not avail.
                        if params['file_extension'] != '.mt':
                            file_checksum = blob.crc32c  # pylint: disable=E1101
                            valid = True
                            size = blob.size  # pylint: disable=E1101
                            break

            return OutputFileInternal.from_db(
                **{
                    'path': path,
                    'basename': params['basename'],  # pylint: disable=E1101
                    'dirname': params['dirname'],  # pylint: disable=E1101
                    'nameroot': params['file_stem'],  # pylint: disable=E1101
                    'nameext': params['file_extension'],  # pylint: disable=E1101
                    'file_checksum': file_checksum,
                    'size': size,  # pylint: disable=E1101
                    # At the moment we don't have any meta data for outputs
                    'meta': None,
                    'valid': valid,
                    'secondary_files': None,
                }
            )
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

        # If the data is a string, it may not be a valid file object, so return it as is.
        if isinstance(data, str):
            return data

        # If the data is a list, it may not be a valid file object, so we should
        # reconstruct the JSON from this.
        def add_to_structure(current: dict, path: list[str], content: dict):
            """Helper function to add content to the correct place in the JSON structure."""

            # We ensure the necessary keys exist in the current dictionary before adding the content.
            for key in path[:-1]:
                if key.isdigit():
                    key = int(key)  # type: ignore [assignment]
                    if key not in current:
                        current[key] = {}
                    current = current[key]
                else:
                    if key not in current:
                        current[key] = {}
                    current = current[key]

            # We add the content to the final key in the current dictionary.
            final_key = path[-1]
            if final_key.isdigit():
                final_key = int(final_key)  # type: ignore [assignment]
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
            # We initialize the file_root dictionary to store the file information.
            file_root: dict = {}

            # If the file variable is a tuple, we assume it is a tuple of (file_obj, json_structure)
            if isinstance(file, tuple):
                file_obj, json_structure = file
                file_obj = file_obj.model_dump()
                fields = OutputFileInternal.model_fields.keys()  # type:ignore[attr-defined]

                # Populate the file_root dictionary with the fields from the file_obj.
                for field in fields:
                    file_root[field] = file_obj.get(field)

                # If json_structure is not None, we assume it is a list of strings representing the path to the desired JSON structure.
                if json_structure:
                    if isinstance(json_structure, str):
                        json_structure = json_structure.split('.')
                    path = json_structure

                    # We add the file_root dictionary to the root dictionary based on the path.
                    add_to_structure(root, path, file_root)
                else:
                    root.update(file_root)

            # If the file variable is not a tuple, we assume it is a string representing the output of the file.
            else:
                file_root['output'] = file
                if 'output' in root:
                    root['output'].append(file)
                else:
                    root['output'] = [file]

        return root


class OutputFile(BaseModel):
    """File model for external use"""

    id: int | None = None
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
