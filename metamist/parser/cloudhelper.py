# pylint: disable=no-member
import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import Callable, Iterable, TypeVar

from cloudpathlib import AnyPath, GSPath
from google.cloud import storage

# type declarations for GroupBy
T = TypeVar('T')
X = TypeVar('X')


def group_by(iterable: Iterable[T], selector: Callable[[T], X]) -> dict[X, list[T]]:
    """Simple group by implementation"""
    ret: dict[X, list[T]] = defaultdict(list)
    for k in iterable:
        ret[selector(k)].append(k)

    return dict(ret)


class CloudHelper:
    """General CloudHelper for parsers"""

    LOCAL_PREFIX = '/'
    GCS_PREFIX = 'gs://'
    AZ_PREFIX = 'az://'

    def __init__(self, search_paths: list[str] | None):
        # gs specific
        self._gcs_client = None
        self.gcs_bucket_refs: dict[str, storage.Bucket] = {}

        self.search_paths = search_paths or []

        self.filename_map: dict[str, str] = self.populate_filename_map(
            self.search_paths
        )
        # pylint: disable

    @staticmethod
    def guess_delimiter_from_filename(filename: str):
        """
        Guess delimiter from filename
        """
        extension_to_delimiter = {'.csv': ',', '.tsv': '\t'}
        relevant_delimiter = next(
            (
                delimiter
                for ext, delimiter in extension_to_delimiter.items()
                if filename.endswith(ext)
            ),
            None,
        )
        if relevant_delimiter:
            return relevant_delimiter

        raise ValueError(f'Unrecognised extension on file: {filename}')

    def file_path(self, filename: str, raise_exception: bool = True) -> str | None:
        """
        If the filename is a fully qualified path, this does NOT check for file existence
        """
        if any(
            filename.startswith(prefix)
            for prefix in (self.GCS_PREFIX, '/', self.AZ_PREFIX)
        ):
            return filename

        expanded_local_path = os.path.abspath(filename)
        if expanded_local_path.startswith('.'):
            expanded_local_path = os.path.abspath(filename)
        if os.path.exists(expanded_local_path):
            return expanded_local_path

        if not self.filename_map:
            raise AssertionError(
                'If no search path is provided, the file_path must be overridden'
            )

        if filename not in self.filename_map:
            if raise_exception:
                raise FileNotFoundError(
                    f'Could not find file {filename!r} in search paths'
                )
            return None

        return self.filename_map[filename]

    def list_directory(self, directory_name) -> list[str]:
        """List directory"""
        path = self.file_path(directory_name)

        # this was super slow to perform, do don't do this atm
        # return [os.path.join(path, p.name) for p in AnyPath(path).iterdir()]

        if path.startswith('gs://'):
            return self._list_gcs_directory(path)

        if path.startswith('/'):
            return [os.path.join(path, f) for f in os.listdir(path)]

        raise ValueError(f'Could not handle listing directory of {directory_name!r}')

    async def file_contents(self, filename) -> str | None:
        """Get contents of file (decoded as utf8)"""
        path = self.file_path(filename)
        with AnyPath(path).open(encoding='utf-8-sig') as f:
            return f.read()

    async def file_exists(self, filename: str) -> bool:
        """Determines whether a file exists"""
        path = self.file_path(filename)
        if path.startswith('gs://'):
            # add a specific case for GCS as the AnyPath implementation calls
            # bucket.get_blob which triggers a read (which humans are not permitted)
            blob = await self.get_gcs_blob(path)
            return blob is not None

        return AnyPath(path).exists()

    async def file_size(self, filename) -> int:
        """Get size of file in bytes"""
        return AnyPath(self.file_path(filename)).stat().st_size

    async def datetime_added(self, filename) -> datetime | None:
        """Get the date and time the file was created"""
        path = self.file_path(filename)
        if path.startswith('gs://'):
            # add a specific case for GCS as the AnyPath implementation calls
            # bucket.get_blob which triggers a read (which humans are not permitted)
            blob = await self.get_gcs_blob(path)
            return blob.time_created

        ctime = AnyPath(path).stat().st_ctime
        if not ctime:
            return None

        return datetime.utcfromtimestamp(ctime)

    def populate_filename_map(self, search_locations: list[str]) -> dict[str, str]:
        """
        FileMapParser uses search locations based on the filename,
        so let's prepopulate that filename_map from the search_locations!
        """

        fn_map: dict[str, str] = {}
        for directory in search_locations:
            directory_list = self.list_directory(directory)
            for file in directory_list:
                file = file.strip()
                file_base = os.path.basename(file)
                if file_base in fn_map:
                    logging.warning(
                        f'File {file!r} from {directory!r} already exists in directory map: {fn_map[file_base]}'
                    )
                    continue
                fn_map[file_base] = file

        return fn_map

    # GCS specific methods
    @property
    def gcs_client(self) -> storage.Client:
        """Get GCP storage client"""
        if not self._gcs_client:
            self._gcs_client = storage.Client()
        return self._gcs_client

    def get_gcs_bucket(self, bucket_name: str) -> storage.Bucket:
        """Get cached bucket client from optional bucket name"""
        assert bucket_name
        if bucket_name not in self.gcs_bucket_refs:
            self.gcs_bucket_refs[bucket_name] = self.gcs_client.get_bucket(bucket_name)

        return self.gcs_bucket_refs[bucket_name]

    async def get_gcs_blob(self, filename: str) -> storage.Blob | None:
        """Convenience function for getting blob from fully qualified GCS path"""
        if not filename.startswith(self.GCS_PREFIX):
            raise ValueError('No blob available')

        blob = storage.Blob.from_string(filename, client=self.gcs_client)

        if not blob.exists():
            # maintain existing behaviour
            return None

        if not blob.time_created:
            # GCP blobs sometimes need a reload
            blob.reload()

        return blob

    def _list_gcs_directory(self, gcs_path) -> list[str]:
        path = GSPath(gcs_path)
        if path.parts[2:]:
            remaining_path = '/'.join(path.parts[2:]) + '/'  # has to end with "/"
        else:
            remaining_path = None
        blobs = self.gcs_client.list_blobs(
            path.bucket, prefix=remaining_path, delimiter='/'
        )
        return [f'gs://{path.bucket}/{blob.name}' for blob in blobs]
