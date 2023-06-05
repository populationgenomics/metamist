# pylint: disable=no-member
import logging
from collections import defaultdict
import csv
from datetime import datetime
from typing import Any

from cloudpathlib import AnyPath
from cpg_utils.cloud import get_path_components_from_gcp_path
from google.cloud import storage


class AuditHelper:
    """General helper class for bucket auditing"""

    LOCAL_PREFIX = '/'
    GCS_PREFIX = 'gs://'
    AZ_PREFIX = 'az://'

    def __init__(self):
        # gs specific
        self._gcs_client = None
        self.gcs_bucket_refs: dict[str, storage.Bucket] = {}

    async def file_size(self, filepath: str) -> int:
        """Get size of file in bytes"""
        return AnyPath(filepath).stat().st_size

    async def datetime_added(self, filepath: str) -> datetime | None:
        """Get the date and time the file was created"""
        if filepath.startswith('gs://'):
            # add a specific case for GCS as the AnyPath implementation calls
            # bucket.get_blob which triggers a read (which humans are not permitted)
            blob = await self.get_gcs_blob(filepath)
            return blob.time_created

        ctime = AnyPath(filepath).stat().st_ctime
        if not ctime:
            return None

        return datetime.utcfromtimestamp(ctime)

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

    async def get_gcs_blob(self, filepath: str) -> storage.Blob | None:
        """Convenience function for getting blob from fully qualified GCS path"""
        if not filepath.startswith(self.GCS_PREFIX):
            raise ValueError('No blob available')

        blob = storage.Blob.from_string(filepath, client=self.gcs_client)

        if not blob.exists():
            # maintain existing behaviour
            return None

        return blob

    @staticmethod
    def get_gcs_bucket_subdirs_to_search(paths: list[str]) -> defaultdict[str, list]:
        """
        Takes a list of paths and extracts the bucket name and subdirectory, returning all unique pairs
        of buckets/subdirectories
        """
        buckets_subdirs_to_search: defaultdict[str, list] = defaultdict(list)
        for path in paths:
            try:
                pc = get_path_components_from_gcp_path(path)
            except ValueError:
                logging.warning(f'{path} invalid')
                continue
            bucket = pc['bucket']
            subdir = pc['suffix']
            if subdir and subdir not in buckets_subdirs_to_search[bucket]:
                buckets_subdirs_to_search[bucket].append(subdir)

        return buckets_subdirs_to_search

    def get_gcs_paths_for_subdir(
        self, bucket_name: str, subdirectory: str, file_extension: tuple[str]
    ):
        """Iterate through a gcp bucket/subdir and get all the blobs with the specified file extension(s)"""
        files_in_bucket_subdir = []
        for blob in self.gcs_client.list_blobs(
            bucket_name, prefix=subdirectory, delimiter='/'
        ):
            # Check if file ends with specified analysis type
            if not blob.name.endswith(file_extension):
                continue
            files_in_bucket_subdir.append(f'gs://{bucket_name}/{blob.name}')

        return files_in_bucket_subdir

    def find_files_in_gcs_buckets_subdirs(
        self, buckets_subdirs: defaultdict[str, list], file_types: tuple[str]
    ):
        """
        Takes a list of (bucket,subdirectory) tuples and finds all the files contained in that directory
        with filetypes defined with an input list
        """
        files_in_bucket = []
        for bucket, subdirs in buckets_subdirs.items():
            for subdir in subdirs:
                # matrixtable / hailtable subdirectories should not appear in main-upload buckets,
                # but handle them just in case. These directories are too large to search.
                if '.mt' in subdir or '.ht' in subdir:
                    continue
                files_in_bucket.extend(
                    self.get_gcs_paths_for_subdir(bucket, subdir, file_types)
                )

        return files_in_bucket

    def find_sequence_files_in_gcs_bucket(
        self, bucket_name: str, file_extensions: tuple[str]
    ) -> list[str]:
        """Gets all the gs paths to fastq files in the datasets upload bucket"""
        if bucket_name.startswith('gs://'):
            bucket_name = bucket_name.removeprefix('gs://')
        sequence_paths = []
        if 'upload' not in bucket_name:
            # No prefix means it will get all blobs in the bucket (regardless of path)
            # This can be a dangerous call outside of the upload buckets
            raise NameError(
                'Call to list_blobs without prefix only valid for upload buckets'
            )

        for blob in self.gcs_client.list_blobs(bucket_name, prefix=''):
            if blob.name.endswith(file_extensions):
                sequence_paths.append(f'gs://{bucket_name}/{blob.name}')
            continue

        return sequence_paths

    @staticmethod
    def write_csv_report_to_cloud(
        data_to_write: list[Any], report_path: AnyPath, header_row: list[str] | None
    ):
        """
        Writes a csv report to the cloud bucket containing the data to write
        at the report path, with an optional header row
        """
        with AnyPath(report_path).open('w+') as f:  # pylint: disable=E1101
            writer = csv.writer(f)
            if header_row:
                writer.writerow(header_row)
            for row in data_to_write:
                if isinstance(row, str):
                    writer.writerow([row])
                    continue
                writer.writerow(row)

        logging.info(f'Wrote {len(data_to_write)} lines to report: {report_path}')
