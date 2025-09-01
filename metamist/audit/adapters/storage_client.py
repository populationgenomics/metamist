"""Google Cloud Storage client adapter."""

from io import StringIO
from typing import cast
from google.cloud import storage
from cpg_utils import to_path

from ..models import FileMetadata, FilePath


class StorageClient:
    """Adapter for Google Cloud Storage operations."""

    def __init__(self, project: str | None = None):
        """
        Initialize the storage client.

        Args:
            project: GCP project ID
        """
        self.client = storage.Client(project=project)
        self.project = project

    def get_bucket(self, bucket_name: str) -> storage.Bucket:
        """
        Get a bucket by name.
        """
        return self.client.get_bucket(bucket_name)

    def get_blob(self, bucket: str | storage.Bucket, blob_name: str) -> storage.Blob:
        """
        Wrapper for bucket.blob() that refreshes the blob
        if it exists.

        Args:
            bucket: The bucket containing the blob.
            blob_name: The name of the blob.

        Returns:
            The Blob object, refreshed if it exists.
        """
        if isinstance(bucket, str):
            bucket = self.get_bucket(bucket)

        blob = bucket.blob(blob_name)
        if blob.exists():
            blob.reload()
        return blob

    def check_blobs(
        self,
        bucket_name: str,
        blobs: list[str],
    ) -> list[str]:
        """
        Check the existence of blobs in a bucket.

        Args:
            bucket: Name of the bucket
            blobs: List of blob names to check
        """
        prefixes = set()
        for blob_name in blobs:
            prefixes.add(blob_name.rsplit('/', 1)[0])
        bucket_blobs = self.find_blobs(bucket_name, prefixes)
        return [
            blob for blob in blobs if blob in [b.filepath.name for b in bucket_blobs]
        ]

    def find_blobs(
        self,
        bucket_name: str,
        prefixes: set[str] | None = None,
        file_extensions: tuple[str] | None = None,
        excluded_prefixes: tuple[str] | None = None,
    ) -> list[FileMetadata]:
        """
        List blobs in a bucket with optional filtering.

        Args:
            bucket_name: Name of the bucket
            prefixes: Optional prefixes to filter blobs
            file_extensions: Optional tuple of file extensions to filter
            excluded_prefixes: Optional tuple of prefixes to exclude

        Returns:
            List of FileMetadata objects
        """
        bucket = self.get_bucket(bucket_name)

        files = []
        for prefix in prefixes:
            for item in self.client.list_blobs(
                bucket, prefix=prefix + ('/' if not prefix.endswith('/') else '')
            ):
                blob = cast(storage.Blob, item)
                # Skip if file doesn't match extensions
                if file_extensions and not blob.name.endswith(file_extensions):
                    continue

                # Skip if file matches excluded prefixes
                if excluded_prefixes and any(
                    blob.name.startswith(prefix) for prefix in excluded_prefixes
                ):
                    continue

                files.append(
                    FileMetadata(
                        filepath=FilePath(to_path(f'gs://{bucket_name}/{blob.name}')),
                        filesize=blob.size,
                        checksum=blob.crc32c,
                    )
                )

        return files

    def delete_blobs(
        self,
        bucket: str | storage.Bucket,
        blob_names: list[str],
    ):
        """
        Delete multiple blobs from Google Cloud Storage.

        Args:
            bucket: The bucket containing the blobs.
            blob_names: List of blob names to delete.
        """
        if isinstance(bucket, str):
            bucket = self.get_bucket(bucket)

        blobs = [bucket.blob(name) for name in blob_names]
        bucket.delete_blobs(blobs)

    def upload_from_buffer(
        self,
        blob: storage.Blob,
        buffer: StringIO,
        content_type: str = 'text/plain',
    ):
        """Upload a StringIO buffer to GCS."""
        blob.upload_from_string(buffer.getvalue(), content_type=content_type)
        buffer.close()
