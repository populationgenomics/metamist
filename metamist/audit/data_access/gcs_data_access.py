"""Repository for Google Cloud Storage data access."""

from typing import List, Optional, Dict
from cpg_utils import to_path
from cpg_utils.config import config_retrieve, dataset_path

from ..models import FileMetadata, FileType
from ..adapters import StorageClient


class GCSDataAccess:
    """Layer for accessing Google Cloud Storage."""

    def __init__(self, storage_client: StorageClient):
        """
        Initialize the data access layer.

        Args:
            storage_client: Storage client adapter
        """
        self.storage_client = storage_client

    @staticmethod
    def get_bucket_name(dataset: str, category: str) -> str:
        """
        Get the bucket name for a dataset and category.

        Args:
            dataset: Dataset name
            category: Bucket category (e.g., 'upload', 'main')

        Returns:
            Bucket name
        """
        test = config_retrieve(['workflow', 'access_level']) == 'test'
        bucket: str = dataset_path(
            suffix='', dataset=dataset, category=category, test=test
        )
        if not bucket:
            raise ValueError(
                f'No bucket found for dataset {dataset} and category {category}'
            )
        return bucket.removeprefix('gs://').removesuffix('/')

    def list_files_in_bucket(
        self,
        bucket_name: str,
        file_types: List[FileType],
        excluded_prefixes: Optional[tuple[str, ...]] = None,
    ) -> List[FileMetadata]:
        """
        List files in a bucket with specified file types.

        Args:
            bucket_name: Name of the bucket
            file_types: List of file types to filter
            excluded_prefixes: Optional prefixes to exclude

        Returns:
            List of FileMetadata objects
        """
        # Combine all extensions from file types
        extensions = []
        for file_type in file_types:
            extensions.extend(file_type.extensions)

        return self.storage_client.list_blobs(
            bucket_name,
            file_extensions=tuple(extensions),
            excluded_prefixes=excluded_prefixes,
        )

    def check_file_exists(self, file_path: str) -> bool:
        """
        Check if a file exists in GCS.

        Args:
            file_path: GCS path to check

        Returns:
            True if file exists, False otherwise
        """
        try:
            path = to_path(file_path)
            return self.storage_client.check_blob_exists(path.bucket, path.blob)
        except (ValueError, AttributeError):
            return False

    def check_files_exist(self, file_paths: List[str]) -> Dict[str, bool]:
        """
        Check if multiple files exist in GCS.

        Args:
            file_paths: List of GCS paths to check

        Returns:
            Dictionary mapping file paths to existence status
        """
        results = {}
        for file_path in file_paths:
            results[file_path] = self.check_file_exists(file_path)
        return results

    def find_files_in_prefixes(
        self, cram_paths: List[str], file_extensions: tuple[str, ...] = ('.cram',)
    ) -> List[str]:
        """
        Find files with specific extensions in the prefixes of given paths.

        Args:
            cram_paths: List of CRAM file paths
            file_extensions: Extensions to search for

        Returns:
            List of found file paths
        """
        # Extract bucket and prefixes from paths
        bucket_prefixes = self.storage_client.extract_bucket_and_prefixes(cram_paths)

        # Find files in those prefixes
        files_by_prefix = self.storage_client.find_files_in_prefixes(
            bucket_prefixes, file_extensions
        )

        # Flatten the results
        all_files = []
        for files in files_by_prefix.values():
            all_files.extend(files)

        return all_files

    def get_file_metadata(self, file_path: str) -> Optional[FileMetadata]: # Unused
        """
        Get metadata for a single file.

        Args:
            file_path: GCS path

        Returns:
            FileMetadata object or None if file doesn't exist
        """
        try:
            path = to_path(file_path)
            return self.storage_client.get_blob_metadata(path.bucket, path.blob)
        except (ValueError, AttributeError):
            return None

    def batch_get_metadata(self, file_paths: List[str]) -> List[FileMetadata]:
        """
        Get metadata for multiple files.

        Args:
            file_paths: List of GCS paths

        Returns:
            List of FileMetadata objects
        """
        return self.storage_client.batch_get_metadata(file_paths)
