"""Repository for Google Cloud Storage data access."""

from cpg_utils.config import config_retrieve, dataset_path

from ..models import FileMetadata, FileType, FilePath
from ..adapters import StorageClient


class GCSDataAccess:
    """Layer for accessing Google Cloud Storage."""

    def __init__(self, dataset: str, gcp_project: str):
        """
        Initialize the data access layer.

        Args:
            dataset: Dataset name
            storage: Storage client adapter
        """
        self.dataset = dataset
        self.gcp_project = gcp_project
        self.storage = StorageClient(project=gcp_project)
        self.main_bucket = self.get_bucket_name(self.dataset, 'default')
        self.upload_bucket = self.get_bucket_name(self.dataset, 'upload')
        self.analysis_bucket = self.get_bucket_name(self.dataset, 'analysis')

    @staticmethod
    def get_bucket_name(dataset: str, category: str) -> str:
        """
        Get the bucket name for a dataset and category.

        Args:
            dataset: Dataset name
            category: Bucket category (e.g., 'upload', 'analysis', 'web')

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
        file_types: list[FileType],
        excluded_prefixes: tuple[str, ...] | None = None,
    ) -> list[FileMetadata]:
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

        return self.storage.find_blobs(
            bucket_name,
            file_extensions=tuple(extensions),
            excluded_prefixes=excluded_prefixes,
        )

    def validate_cram_files(self, cram_paths: list[FilePath]) -> list[str]:
        """
        Validate the existence of CRAM files in the bucket.

        Args:
            cram_paths: List of CRAM file paths
            file_extensions: Extensions to search for

        Returns:
            List of found file paths
        """
        assert all(
            p.path.bucket == self.main_bucket for p in cram_paths
        ), 'All CRAM paths must be in the main(|test) bucket'
        return self.storage.check_blobs(
            self.main_bucket, [p.path.blob for p in cram_paths]
        )

    def delete_blobs(self, bucket_name: str, blobs: list[str]):
        """
        Delete blobs from a GCS bucket.

        Args:
            bucket_name: Name of the bucket
            blobs: List of blob names to delete
        """
        self.storage.delete_blobs(bucket_name, blobs)
