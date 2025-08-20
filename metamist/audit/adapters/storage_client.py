"""Google Cloud Storage client adapter."""

from typing import cast, List, Optional, Dict
from collections import defaultdict
from google.cloud import storage
from cpg_utils import to_path

from ..models import FileMetadata, FilePath


class StorageClient:
    """Adapter for Google Cloud Storage operations."""
    
    def __init__(self, project: Optional[str] = None):
        """
        Initialize the storage client.
        
        Args:
            project: GCP project ID
        """
        self.client = storage.Client(project=project)
        self.project = project
    
    def list_blobs(
        self,
        bucket_name: str,
        prefix: Optional[str] = None,
        file_extensions: Optional[tuple[str, ...]] = None,
        excluded_prefixes: Optional[tuple[str, ...]] = None
    ) -> List[FileMetadata]:
        """
        List blobs in a bucket with optional filtering.
        
        Args:
            bucket_name: Name of the bucket
            prefix: Optional prefix to filter blobs
            file_extensions: Optional tuple of file extensions to filter
            excluded_prefixes: Optional tuple of prefixes to exclude
            
        Returns:
            List of FileMetadata objects
        """
        bucket_name = bucket_name.removeprefix('gs://').removesuffix('/')
        bucket = self.client.bucket(bucket_name, user_project=self.project)
        
        files = []
        for item in self.client.list_blobs(bucket, prefix=prefix or ''):
            blob = cast(storage.Blob, item)
            # Skip if file doesn't match extensions
            if file_extensions and not blob.name.endswith(file_extensions):
                continue
            
            # Skip if file matches excluded prefixes
            if excluded_prefixes and any(
                blob.name.startswith(prefix) for prefix in excluded_prefixes
            ):
                continue
            
            file_path = FilePath(to_path(f'gs://{bucket_name}/{blob.name}'))
            files.append(
                FileMetadata(
                    filepath=file_path,
                    filesize=blob.size,
                    checksum=blob.crc32c
                )
            )
        
        return files
    
    def get_blob_metadata(
        self,
        bucket_name: str,
        blob_name: str
    ) -> Optional[FileMetadata]:
        """
        Get metadata for a specific blob.
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the blob
            
        Returns:
            FileMetadata object or None if blob doesn't exist
        """
        bucket_name = bucket_name.removeprefix('gs://').removesuffix('/')
        bucket = self.client.bucket(bucket_name, user_project=self.project)
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            return None
        
        blob.reload()
        file_path = FilePath(to_path(f'gs://{bucket_name}/{blob_name}'))
        
        return FileMetadata(
            filepath=file_path,
            filesize=blob.size,
            checksum=blob.crc32c
        )
    
    def batch_get_metadata(
        self,
        paths: List[str]
    ) -> List[FileMetadata]:
        """
        Get metadata for multiple files in batch.
        
        Args:
            paths: List of GCS paths
            
        Returns:
            List of FileMetadata objects
        """
        # Group paths by bucket for batch operations
        paths_by_bucket: Dict[str, List[str]] = defaultdict(list)
        
        for path_str in paths:
            path = to_path(path_str)
            paths_by_bucket[path.bucket].append(path.blob)
        
        results = []
        for bucket_name, blob_names in paths_by_bucket.items():
            bucket = self.client.bucket(bucket_name, user_project=self.project)
            
            for blob_name in blob_names:
                blob = bucket.blob(blob_name)
                try:
                    blob.reload()
                    file_path = FilePath(to_path(f'gs://{bucket_name}/{blob_name}'))
                    results.append(
                        FileMetadata(
                            filepath=file_path,
                            filesize=blob.size,
                            checksum=blob.crc32c
                        )
                    )
                except Exception:  # pylint: disable=broad-exception-caught
                    # Blob doesn't exist or error accessing it
                    continue
        
        return results
    
    def check_blob_exists(
        self,
        bucket_name: str,
        blob_name: str
    ) -> bool:
        """
        Check if a blob exists.
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the blob
            
        Returns:
            True if blob exists, False otherwise
        """
        bucket_name = bucket_name.removeprefix('gs://').removesuffix('/')
        bucket = self.client.bucket(bucket_name, user_project=self.project)
        blob = bucket.blob(blob_name)
        return blob.exists()
    
    def find_files_in_prefixes(
        self,
        bucket_prefixes: Dict[str, List[str]],
        file_extensions: tuple[str, ...]
    ) -> Dict[str, List[str]]:
        """
        Find files with specific extensions in multiple bucket/prefix combinations.
        
        Args:
            bucket_prefixes: Dictionary of bucket names to list of prefixes
            file_extensions: Tuple of file extensions to find
            
        Returns:
            Dictionary of prefixes to list of file paths found
        """
        files_by_prefix: Dict[str, List[str]] = {}
        
        # Skip hail table/matrix table/vds paths
        hail_extensions = ['.ht', '.mt', '.vds']
        
        for bucket_name, prefixes in bucket_prefixes.items():
            bucket = self.client.bucket(bucket_name, user_project=self.project)
            
            for prefix in prefixes:
                # Skip if prefix contains hail extensions
                if any(ext in prefix for ext in hail_extensions):
                    continue
                
                found_files = []
                for blob in self.client.list_blobs(bucket, prefix=f'{prefix}/'):
                    if blob.name.endswith(file_extensions):
                        found_files.append(f'gs://{bucket_name}/{blob.name}')
                
                files_by_prefix[prefix] = found_files
        
        return files_by_prefix
    
    def extract_bucket_and_prefixes(
        self,
        paths: List[str]
    ) -> Dict[str, List[str]]:
        """
        Extract unique bucket and prefix combinations from a list of paths.
        
        Args:
            paths: List of GCS paths
            
        Returns:
            Dictionary of bucket names to list of unique prefixes
        """
        buckets_prefixes: Dict[str, List[str]] = defaultdict(list)
        
        for path_str in paths:
            try:
                path = to_path(path_str)
            except ValueError:
                continue
            
            bucket = path.bucket
            # Get prefix (everything except the filename)
            prefix = '/'.join(path.parts[2:-1])  # Skip 'gs://', bucket, and filename
            
            if prefix and prefix not in buckets_prefixes[bucket]:
                buckets_prefixes[bucket].append(prefix)
        
        return dict(buckets_prefixes)

    def delete_blobs(
        self,
        paths: List[str],
    ):
        """
        Delete blobs from Google Cloud Storage.
        """
        for path in paths:
            self.delete_blob(path)

    def delete_blob(
        self,
        path: str,
    ):
        """
        Delete a blob from Google Cloud Storage.

        Args:
            path: The GCS path of the blob to delete.
        """
        path = to_path(path)
        bucket_name, blob_name = path.bucket, path.blob
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
