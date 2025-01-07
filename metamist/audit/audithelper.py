# pylint: disable=no-member
import csv
import logging
from collections import defaultdict
from io import StringIO
from typing import Any

from cpg_utils.cloud import get_path_components_from_gcp_path
from cpg_utils.config import config_retrieve, get_gcp_project

from metamist.graphql import gql, query
from metamist.parser.cloudhelper import CloudHelper

from google.cloud import storage

FASTQ_EXTENSIONS = ('.fq.gz', '.fastq.gz', '.fq', '.fastq')
BAM_EXTENSIONS = ('.bam',)
CRAM_EXTENSIONS = ('.cram',)
READ_EXTENSIONS = FASTQ_EXTENSIONS + BAM_EXTENSIONS + CRAM_EXTENSIONS
GVCF_EXTENSIONS = ('.g.vcf.gz',)
VCF_EXTENSIONS = ('.vcf', '.vcf.gz')
ARCHIVE_EXTENSIONS = ('.tar', '.tar.gz', '.zip')
ALL_EXTENSIONS = (
    FASTQ_EXTENSIONS
    + BAM_EXTENSIONS
    + CRAM_EXTENSIONS
    + GVCF_EXTENSIONS
    + VCF_EXTENSIONS
    + ARCHIVE_EXTENSIONS
)

FILE_TYPES_MAP = {
    'fastq': FASTQ_EXTENSIONS,
    'bam': BAM_EXTENSIONS,
    'cram': CRAM_EXTENSIONS,
    'all_reads': READ_EXTENSIONS,
    'gvcf': GVCF_EXTENSIONS,
    'vcf': VCF_EXTENSIONS,
    'archive': ARCHIVE_EXTENSIONS,
    'all': ALL_EXTENSIONS,
}

HAIL_EXTENSIONS = ['.ht', '.mt', '.vds']

ENUMS_QUERY = gql(
    """
    query analysisTypes {
        enum {
            analysisType
            sequencingType
        }
    }
    """
)


def get_enums(enum_name) -> list[str]:
    """
    Return the list of allowed values of a particular enum type from the enum table.
    Used to get the list of allowed analysis types and sequencing types.
    """
    enums_query_result = query(ENUMS_QUERY)
    return enums_query_result['enum'][enum_name]


class AuditHelper(CloudHelper):
    """General helper class for bucket auditing"""

    def __init__(
        self,
        gcp_project: str,
        all_analysis_types: list[str] = None,
        all_sequencing_types: list[str] = None,
        excluded_sequencing_groups: list[str] = None,
    ):
        super().__init__(search_paths=None)

        # GCP project is used for GCP calls, which the auditor does a lot of
        self.gcp_project = (
            gcp_project
            or get_gcp_project()
            or config_retrieve(['workflow', 'gcp_project'])
        )
        if not self.gcp_project:
            raise ValueError('GCP project is required')

        self.all_analysis_types = all_analysis_types or get_enums('analysisType')
        self.all_sequencing_types = all_sequencing_types or get_enums('sequencingType')

        self.excluded_sequencing_groups = excluded_sequencing_groups or config_retrieve(
            ['workflow', 'audit', 'excluded_sequencing_groups']
        )

    @staticmethod
    def get_gcs_buckets_and_prefixes_from_paths(
        paths: list[str],
    ) -> defaultdict[str, list]:
        """
        Takes a list of paths and extracts the bucket names and prefix, returning all unique pairs
        of buckets and prefixes. Does not make any calls to GCS.
        """
        buckets_prefixes: defaultdict[str, list] = defaultdict(list)
        for path in paths:
            try:
                pc = get_path_components_from_gcp_path(path)
            except ValueError:
                logging.warning(f'{path} invalid')
                continue
            bucket = pc['bucket']
            prefix = pc[
                'suffix'
            ]  # This is the prefix (i.e. the "subdirectory" in the bucket)
            if prefix and prefix not in buckets_prefixes[bucket]:
                buckets_prefixes[bucket].append(prefix)

        return buckets_prefixes

    def get_all_files_in_gcs_bucket_with_prefix_and_extensions(
        self, bucket_name: str, prefix: str, file_extension: tuple[str]
    ):
        """Iterate through a gcp bucket/prefix and get all the blobs with the specified file extension(s)"""
        bucket = self.gcs_client.bucket(bucket_name, user_project=self.user_project)

        files_in_bucket_prefix = []
        for blob in self.gcs_client.list_blobs(bucket, prefix=prefix, delimiter='/'):
            # Check if file ends with specified analysis type
            if not blob.name.endswith(file_extension):
                continue
            files_in_bucket_prefix.append(f'gs://{bucket_name}/{blob.name}')

        return files_in_bucket_prefix

    def find_files_in_gcs_buckets_prefixes(
        self, buckets_prefixes: defaultdict[str, list[str]], file_types: tuple[str]
    ):
        """
        Takes a dict of {bucket: [prefix1, prefix2, ...]} tuples and finds all the files contained in that bucket/prefix
        that end with the specified file type extensions. Skips hailtable, matrixtable, and vds paths.
        """
        files_in_bucket = []
        for bucket_name, prefixes in buckets_prefixes.items():
            for prefix in prefixes:
                # matrixtable / hailtable / vds prefix should not appear in main-upload buckets,
                # but handle them just in case. These directories are too large to search.
                if any(hl_extension in prefix for hl_extension in HAIL_EXTENSIONS):
                    continue
                files_in_bucket.extend(
                    self.get_all_files_in_gcs_bucket_with_prefix_and_extensions(
                        bucket_name, prefix, file_types
                    )
                )

        return files_in_bucket

    def find_assay_files_in_gcs_bucket(
        self, bucket_name: str, file_extensions: tuple[str]
    ) -> dict[str, int]:
        """
        Gets all the paths and sizes to assay files in the dataset's upload bucket.
        Calls list_blobs on the bucket with the specified file extensions, returning a dict of paths and sizes.
        """
        bucket_name = bucket_name.removeprefix('gs://').removesuffix('/')
        if 'upload' not in bucket_name:
            # No prefix means it will get all blobs in the bucket (regardless of path)
            # This can be a dangerous call outside of the upload buckets
            raise NameError(
                'Call to list_blobs without prefix only valid for upload buckets'
            )
        bucket = self.gcs_client.bucket(bucket_name, user_project=self.user_project)

        assay_paths_sizes = {}
        for blob in self.gcs_client.list_blobs(bucket, prefix=''):
            if not blob.name.endswith(file_extensions):
                continue
            blob.reload()
            assay_paths_sizes[blob.name] = blob.size

        return assay_paths_sizes

    def get_audit_report_prefix(
        self,
        seq_types: str,
        file_types: str,
    ):
        """Get the prefix for the report file based on the sequencing and file types audited"""
        if set(seq_types) == set(self.all_sequencing_types):
            sequencing_types_str = 'all_seq_types'
        else:
            sequencing_types_str = ('_').join(self.sequencing_types) + '_seq_types'

        if set(file_types) == set(ALL_EXTENSIONS):
            file_types_str = 'all_file_types'
        elif set(file_types) == set(READ_EXTENSIONS):
            file_types_str = 'all_reads_file_types'
        else:
            file_types_str = ('_').join(self.file_types) + '_file_types'

        return f'{file_types_str}_{sequencing_types_str}'

    def write_report_to_cloud(
        self,
        data_to_write: list[dict[str, Any]] | None,
        bucket_name: str,
        blob_path: str,
    ) -> None:
        """
        Writes a CSV/TSV report directly to Google Cloud Storage.

        Args:
            data_to_write: List of data rows to write to the CSV/TSV
            bucket_name: Name of the GCS bucket
            blob_path: Path where the blob should be stored in the bucket (with either .tsv or .csv extension)

        Raises:
            ValueError: If the blob path doesn't end with .csv or .tsv
            google.cloud.exceptions.NotFound: If the bucket doesn't exist
            google.cloud.exceptions.Forbidden: If permissions are insufficient
        """
        if not data_to_write:
            logging.info('No data to write to report')
            return

        logging.info(f'Writing report to gs://{bucket_name}/{blob_path}')

        # Create a string buffer to hold the data
        if blob_path.endswith('.csv'):
            delimiter = ','
            content_type = 'text/csv'
        elif blob_path.endswith('.tsv'):
            delimiter = '\t'
            content_type = 'text/tab-separated-values'
        else:
            raise ValueError('Blob path must end with either .csv or .tsv')

        buffer = StringIO()
        writer = csv.DictWriter(
            buffer, fieldnames=data_to_write[0].keys(), delimiter=delimiter
        )

        writer.writeheader()
        writer.writerows(data_to_write)

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name, user_project=self.user_project)
        blob = bucket.blob(blob_path)

        # Upload the TSV content
        blob.upload_from_string(
            buffer.getvalue(),
            content_type=content_type,
        )

        buffer.close()
        logging.info(
            f'Wrote {len(data_to_write)} lines to gs://{bucket_name}/{blob_path}'
        )
        return
