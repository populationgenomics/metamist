# pylint: disable=no-member
import logging
from collections import defaultdict
from datetime import datetime

from cloudpathlib import AnyPath, GSPath
from cpg_utils.cloud import get_path_components_from_gcp_path
from google.cloud import storage


class AuditHelper:
    """General helper class for bucket auditing"""

    LOCAL_PREFIX = '/'
    GCS_PREFIX = 'gs://'
    AZ_PREFIX = 'az://'
    FASTQ_EXTENSIONS = ('.fq.gz', '.fastq.gz', '.fq', '.fastq')
    BAM_EXTENSIONS = ('.bam',)
    CRAM_EXTENSIONS = ('.cram',)
    GVCF_EXTENSIONS = ('.g.vcf.gz',)
    VCF_EXTENSIONS = ('.vcf', '.vcf.gz')
    READ_EXTENSIONS = FASTQ_EXTENSIONS + BAM_EXTENSIONS + CRAM_EXTENSIONS
    ALL_EXTENSIONS = (
        FASTQ_EXTENSIONS
        + BAM_EXTENSIONS
        + CRAM_EXTENSIONS
        + GVCF_EXTENSIONS
        + VCF_EXTENSIONS
    )

    FILE_TYPES_MAP = {
        'fastq': FASTQ_EXTENSIONS,
        'bam': BAM_EXTENSIONS,
        'cram': CRAM_EXTENSIONS,
        'gvcf': GVCF_EXTENSIONS,
        'vcf': VCF_EXTENSIONS,
        'all_reads': READ_EXTENSIONS,
        'all': ALL_EXTENSIONS,
    }

    SEQUENCE_TYPES_MAP = {
        'genome': [
            'genome',
        ],
        'exome': [
            'exome',
        ],
        'all': [
            'genome',
            'exome',
        ],
    }

    EXCLUDED_SAMPLES = [
        'CPG11783',  # acute-care, no FASTQ data
        'CPG13409',  # perth-neuro, coverage ~0x
        'CPG243717',  # validation, NA12878_KCCG low coverage https://main-web.populationgenomics.org.au/validation/qc/cram/multiqc.html,
        'CPG246645',  # ag-hidden, eof issue  https://batch.hail.populationgenomics.org.au/batches/97645/jobs/440
        'CPG246678',  # ag-hidden, diff fastq size  https://batch.hail.populationgenomics.org.au/batches/97645/jobs/446
        'CPG261792',  # rdp-kidney misformated fastq - https://batch.hail.populationgenomics.org.au/batches/378736/jobs/43
        # acute care fasq parsing errors https://batch.hail.populationgenomics.org.au/batches/379303/jobs/24
        'CPG259150',
        'CPG258814',
        'CPG258137',
        'CPG258111',
        'CPG258012',
        # ohmr4 cram parsing in align issues
        'CPG261339',
        'CPG261347',
        # IBMDX truncated sample? https://batch.hail.populationgenomics.org.au/batches/422181/jobs/99
        'CPG265876',
    ]

    def __init__(self):
        # gs specific
        self._gcs_client = None
        self.gcs_bucket_refs: dict[str, storage.Bucket] = {}

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

    async def file_contents(self, filepath: str) -> str | None:
        """Get contents of file (decoded as utf8)"""
        with AnyPath(filepath).open(encoding='utf-8-sig') as f:
            return f.read()

    async def file_exists(self, filepath: str) -> bool:
        """Determines whether a file exists"""
        if filepath.startswith('gs://'):
            # add a specific case for GCS as the AnyPath implementation calls
            # bucket.get_blob which triggers a read (which humans are not permitted)
            blob = await self.get_gcs_blob(filepath)
            return blob is not None

        return AnyPath(filepath).exists()

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

    def get_gcs_bucket_subdirs_to_search(
        self, paths: list[str]
    ) -> defaultdict[str, list]:
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

    def get_paths_for_subdir(
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

    def find_files_in_buckets_subdirs(
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
                    self.get_paths_for_subdir(bucket, subdir, file_types)
                )

        return files_in_bucket

    def find_sequence_files_in_bucket(
        self, bucket_name: str, file_extensions: tuple[str]
    ) -> list[str]:
        """Gets all the gs paths to fastq files in the projects upload bucket"""
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
