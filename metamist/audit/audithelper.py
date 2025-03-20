# pylint: disable=no-member
import csv
import logging
from collections import defaultdict
from io import StringIO

from cpg_utils.config import config_retrieve, dataset_path, get_gcp_project

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
    query enumsQuery {
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


# Variable type definitions
AnalysisId = int
AssayId = int
ParticipantId = int
ParticipantExternalId = str
SampleId = str
SampleExternalId = str
SequencingGroupId = str

handler = logging.StreamHandler()
formatter = logging.Formatter(
    fmt='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False


class AuditReportEntry:  # pylint: disable=too-many-instance-attributes
    """Class to hold the data for an audit report entry"""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        filepath: str | None = None,
        filesize: int | None = None,
        sg_id: str | None = None,
        assay_id: int | None = None,
        cram_analysis_id: int | None = None,
        cram_file_path: str | None = None,
        sample_id: str | None = None,
        sample_external_id: str | None = None,
        participant_id: int | None = None,
        participant_external_id: str | None = None,
    ):
        self.filepath = filepath
        self.filesize = filesize
        self.sg_id = sg_id
        self.assay_id = assay_id
        self.cram_analysis_id = cram_analysis_id
        self.cram_file_path = cram_file_path
        self.sample_id = sample_id
        self.sample_external_id = sample_external_id
        self.participant_id = participant_id
        self.participant_external_id = participant_external_id

    def __repr__(self) -> str:
        return f'AuditReportEntry({self.__dict__})'

    def to_report_dict(self) -> dict:
        """Returns a dictionary with custom field names for the report"""
        return {
            'File Path': self.filepath,
            'File Size': self.filesize,
            'SG ID': self.sg_id,
            'Assay ID': self.assay_id,
            'Sample ID': self.sample_id,
            'Sample External ID': self.sample_external_id,
            'Participant ID': self.participant_id,
            'Participant External ID': self.participant_external_id,
            'CRAM Analysis ID': self.cram_analysis_id,
            'CRAM Path': self.cram_file_path,
        }


class ParticipantData:
    """Class to hold the data for a participant"""

    def __init__(
        self,
        id_: ParticipantId,
        external_id: ParticipantExternalId,
    ):
        self.id = id_
        self.external_id = external_id


class SampleData:
    """Class to hold the data for a sample"""

    def __init__(
        self,
        id_: SampleId,
        external_id: SampleExternalId,
        participant: ParticipantData,
    ):
        self.id = id_
        self.external_id = external_id
        self.participant = participant


class ReadFileData:
    """Class to hold the data for a read file"""

    def __init__(
        self,
        filepath: str,
        filesize: int,
        checksum: str | None = None,
    ):
        self.filepath = filepath
        self.filesize = filesize
        self.checksum = checksum


class AssayData:
    """Class to hold the data for an assay"""

    def __init__(
        self,
        id_: AssayId,
        read_files: list[ReadFileData],
        sample: SampleData,
    ):
        self.id = id_
        self.read_files = read_files
        self.sample = sample


class SequencingGroupData:
    """Class to hold the data for a sequencing group"""

    def __init__(
        self,
        id_: str,
        sequencing_type: str,
        sequencing_technology: str,
        sample: SampleData,
        assays: list[AssayData],
        cram_analysis_id: int | None = None,
        cram_file_path: str | None = None,
    ):
        self.id = id_
        self.sequencing_type = sequencing_type
        self.sequencing_technology = sequencing_technology
        self.sample = sample
        self.assays = assays
        self.cram_analysis_id = cram_analysis_id
        self.cram_file_path = cram_file_path


class AuditHelper(CloudHelper):
    """General helper class for bucket auditing"""

    def __init__(
        self,
        gcp_project: str = None,
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
            ['metamist', 'audit', 'excluded_sequencing_groups'], default=[]
        )

    def validate_dataset(self, dataset: str) -> str:
        """Validate the input dataset"""
        if not dataset:
            dataset = config_retrieve(['workflow', 'dataset'])
        if not dataset:
            raise ValueError('Metamist dataset is required')
        return dataset

    def validate_sequencing_types(self, sequencing_types: list[str]) -> list[str]:
        """Validate the input sequencing types"""
        if sequencing_types == ('all',):
            return self.all_sequencing_types
        invalid_types = [
            st for st in sequencing_types if st not in self.all_sequencing_types
        ]
        if invalid_types:
            raise ValueError(
                f'Input sequencing types "{invalid_types}" must be in the allowed types: {self.all_sequencing_types}'
            )
        return sequencing_types

    def validate_file_types(self, file_types: tuple[str]) -> tuple[str]:
        """Validate the input file types"""
        if file_types in (('all',), ('all_reads',)):
            return FILE_TYPES_MAP[file_types[0]]
        invalid_file_types = [ft for ft in file_types if ft not in FILE_TYPES_MAP]
        if invalid_file_types:
            raise ValueError(
                f'Input file types "{invalid_file_types}" must be in the allowed types: {", ".join(FILE_TYPES_MAP.keys())}'
            )
        return file_types

    def get_bucket_name(self, dataset: str, category: str) -> str:
        """Get the bucket name for the given dataset and category"""
        test = config_retrieve(['workflow', 'access_level']) == 'test'
        bucket: str = dataset_path(
            suffix='', dataset=dataset, category=category, test=test
        )
        if not bucket:
            raise ValueError(
                f'No bucket found for dataset {dataset} and category {category}'
            )
        return bucket.removeprefix('gs://').removesuffix('/')

    def get_sequencing_group_data_by_id(
        self,
        sg_id: str,
        sequencing_groups: list[SequencingGroupData],
    ):
        """Get the sequencing group data for a given sg_id"""
        for sg in sequencing_groups:
            if sg.id == sg_id:
                return sg
        return None

    def get_gcs_buckets_and_prefixes_from_paths(
        self,
        paths: list[str] | set[str],
    ) -> defaultdict[str, list]:
        """
        Takes a list of paths and extracts the bucket names and prefix, returning all unique pairs
        of buckets and prefixes. Does not make any calls to GCS.
        """
        buckets_prefixes: defaultdict[str, list] = defaultdict(list)
        for path in paths:
            try:
                pc = self.get_path_components_from_gcp_path(path)
            except ValueError:
                logger.warning(f'{path} invalid')
                continue
            bucket = pc['bucket']
            prefix = pc['prefix']
            if prefix and prefix not in buckets_prefixes[bucket]:
                buckets_prefixes[bucket].append(prefix)

        return buckets_prefixes

    def get_all_files_in_gcs_bucket_with_prefix_and_extensions(
        self, bucket_name: str, prefix: str, file_extensions: tuple[str]
    ):
        """Iterate through a gcp bucket/prefix and get all the blobs with the specified file extension(s)"""
        bucket = self.gcs_client.bucket(bucket_name, user_project=self.gcp_project)

        files_in_bucket_prefix = []
        for blob in self.gcs_client.list_blobs(bucket, prefix=prefix, delimiter='/'):
            # If specified, check if the file ends with a valid extension
            if file_extensions and not blob.name.endswith(file_extensions):
                continue
            files_in_bucket_prefix.append(f'gs://{bucket_name}/{blob.name}')

        return files_in_bucket_prefix

    def find_files_in_gcs_buckets_prefixes(
        self,
        buckets_prefixes: defaultdict[str, list[str]],
        file_types: tuple[str] | None,
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

    def get_read_file_blobs_in_gcs_bucket(
        self, bucket_name: str, file_extensions: tuple[str]
    ) -> list[ReadFileData]:
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
        bucket = self.gcs_client.bucket(bucket_name, user_project=self.gcp_project)

        read_files = []
        for blob in self.gcs_client.list_blobs(bucket, prefix=''):
            if not blob.name.endswith(file_extensions):
                continue
            blob.reload()
            read_files.append(
                ReadFileData(
                    filepath=f'gs://{bucket_name}/{blob.name}',
                    filesize=blob.size,
                    checksum=blob.crc32c,
                )
            )

        return read_files

    def get_audit_report_prefix(
        self,
        seq_types: str,
        file_types: str,
    ):
        """Get the prefix for the report file based on the sequencing and file types audited"""
        if set(seq_types) == set(self.all_sequencing_types):
            sequencing_types_str = 'all_seq_types'
        else:
            sequencing_types_str = ('_').join(seq_types) + '_seq_types'

        if set(file_types) == set(ALL_EXTENSIONS):
            file_types_str = 'all_file_types'
        elif set(file_types) == set(READ_EXTENSIONS):
            file_types_str = 'all_reads_file_types'
        else:
            file_types_str = ('_').join(file_types) + '_file_types'

        return f'{file_types_str}_{sequencing_types_str}'

    def get_audit_report_records_from_sgs(
        self,
        sequencing_groups: list[SequencingGroupData],
    ) -> list[AuditReportEntry]:
        """
        Get the audit report records from the sequencing group data.
        """
        audit_report_records = []
        for sg in sequencing_groups:
            for assay in sg.assays:
                audit_report_records.append(
                    AuditReportEntry(
                        sg_id=sg.id,
                        assay_id=assay.id,
                        cram_analysis_id=sg.cram_analysis_id,
                        cram_file_path=sg.cram_file_path,
                        sample_id=sg.sample.id,
                        sample_external_id=sg.sample.external_id,
                        participant_id=sg.sample.participant.id,
                        participant_external_id=sg.sample.participant.external_id,
                    )
                )

        return audit_report_records

    def write_report_to_cloud(
        self,
        data_to_write: list[AuditReportEntry] | None,
        bucket_name: str,
        blob_path: str,
        report_name: str = None,
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
        if not report_name:
            report_name = blob_path

        if not data_to_write:
            logger.info(f'{report_name} - SKIPPED - No data to write\n')
            return

        logger.info(
            f'{report_name} - Writing report to gs://{bucket_name}/{blob_path}...'
        )

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
            buffer, fieldnames=data_to_write[0].to_report_dict(), delimiter=delimiter
        )

        rows_to_write = [entry.to_report_dict() for entry in data_to_write]
        writer.writeheader()
        writer.writerows(rows_to_write)

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name, user_project=self.gcp_project)
        blob = bucket.blob(blob_path)

        # Upload the TSV content
        blob.upload_from_string(
            buffer.getvalue(),
            content_type=content_type,
        )

        buffer.close()
        logger.info(f'Wrote {len(rows_to_write)} lines.\n')
        return
