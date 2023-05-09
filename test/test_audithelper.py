import unittest
from collections import defaultdict
from cpg_utils import cloud
from audit.audithelper import AuditHelper


def gcs_list_blobs(prefix):
    """Mocks the result of the gcs client list_blobs method"""
    transfer_blobs = [
        'transfer/data.cram',
    ]
    transfer2_blobs = [
        'transfer/transfer2/data2.cram',
    ]
    transfer3_blobs = [
        'transfer/transfer2/transfer3/data3.cram',
    ]
    transfer4_blobs = [
        'transfer4/data4.cram',
    ]
    seqdir_blobs = [
        'sequences/read_R1.fastq',
        'sequences/read_R2.fastq.gz',
        'sequences/mainfest.txt',
    ]

    if prefix == 'transfer/':
        blob_names = transfer_blobs
    elif prefix == 'transfer/transfer2/':
        blob_names = transfer2_blobs
    elif prefix == 'transfer/transfer2/transfer3/':
        blob_names = transfer3_blobs
    elif prefix == 'transfer4/':
        blob_names = transfer4_blobs
    elif prefix == 'sequences/':
        blob_names = seqdir_blobs

    return blob_names


class TestAuditHelper(unittest.TestCase):
    """Test the audit helper functions"""

    def test_get_gcs_bucket_subdirs_to_search(self):
        """
        Takes a list of paths and extracts the bucket name and subdirectory, returning all unique pairs
        of buckets/subdirectories
        """
        paths = [
            'gs://cpg-dataset-main-upload/transfer/data.cram',
            'gs://cpg-dataset-main-upload/transfer/transfer2/data2.cram',
            'gs://cpg-dataset-main-upload/transfer/transfer2/transfer3/data3.cram',
            'gs://cpg-dataset-main-upload/transfer4/data4.cram',
            'gs://cpg-dataset-main-upload/data5.cram',
        ]

        buckets_subdirs_to_search: defaultdict[str, list] = defaultdict(list)
        for path in paths:
            try:
                pc = cloud.get_path_components_from_gcp_path(path)
            except ValueError:
                # logging.warning(f'{path} invalid')
                continue
            bucket = pc['bucket']
            subdir = pc['suffix']
            if subdir and subdir not in buckets_subdirs_to_search[bucket]:
                buckets_subdirs_to_search[bucket].append(subdir)

        expected_result = defaultdict(list)
        expected_result['cpg-dataset-main-upload'] = [
            'transfer/',
            'transfer/transfer2/',
            'transfer/transfer2/transfer3/',
            'transfer4/',
        ]

        self.assertDictEqual(buckets_subdirs_to_search, expected_result)

    def test_get_gcs_paths_for_subdir(self):
        """Iterate through a gcp bucket/subdir and get all the blobs with the specified file extension(s)"""
        bucket_name = 'cpg-dataset-main-upload'
        subdir1 = 'transfer/'
        subdir2 = 'transfer/transfer2/'
        subdir3 = 'transfer/transfer2/transfer3/'
        subdir4 = 'transfer4/'

        files_in_bucket_subdir = []
        for subdir in [subdir1, subdir2, subdir3, subdir4]:
            for blob_name in gcs_list_blobs(prefix=subdir):
                # Check if file ends with specified analysis type
                if not blob_name.endswith('cram'):
                    continue
                files_in_bucket_subdir.append(f'gs://{bucket_name}/{blob_name}')

        expected_results = [
            'gs://cpg-dataset-main-upload/transfer/data.cram',
            'gs://cpg-dataset-main-upload/transfer/transfer2/data2.cram',
            'gs://cpg-dataset-main-upload/transfer/transfer2/transfer3/data3.cram',
            'gs://cpg-dataset-main-upload/transfer4/data4.cram',
        ]

        self.assertEqual(files_in_bucket_subdir, expected_results)

    def test_find_sequence_files_in_gcs_bucket(self):
        """Gets all the gs paths to sequence files of a type (e.g., fastqs) in the project's upload bucket"""
        bucket_name = 'cpg-dataset-main-upload'
        file_extensions = ('.fq.gz', '.fastq.gz', '.fq', '.fastq')

        sequence_paths = []
        if 'upload' not in bucket_name:
            # No prefix means it will get all blobs in the bucket (regardless of path)
            # This can be a dangerous call outside of the upload buckets
            raise NameError(
                'Call to list_blobs without prefix only valid for upload buckets'
            )

        with self.assertRaises(NameError):
            AuditHelper.find_sequence_files_in_gcs_bucket(
                self, bucket_name='cpg-dataset-main', file_extensions=('test',)
            )

        for blob_name in gcs_list_blobs(prefix='sequences/'):
            if blob_name.endswith(file_extensions):
                sequence_paths.append(f'gs://{bucket_name}/{blob_name}')
            continue

        expected_result = [
            'gs://cpg-dataset-main-upload/sequences/read_R1.fastq',
            'gs://cpg-dataset-main-upload/sequences/read_R2.fastq.gz',
        ]

        self.assertEqual(sequence_paths, expected_result)
