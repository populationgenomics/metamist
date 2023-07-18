# pylint: disable=no-member
import csv
from collections import defaultdict
import logging
from typing import Any

from cloudpathlib import AnyPath
from cpg_utils.cloud import get_path_components_from_gcp_path
from metamist.parser.cloudhelper import CloudHelper


class AuditHelper(CloudHelper):
    """General helper class for bucket auditing"""

    EXCLUDED_SGS = [
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

    def find_assay_files_in_gcs_bucket(
        self, bucket_name: str, file_extensions: tuple[str]
    ) -> list[str]:
        """Gets all the gs paths to fastq files in the datasets upload bucket"""
        if bucket_name.startswith('gs://'):
            bucket_name = bucket_name.removeprefix('gs://')
        assay_paths = []
        if 'upload' not in bucket_name:
            # No prefix means it will get all blobs in the bucket (regardless of path)
            # This can be a dangerous call outside of the upload buckets
            raise NameError(
                'Call to list_blobs without prefix only valid for upload buckets'
            )

        for blob in self.gcs_client.list_blobs(bucket_name, prefix=''):
            if blob.name.endswith(file_extensions):
                assay_paths.append(f'gs://{bucket_name}/{blob.name}')
            continue

        return assay_paths

    @staticmethod
    def get_sequencing_group_ids_from_analysis(analysis) -> list[str]:
        """Tries a number of different field names to retrieve the sg ids from an analysis"""
        while True:
            try:
                sg_ids = analysis['meta']['sample']
                break
            except KeyError:
                pass

            try:
                sg_ids = analysis['meta']['samples']
                break
            except KeyError:
                pass

            try:
                sg_ids = analysis['meta']['sample_ids']
                break
            except KeyError:
                pass

            try:
                sg_ids = analysis['meta']['sequencing_group']
                break
            except KeyError:
                pass

            try:
                sg_ids = analysis['meta']['sequencing_groups']
                break
            except KeyError as exc:
                raise ValueError(
                    f'Analysis {analysis["id"]} missing sample or sequencing group field.'
                ) from exc

        if isinstance(sg_ids, str):
            return [
                sg_ids,
            ]
        return sg_ids

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
