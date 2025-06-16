#!/usr/bin/env python

"""
This script takes a path to a csv/tsv report, and a column name.
It will delete all the files in the column from the cloud storage bucket.
A log file of the deletes will be written to the same directory as the input report.
"""

import csv
from io import StringIO
import logging
import os
import sys
from datetime import datetime
from typing import Any

import click
from cloudpathlib import AnyPath, CloudPath
from google.cloud import storage

from cpg_utils.config import config_retrieve

from metamist.audit.audithelper import AuditHelper

# from metamist.audit.audithelper import AuditHelper

CLIENT = storage.Client()

TODAY = datetime.today().strftime('%Y-%m-%d')

def chunked(iterable, size: int):
    """Yield successive n-sized chunks from iterable."""
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]
        
def delete_files_from_bucket(locations: list[CloudPath], bucket: storage.Bucket):
    """Delete files from the bucket in chunks of 100."""
    blobs_to_delete = [
        bucket.blob('/'.join(location.parts[2:])) for location in locations
    ]
    
    # batch delete blobs
    with CLIENT.batch():
        for blob in blobs_to_delete:
            blob.delete(client=CLIENT)
            logging.info(f'DELETED gs://{blob.bucket.name}/{blob.name}')


def clean_up_cloud_storage(locations: list[CloudPath], gcp_project: str):
    """Given a list of locations of files to be deleted"""
    bucket_name = locations[0].parts[1]  # Assuming the first part is 'gs://'
    bucket = CLIENT.bucket(bucket_name, user_project=gcp_project)
    
    # Delete blobs in chunks of 100 using batch requests
    for chunk in chunked(locations, 100):
        logging.info(f'Deleting {len(chunk)} files from cloud storage...')
        delete_files_from_bucket(chunk, bucket)
    
    # blobs_to_delete = [
    #     bucket.blob('/'.join(location.parts[2:])) for location in locations
    # ]
    
    # # batch delete blobs
    # with CLIENT.batch():
    #     for blob in blobs_to_delete:
    #         blob.delete()
    #         logging.info(f'DELETED gs://{blob.bucket.name}/{blob.name}')
    
    return [location.as_uri() for location in locations]    
    
    # for location in locations:
    #     blob_name = '/'.join(location.parts[2:])
    #     # logging.info(f'Deleting {location}...')
    #     # continue
    #     try:
    #         location.unlink()
    #         logging.info(f'{location.as_uri()} was deleted from cloud storage.')
    #         deleted_files.append(location.as_uri())
    #     # Many possible http exceptions could occur so use a broad exception
    #     except Exception:  # pylint: disable=W0718
    #         logging.warning(f'{location.as_uri()} threw an exception - file not deleted.')
    # # exit()
    # return deleted_files


@click.command('Delete the assay files found by the audit')
@click.option(
    '--delete-field-name',
    '-d',
    help='The field in the input csv corresponding to the files to delete',
    required=True,
)
@click.option('--dataset', '-ds', help='The dataset to delete files from')
@click.argument('delete-file-path')
def main(delete_field_name: str, dataset: str, delete_file_path: str):
    """
    Inputs:
        - The name of the field containing the paths to delete in the input csv
        - The full GSPath to the csv containing the paths to delete
    """
    access_level = config_retrieve(['workflow', 'access_level'])

    if access_level == 'standard':
        raise ValueError(
            'Standard cannot be used as the access level. Full or test is required.'
        )
    
    gcp_project = config_retrieve(['workflow', dataset, 'gcp_project'])
    if not gcp_project:
        raise ValueError(f'No GCP project found for dataset {dataset}. Please check the config.')

    paths_to_delete: list[CloudPath] = []
    with AnyPath(delete_file_path).open('r') as f:  # pylint: disable=E1101
        logging.info(f'Getting file {delete_file_path}...')
        reader = csv.DictReader(f)
        for row in reader:
            paths_to_delete.append(CloudPath(row[delete_field_name]))
            
    unique_paths_to_delete = []
    [unique_paths_to_delete.append(path) for path in paths_to_delete if path not in unique_paths_to_delete]

    audithelper = AuditHelper(dataset=dataset, gcp_project=gcp_project)
    buckets_prefixes = audithelper.get_gcs_buckets_and_prefixes_from_paths([str(path) for path in unique_paths_to_delete])
    bucket_files = audithelper.find_files_in_gcs_buckets_prefixes(buckets_prefixes)
    
    # Any files in paths_to_delete that are not in bucket_files will be skipped
    paths = [
        path for path in unique_paths_to_delete if str(path) in bucket_files
    ]

    deleted_files = clean_up_cloud_storage(paths, gcp_project)
    logging.info(f'{len(deleted_files)} sequence files deleted.')
    
    # Write a log of the deleted files to the same location
    log_path = f'{delete_file_path.replace(os.path.basename(delete_file_path), f"{dataset}_deleted_{TODAY}.csv")}'
    # AuditHelper.write_csv_report_to_cloud(
    write_csv_report(deleted_files, CloudPath(log_path), gcp_project)


def write_csv_report(
    data_to_write: list[Any], report_path: CloudPath, gcp_project: str,
):
    """Write a csv report to the local filesystem."""
    buffer = StringIO()
    writer = csv.DictWriter(
        buffer, fieldnames=['File Path'], delimiter=','
    )
    writer.writeheader()
    writer.writerows({'File Path': row} for row in data_to_write)
    
    bucket_name = report_path.parts[1]  # Assuming the first part is 'gs://'
    
    bucket = CLIENT.bucket(bucket_name, user_project=gcp_project)
    blob = bucket.blob('/'.join(report_path.parts[2:]))
    blob.upload_from_string(
        buffer.getvalue(),
        content_type='text/csv',
    )
    
    logging.info(f'Wrote report to {report_path}')


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter
