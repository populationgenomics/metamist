#!/usr/bin/env python

"""
This script will take a path to a csv file, containing a column with rows populated
by paths to delete. The --delete-field-name option allows you to specify which field
of the csv contains the paths to delete.
Creates a log file of all the deletes, in the same directory as the input csv.

The typical upload-bucket audit results will be found in a file such as:
 > gs://cpg-dataset-main-upload/audit_results/2023-xx-xx/dataset_seqtype_readstype_assay_files_to_delete_20xx-xx-xx.csv
And the field name containing the delete paths will be
 > "Assay_Read_File_Path"
"""

import csv
from datetime import datetime
import logging
import os
import sys
import click
from google.cloud import storage
from cloudpathlib import CloudPath, AnyPath
from cpg_utils.config import get_config
from metamist.audit.audithelper import AuditHelper


CLIENT = storage.Client()

TODAY = datetime.today().strftime('%Y-%m-%d')


def clean_up_cloud_storage(locations: list[CloudPath]):
    """Given a list of locations of files to be deleted"""
    deleted_files = []
    for location in locations:
        try:
            location.unlink()
            logging.info(f'{location.name} was deleted from cloud storage.')
            deleted_files.append(location.name)
        # Many possible http exceptions could occur so use a broad exception
        except Exception:  # pylint: disable=W0718
            logging.warning(f'{location.name} threw an exception - file not deleted.')

    return deleted_files


@click.command('Delete the assay files found by the audit')
@click.option(
    '--delete-field-name',
    '-d',
    help='The field in the input csv corresponding to the files to delete',
    required=True,
)
@click.argument('delete-file-path')
def main(delete_field_name, delete_file_path):
    """
    Inputs:
        - The name of the field containing the paths to delete in the input csv
        - The full GSPath to the csv containing the paths to delete
    """
    access_level = get_config()['workflow']['access_level']

    if access_level == 'standard':
        raise ValueError(
            'Standard cannot be used as the access level. Full or test is required.'
        )

    paths = set()
    with AnyPath(delete_file_path).open('r') as f:  # pylint: disable=E1101
        logging.info(f'Getting file {delete_file_path}...')
        reader = csv.DictReader(f)
        for row in reader:
            paths.add(CloudPath(row[delete_field_name]))

    deleted_files = clean_up_cloud_storage(list(paths))
    logging.info(f'{len(deleted_files)} sequence files deleted.')

    # Write a log of the deleted files to the same location
    log_path = f'{delete_file_path.removesuffix(os.path.basename(delete_file_path))}deleted_{TODAY}.csv'
    AuditHelper.write_csv_report_to_cloud(
        deleted_files, log_path, header_row=['Deleted_file_path']
    )


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter
