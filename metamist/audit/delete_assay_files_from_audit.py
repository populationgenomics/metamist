#!/usr/bin/env python

"""
This script takes a path to a csv/tsv report, and a column name. 
It will delete all the files in the column from the cloud storage bucket.
A log file of the deletes will be written to the same directory as the input report.
"""

import csv
import logging
import os
import sys
from datetime import datetime
from typing import Any

import click
from cloudpathlib import AnyPath, CloudPath
from google.cloud import storage

from cpg_utils.config import get_config

# from metamist.audit.audithelper import AuditHelper

CLIENT = storage.Client()

TODAY = datetime.today().strftime('%Y-%m-%d')


def clean_up_cloud_storage(locations: list[CloudPath]):
    """Given a list of locations of files to be deleted"""
    deleted_files = []
    for location in locations:
        # logging.info(f'Deleting {location}...')
        # continue
        try:
            location.unlink()
            logging.info(f'{location.name} was deleted from cloud storage.')
            deleted_files.append(location.name)
        # Many possible http exceptions could occur so use a broad exception
        except Exception:  # pylint: disable=W0718
            logging.warning(f'{location.name} threw an exception - file not deleted.')
    # exit()
    return deleted_files


@click.command('Delete the assay files found by the audit')
@click.option(
    '--delete-field-name',
    '-d',
    help='The field in the input csv corresponding to the files to delete',
    required=True,
)
@click.option('--dataset', '-ds', help='The dataset to delete files from')
@click.argument('delete-file-path')
def main(delete_field_name, dataset, delete_file_path):
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
    log_path = f'{delete_file_path.removesuffix(os.path.basename(delete_file_path))}{dataset}_deleted_{TODAY}.csv'
    # AuditHelper.write_csv_report_to_cloud(
    write_csv_report_to_local(deleted_files, log_path, header_row=['Deleted_file_path'])


def write_csv_report_to_local(
    data_to_write: list[Any], report_path: AnyPath, header_row: list[str] | None
):
    """Write a csv report to the local filesystem."""
    with open(report_path, 'w', newline='') as report_file:
        writer = csv.writer(report_file)
        if header_row:
            writer.writerow(header_row)
        for row in data_to_write:
            writer.writerow(row)
    logging.info(f'Wrote report to {report_path}')


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter
