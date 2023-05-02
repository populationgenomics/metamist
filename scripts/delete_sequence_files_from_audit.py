#!/usr/bin/env python
import csv
from datetime import datetime
import logging
import sys
import click
from google.cloud import storage
from cloudpathlib import CloudPath
from cpg_utils.config import get_config


CLIENT = storage.Client()

TODAY = datetime.today().strftime('%Y-%m-%d')


def clean_up_cloud_storage(locations: list[CloudPath]):
    """Given a list of locations of files to be deleted"""
    for location in locations:
        location.unlink()
        logging.info(f'{location.name} was deleted from cloud storage.')


@click.command('Delete the sequence paths found by the audit')
@click.option('--date', help='Date of audit results to pass to delete', default=TODAY)
@click.option(
    '--sequence-type',
    '-s',
    type=click.Choice(['genome', 'exome', 'all']),
    required='True',
    help='genome, exome, or all',
)
@click.option(
    '--file-types',
    '-f',
    type=click.Choice(['fastq', 'bam', 'cram', 'gvcf', 'vcf', 'all']),
    required='True',
    help='Find fastq, bam, cram, gvcf, vcf, or all sequence file types',
)
def main(date, sequence_type, file_types):
    """
    Read the csv containing the results of the upload bucket audit
    Deletes the files given in the 'Sequence_Path' field of the csv
    Relatively safe as only files in the specific audit results file can be deleted
    """
    config = get_config()
    project = config['workflow']['dataset']
    access_level = config['workflow']['access_level']
    if access_level == 'full':
        bucket_name = f'cpg-{project}-main-upload'
    else:
        bucket_name = f'cpg-{project}-test-upload'

    subdir = f'audit_results/{date}/'

    file = f'{subdir}/{project}_{file_types}_{sequence_type}_sequences_to_delete_{date}.csv'

    bucket = CLIENT.get_bucket(bucket_name)
    blob = bucket.get_blob(file)

    blob.download_to_filename('./sequences_to_delete.csv', CLIENT)
    logging.info(f'Downloaded audit results: {blob.name}')

    paths = set()  # Read the delete paths as a set to prevent duplicates
    with open('./sequences_to_delete.csv', 'r') as f:
        reader = csv.DictReader(f)
        next(reader)  # Skip header
        for row in reader:
            paths.add(CloudPath(row['Sequence_Path']))

    clean_up_cloud_storage(list(paths))
    logging.info(f'{len(paths)} deleted')


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter