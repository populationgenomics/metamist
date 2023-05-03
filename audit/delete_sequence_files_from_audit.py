#!/usr/bin/env python
import csv
from datetime import datetime
import logging
import sys
import click
from google.cloud import storage
from cloudpathlib import CloudPath, AnyPath
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
    type=click.Choice(['fastq', 'bam', 'cram', 'all_reads', 'gvcf', 'vcf', 'all']),
    required='True',
    help='Delete fastq, bam, cram, all_reads (fastq + bam + cram), gvcf, vcf, or all sequence file types',
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
    subdir = f'audit_results/{date}'
    if access_level == 'full':
        bucket_name = f'cpg-{project}-main-upload'
        file = f'{subdir}/{project}_{file_types}_{sequence_type}_sequences_to_delete_{date}.csv'
    else:
        bucket_name = f'cpg-{project}-test-upload'
        file = f'{subdir}/{project}-test_{file_types}_{sequence_type}_sequences_to_delete_{date}.csv'

    report_path = f'gs://{bucket_name}/{file}'

    paths = set()
    with AnyPath(report_path).open('r') as f:  # pylint: disable=E1101
        logging.info(f'Getting file {report_path}...')
        reader = csv.DictReader(f)
        for row in reader:
            paths.add(CloudPath(row['Sequence_Path']))

    clean_up_cloud_storage(list(paths))
    logging.info(f'{len(paths)} sequence files deleted.')


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter
