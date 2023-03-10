#!/usr/bin/env python3


"""
Given a csv with project IDs and  sample IDs, copies cram files for
each sample listed into the project's release bucket.
"""

from collections import defaultdict
import csv
import logging
import sys
import subprocess
import click

# pylint: disable=E0401,E0611
from sample_metadata.apis import AnalysisApi
from sample_metadata.models import AnalysisType


def copy_to_release(project: str, path: str):
    """
    Copy a single file from a main bucket path to the equivalent release bucket
    """
    release_path = path.replace(
        f'cpg-{project}-test',#f'cpg-{project}-main',
        f'cpg-{project}-release',
    )

    subprocess.run(f'gsutil cp {path} {release_path}', shell=True, check=True)
    logging.info(f'Copied {release_path}')

@click.command()
@click.option('--f', '--filename', help='Path for the project : external id mapping csv', required=True)
def main(filename: str,):
    """

    Parameters
    ----------
    filename :  a path to a csv containing metmaist project names in column 1, sample ids in column 2, and
                external participant ids in column 3. 
                Only Columns 1 and 3 are required for this script.
    """
    # Create the { project : [sample_ids,] } dict mapping each project to all its sample IDs in the file
    project_samples = defaultdict(list)
    with open(filename, 'r') as mapping_file:
        reader = csv.reader(mapping_file)
        # Each project ID becomes a key with its list of sample IDs as its value
        for row in reader:
            project_samples[row[0]].append(row[1])

    for project, sample_ids in project_samples.items():
        # Retrieve latest crams for selected samples
        latest_crams = AnalysisApi().get_latest_analysis_for_samples_and_type(
            AnalysisType('cram'), project, request_body=sample_ids
        )

        # Copy files to test
        for cram in latest_crams:
            copy_to_release(project, cram['output'])
            copy_to_release(project, cram['output'] + '.crai')

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%M-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()