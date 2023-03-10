#!/usr/bin/env python3


"""
Given a csv with project IDs and  sample IDs, copies cram files for
each sample listed into the project's release bucket.
"""


from argparse import ArgumentParser
from collections import defaultdict
import csv
import logging
import sys
import subprocess

# pylint: disable=E0401,E0611
from sample_metadata.apis import AnalysisApi
from sample_metadata.models import AnalysisType


def copy_to_release(project: str, path: str):
    """
    Copy a single file from a main bucket path to the equivalent release bucket
    """
    release_path = path.replace(
        f'cpg-{project}-main',
        f'cpg-{project}-release',
    )

    subprocess.run(f'gsutil cp {path} {release_path}', shell=True, check=True)
    logging.info(f'Copied {release_path}')


def main(
    project: str,
    sample_ids: list[str],
):
    """

    Parameters
    ----------
    project : metamist project name
    sample_ids : a list of internal sample IDs whose crams will be transferred to release
    """

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

    parser = ArgumentParser()
    parser.add_argument('-m', '--mapping', help='Path to file mapping projects to Sample IDs')
    parser.add_argument('-h', '--headers', help='True/False flag if mapping file contains header row')

    args, unknown = parser.parse_known_args()

    if unknown:
        raise ValueError(f'Unknown args, could not parse: "{unknown}"')

    project_samples = defaultdict(list)
    with open(args.mapping, 'r') as mapping_file:
        reader = csv.reader(mapping_file)
        if args.header:
            next(reader) # skip the header
        
        # Each project ID becomes a key with its list of sample IDs as its value
        for row in reader:
            project_samples[row[0]].append(row[2])

        mapping_file.close()

    for project, sample_ids in project_samples.items():
        main(
            project=project,
            sample_ids=sample_ids
        )
