#!/usr/bin/env python3


"""
Given a csv with project IDs and  sample IDs, copies cram files for
each sample listed into the project's release bucket.
"""

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
@click.option('--project', '-p', help='Metamist name of the project', required=True)
@click.argument('samples', nargs=-1)
def main(project: str, samples):
    """

    Parameters
    ----------
    project :   a metamist project name
    samples :   a list of sample ids to copy
    """

    sample_ids = list(samples)

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