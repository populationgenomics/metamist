""" This script finds analysis entries for a list of samples
    and checks the bucket for extra cram/gvcf files.
    Outputs a csv with all analyses for each sample and
    the paths to any extra files.
    """
from collections import defaultdict
import csv
import logging
import click

from google.cloud import storage

from sample_metadata.apis import AnalysisApi
from sample_metadata.model.analysis_query_model import AnalysisQueryModel
from sample_metadata.model.analysis_type import AnalysisType

client = storage.Client()
aapi = AnalysisApi()

ANALYSIS_TYPES = [
    'qc',
    'joint-calling',
    'gvcf',
    'cram',
    'custom',
    'es-index',
    'sv',
    'web',
    'analysis-runner',
]


def get_analyses_for_samples(datasets, sample_ids, analysis_type) -> list[dict]:
    """API call to get all analysis objects for samples in datasets"""
    return aapi.query_analyses(
        AnalysisQueryModel(
            projects=datasets,
            type=AnalysisType(analysis_type),
            sample_ids=sample_ids
            # status=AnalysisStatus('completed'),
        )
    )


def find_sample_files(projects: list[str], samples: list[str], csv_path: str):
    """Find all analyses associated with the provided samples"""
    analyses = []
    for analysis_type in ANALYSIS_TYPES:
        analyses.extend(get_analyses_for_samples(projects, samples, analysis_type))

    analyses_for_samples = []
    for analysis in analyses:
        if analysis['output']:
            analyses_for_samples.append(analysis)
            logging.info(f'Found {analysis["output"]}')

    with open(csv_path, 'w') as f:
        writer = csv.writer(f)
        header = [
            'Sample_ID',
            'Analysis_ID',
            'Type',
            'Status',
            'Output',
            'Timestamp',
            'Multiple_Samples',
        ]

        writer.writerow(header)

        for analysis in analyses_for_samples:
            if len(analysis['sample_ids']) == 1:
                sample_ids = analysis['sample_ids'][0]
                multiple_samples = False
            else:
                sample_ids = analysis['sample_ids']
                multiple_samples = True
            row = [
                sample_ids,
                analysis['id'],
                analysis['type'],
                analysis['status'],
                analysis['output'],
                analysis['timestamp_completed'],
                multiple_samples,
            ]
            writer.writerow(row)


def get_extra_files(project: str, csv_path: str):
    """Reads the analysis_for_samples csv and finds extra files
    for the cram and gvcf analysis entries"""

    sample_paths = defaultdict(list)
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Get the output paths for cram and gvcf analyses
            if row['Type'] == 'gvcf' or row['Type'] == 'cram':
                sample_paths[row['Sample_ID']].append(row['Output'])

    # Find extra file paths associated with crams and gvcfs
    extra_filepaths = get_extra_file_paths(project, sample_paths)

    # Add these extra paths to the csv
    with open(csv_path, 'a') as f:
        writer = csv.writer(f)
        for sample_id, extra_paths in extra_filepaths.items():
            for extra_path in extra_paths:
                if 'cram' in extra_path:
                    filetype = 'cram'
                else:
                    filetype = 'gvcf'

                writer.writerow([sample_id, '', filetype, '', extra_path, '', False])


def get_extra_file_paths(project: str, sample_paths: dict[str, list]):
    """
    Search the cloud buckets for files related to the analysis
    entries found for the samples. Return the paths to extra
    files found in the analysis output paths for the samples
    (e.g. cram.crai, cram.md5, g.vcf.gz.tbi).
    """
    # Get the directories for the analysis entries of interest
    paths_to_search = []
    for paths in sample_paths.values():
        for path in paths:
            prefix = path.rsplit('/', 1)[0]
            if prefix not in paths_to_search:
                paths_to_search.append(prefix)

    bucket_name = f'cpg-{project}-main'

    # Get the subdirectories for the paths being searched
    subdirs = []
    for prefix in paths_to_search:
        subdir = prefix.removeprefix(f'gs://{bucket_name}/')
        if subdir not in subdirs:
            subdirs.append(subdir)

    # List all blobs in the subdirectory of the bucket
    blob_paths = []
    for subdir in subdirs:
        for blob in client.list_blobs(bucket_name, prefix=f'{subdir}/', delimiter='/'):
            blob_paths.append(f'gs://{bucket_name}/{blob.name}')

    # Identify the extra files paths by checking if the filename contains the sample ID
    extra_paths = defaultdict(list)
    for path in blob_paths:
        for sample_id in sample_paths.keys():
            if sample_id in path:
                extra_paths[sample_id].append(path)

    return extra_paths


@click.command()
@click.option('--project', help='Metamist project, used to filter samples')
@click.option('--csv-path', default='./')
@click.option('--samples', default=None, multiple=True)
def main(project, csv_path, samples):
    """Searches for analysis objects for the samples in
    the project + the seqr project, saves these to a csv"""

    samples = list(samples)
    projects = [project, 'seqr']

    find_sample_files(projects, samples, csv_path)

    get_extra_files(project, csv_path)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
