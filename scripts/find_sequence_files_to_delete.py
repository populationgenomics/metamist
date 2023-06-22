#!/usr/bin/env python3
"""
Get a list of uploaded files for seqr that have already been successfully processed,
and can therefore be safely deleted.
This requires read-access to all seqr projects in metamist,
and list access to all seqr main-upload buckets.

Requirements:
    pip install sample-metadata google-cloud-storage click
"""

# pylint: disable=W0703

import os
import asyncio
import logging
from collections import defaultdict
from typing import Any

import click
from google.cloud import storage
from metamist.apis import ProjectApi
from metamist.graphql import query_async

# Global vars
EXTENSIONS = ['.fastq.gz', '.fastq', '.bam', '.cram', '.fq', 'fq.gz']

logger = logging.getLogger(__file__)
logger.setLevel(level=logging.INFO)

client = storage.Client()

projapi = ProjectApi()

# TODO: fetch this from metamist
CPG_SEQUENCING_GROUP_IDS_TO_SKIP = {
    'CPG11783',  # acute-care, no FASTQ data
    'CPG255232',  # perth-neuro: new
    'CPG255240',  # perth-neuro: new
    'CPG253328',  # perth-neuro, contamination rate 32%
    'CPG13409',  # perth-neuro, coverage ~0x
    'CPG243717',
    'CPG246645',  # ag-hidden, eof issue  https://batch.hail.populationgenomics.org.au/batches/97645/jobs/440
    'CPG246678',  # ag-hidden, diff fastq size  https://batch.hail.populationgenomics.org.au/batches/97645/jobs/446
    'CPG246561',  # ag-hidden, coverage ~0x https://main-web.populationgenomics.org.au/ag-hidden/qc/cram/multiqc.html
}


def get_bucket_name_from_path(path_input):
    """
    >>> get_bucket_name_from_path('gs://my-bucket/path')
    'my-bucket'
    """

    path = str(path_input)

    if not path.startswith('gs://'):
        raise ValueError(f'path does not start with gs://: {path}')

    return path[len('gs://') :].split('/', maxsplit=1)[0]


def get_filenames_from_assays(assays: list[dict[str, Any]]) -> set[str]:
    """
    Convert sm-formatted files to list of gs:// filenames
    """
    all_filenames = set()
    if not isinstance(assays, list):
        assays = list(assays)
    for assay in assays:
        reads = assay['meta'].get('reads')
        if not reads:
            continue
        # support up to three levels of nesting, because:
        #
        #   - reads is typically an array
        #       - handle the case where it's not an array
        #   - fastqs exist in pairs.
        # eg:
        #   - reads: cram   # this shouldn't happen, but handle it anyway
        #   - reads: [cram1, cram2]
        #   - reads: [[fq1_forward, fq1_back], [fq2_forward, fq2_back]]

        if isinstance(reads, dict):
            all_filenames.add(reads['location'])
            continue
        if not isinstance(reads, list):
            logging.error(f'Invalid read type: {type(reads)}: {reads}')
            continue
        for read in reads:
            if isinstance(read, list):
                for inner_read in read:
                    if not isinstance(inner_read, dict):
                        logging.error(
                            f'Got {type(inner_read)} read, expected dict: {inner_read}'
                        )
                        continue
                    all_filenames.add(inner_read['location'])
            else:
                if not isinstance(read, dict):
                    logging.error(f'Got {type(read)} read, expected dict: {read}')
                    continue
                all_filenames.add(read['location'])

    return all_filenames


async def get_filenames_from_assays_for_which_crams_exist(
    cpg_project, sequencing_types_to_remove: set[str]
) -> set[str]:
    """
    Get all sequence files for a project
    """
    try:
        sg_query = """
query GetSgIdsQuery($project: String!) {
    project(name: $project) {
        samples {
            # activeOnly to find assays for older excluded samples
            sequencing_groups(activeOnly: false) {
                id
                assays {
                    id
                    meta
                }
                analyses(type: "cram") {
                    id
                    meta
                }
            }
        }
    }
}
        """
        response = await query_async(sg_query, {'project': cpg_project})
        filenames_to_remove: set[str] = set()
        for _ in response['project']['samples']:
            for sg in _['sequencing_groups']:
                if 'analyses' not in sg:
                    # there are no crams, so we won't remove
                    continue
                if sg['id'] in CPG_SEQUENCING_GROUP_IDS_TO_SKIP:
                    continue
                if sg['type'] not in sequencing_types_to_remove:
                    continue

                filenames_to_remove |= get_filenames_from_assays(sg['assays'])

        return filenames_to_remove

    except Exception as err:
        logging.error(f'{cpg_project} :: Cannot get assays {repr(err)}')

    return set()


def find_existing_files_in_bucket(
    bucket_name: str, filenames: set[str]
) -> dict[str, str]:
    """
    Find set of files from filenames in bucket
    """
    if not bucket_name.endswith('-main-upload'):
        logging.error(f'Got a non main-upload bucket? {bucket_name}, {filenames}')
        return {}

    bucket = client.get_bucket(bucket_name)
    logging.info(f'Loading {bucket_name}')

    # no prefix means it will get all blobs in the bucket (regardless of path)
    # this can be a dangerous call
    files_in_bucket = {}
    for blob in client.list_blobs(bucket):
        bl = blob.name.lower()
        if not any(bl.endswith(ext) for ext in EXTENSIONS):
            continue
        files_in_bucket[os.path.basename(blob.name)] = f'gs://{bucket_name}/{blob.name}'

    fns = set(os.path.basename(f) for f in filenames)
    files_in_sm = set(files_in_bucket.keys()).intersection(fns)

    return {name: files_in_bucket[name] for name in files_in_sm}


async def find_files_to_delete(
    sequencing_types_to_remove: list[str], projects_to_ignore: list[str], output_path: str
):
    """
    Get all the sequences across all the projects from metamist
    """

    _projects_to_ignore = set(projects_to_ignore or [])
    my_projects_raw, seqr_projects_raw = await asyncio.gather(
        projapi.get_my_projects_async(), projapi.get_seqr_projects_async()
    )

    # I can only action projects I'm a part of, and I only want to

    my_projects = set(p.replace('-test', '') for p in my_projects_raw)
    seqr_projects = set(p['dataset'] for p in seqr_projects_raw)
    missing_projects = seqr_projects - my_projects
    projects = my_projects.intersection(seqr_projects) - _projects_to_ignore
    if missing_projects:
        logging.info(
            f'FYI, you are missing ({len(missing_projects)}) projects from this count: '
            + ', '.join(missing_projects)
        )

    names = sorted(projects)
    print(names)

    jobs = [
        get_filenames_from_assays_for_which_crams_exist(
            p, set(sequencing_types_to_remove)
        )
        for p in names
    ]
    list_of_filenames: list[set[str]] = await asyncio.gather(*jobs)
    assay_filenames = list_of_filenames[0]
    for _assay_filenames in list_of_filenames[1:]:
        assay_filenames |= _assay_filenames

    if not assay_filenames:
        raise ValueError('Probably an error as no filenames were found to remove')

    filenames_by_buckets: dict[str, set[str]] = defaultdict(set)

    for file in assay_filenames:
        bucket = get_bucket_name_from_path(file)
        filenames_by_buckets[bucket].add(file)

    for key, value in filenames_by_buckets.items():
        logging.info(f'{key}, {len(value)}')

    # filename to file path
    missing = {}
    for bucket, files in filenames_by_buckets.items():
        missing.update(find_existing_files_in_bucket(bucket, files))

    logging.error(f'Found {len(missing)} sequencing files to delete')

    with open(output_path, 'w+', encoding='utf-8') as fout:
        fout.write('\n'.join(sorted(missing.values())))

    return list(missing.values())


@click.command()
@click.option(
    '--sequence-type',
    type=click.Choice(['genome', 'exome']),
    multiple=True,
    # required=True,
    default=['genome', 'exome'],
)
@click.option('--project-to-ignore', multiple=True)
@click.option('--output-path', default='files-to-remove.txt')
def main(sequencing_type: list[str], project_to_ignore: list[str], output_path: str):
    """Main from CLI"""
    asyncio.run(
        find_files_to_delete(
            sequencing_types_to_remove=sequencing_type,
            projects_to_ignore=project_to_ignore,
            output_path=output_path,
        )
    )


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
