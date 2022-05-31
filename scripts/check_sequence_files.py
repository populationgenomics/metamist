#!/usr/bin/env python3
# pylint: disable=W0703

import os
import asyncio
import logging
from collections import defaultdict
from typing import Set, Dict, Any, List

from google.cloud import storage
from sample_metadata.apis import WebApi, SampleApi, SequenceApi, ProjectApi

# Global vars
logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)

client = storage.Client()

projapi = ProjectApi()
webapi = WebApi()
sampapi = SampleApi()
seqapi = SequenceApi()


def get_bucket_name_from_path(path_input):
    """
    >>> get_bucket_name_from_path('gs://my-bucket/path')
    'my-bucket'
    """

    path = str(path_input)

    if not path.startswith('gs://'):
        raise ValueError(f'path does not start with gs://: {path}')

    return path[len('gs://') :].split('/', maxsplit=1)[0]


def get_filenames_from_sequences(sequences: List[Dict[str, Any]]) -> Set[str]:
    """
    Convert sm formated files to list of gs:// filenames
    """
    all_filenames = set()
    for sequence in sequences:
        reads = sequence['meta'].get('reads')
        if not reads:
            continue
        if isinstance(reads, dict):
            all_filenames.add(reads['location'])
            continue
        if not isinstance(reads, list):
            logging.error(f'Invalid read type: {type(reads)}: {reads}')
            continue
        for read in reads:
            if isinstance(read, list):
                for r in read:
                    if isinstance(r, str):
                        logging.error(f'Got string read: {r}')
                        continue
                    all_filenames.add(r['location'])
            else:
                if isinstance(read, str):
                    logging.error(f'Got string read: {r}')
                    continue
                all_filenames.add(read['location'])

    return all_filenames


async def get_all_sequence_files(cpg_project):
    """
    Get all sequence files for a project
    """
    try:
        resp = await sampapi.get_all_sample_id_map_by_internal_async(cpg_project)
        if len(resp) == 0:
            return []
        sequences = await seqapi.get_sequences_by_sample_ids_async(
            get_latest_sequence_only=False, request_body=list(resp.keys())
        )
        return sequences
    except Exception as err:
        logging.error(f'{cpg_project} :: Cannot get sequences {err}')

    return []


async def get_sm_sequences():
    """
    Get all the sequences across all the projects from sample_metadata
    """
    my_projects_raw, all_projects_raw = await asyncio.gather(
        projapi.get_my_projects_async(), projapi.get_all_projects_async()
    )

    projects = set(p.replace('-test', '') for p in my_projects_raw)
    all_projects = set(p['dataset'] for p in all_projects_raw)
    missing_projects = all_projects - projects
    if missing_projects:
        logging.info(
            f'FYI, you are missing ({len(missing_projects)}) projects from this count: '
            + ', '.join(missing_projects)
        )

    names = sorted(projects)
    print(names)

    jobs = [get_all_sequence_files(p) for p in names]
    sequences = await asyncio.gather(*jobs)
    sequences = sum(sequences, [])

    sequence_filenames = get_filenames_from_sequences(sequences)
    filenames_by_buckets: Dict[str, Set[str]] = defaultdict(set)

    for file in sequence_filenames:
        bucket = get_bucket_name_from_path(file)
        filenames_by_buckets[bucket].add(file)

    for k, v in filenames_by_buckets.items():
        logging.info(f'{k}, {len(v)}')

    # filename to file path
    missing = {}
    for bucket, files in filenames_by_buckets.items():
        missing.update(check_missing_file_names(bucket, files))

    if missing:
        logging.error(f'Missing files {len(missing)}')

        with open('missing_sequence_files.txt', 'w') as fout:
            fout.write('\n'.join(sorted(missing.values())))

    return sequences, missing


EXTENSIONS = ['.fastq.gz', '.fastq', '.bam', '.cram', '.fq', 'fq.gz']


def check_missing_file_names(bucket_name: str, filenames: Set[str]) -> Dict[str, str]:
    """
    Find set of missing files compared to list of filenames in a bucket
    """
    bucket = client.get_bucket(bucket_name)
    if not bucket_name.endswith('-main-upload'):
        logging.error(f'Got a non main-upload bucket? {bucket_name}')
        return {}

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
    files_not_in_sm = set(files_in_bucket.keys()) - fns
    if files_not_in_sm:
        logging.error(
            f'{bucket_name} :: {len(files_not_in_sm)}/{len(filenames)} missing files'
        )

    return {name: files_in_bucket[name] for name in files_not_in_sm}


def main():
    """Main from CLI"""
    asyncio.run(get_sm_sequences())


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
