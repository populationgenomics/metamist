import asyncio
import csv
import logging
from typing import Any

from cloudpathlib import GSPath
from google.cloud import storage

from sample_metadata.api.project_api import ProjectApi
from sample_metadata.api.sample_api import SampleApi
from sample_metadata.api.sequence_api import SequenceApi

loggers_to_silence = [
    'urllib3.connectionpool',
    'urllib3.util.retry',
    'google.auth._default',
    'google.auth.compute_engine._metadata',
    'google.auth.transport.requests',
    'fsspec',
    'gcsfs',
    'gcsfs.credentials',
]
for logger_name in loggers_to_silence:
    logging.getLogger(logger_name).setLevel(level=logging.WARN)

logger = logging.getLogger(__file__)
logger.setLevel(level=logging.INFO)

sapi = SampleApi()
seqapi = SequenceApi()
gcs_client = storage.Client()


def _list_gcs_directory(gcs_path) -> dict[str, Any]:
    path = GSPath(gcs_path)
    if path.parts[2:]:
        remaining_path = '/'.join(path.parts[2:]) + '/'  # has to end with "/"
    else:
        remaining_path = None
    blobs = gcs_client.list_blobs(path.bucket, prefix=remaining_path, delimiter='/')
    return {f'gs://{path.bucket}/{blob.name}': blob for blob in blobs}


async def process_datasets(seqr_only: bool = False):
    """Find misplaced exomes in all (or seqr only) projects"""
    if seqr_only:
        projects = [p['name'] for p in ProjectApi().get_seqr_projects()]
    else:
        projects = [p['name'] for p in ProjectApi().get_all_projects()]

    bad_crams_nested = await asyncio.gather(
        *[get_bad_crams_for_dataset(dataset) for dataset in projects]
    )
    bad_crams_unnested = [row for rows in bad_crams_nested for row in rows]

    if bad_crams_unnested:
        with open('bad-crams.tsv', encoding='utf-8', mode='w+') as file:
            tsv_writer = csv.writer(file, delimiter='\t')
            tsv_writer.writerows(bad_crams_unnested)


async def get_bad_crams_for_dataset(dataset) -> list[list[str]]:
    """
    For a specific project:
        * get all sample IDs
        * get all exome sequences (to link which samples have exomes)
        * get all CRAMs for samples with exomes
        * Work out those CRAMs are directly in the cpg-{dataset}-main/cram directory
        * report [(cram_path, size)]
    """
    internal_sids = await sapi.get_all_sample_id_map_by_internal_async(project=dataset)
    if not internal_sids:
        logger.warning(f'{dataset} :: Skipping, no samples')
        return []

    sequences = await seqapi.get_sequences_by_sample_ids_async(
        list(internal_sids.keys()), get_latest_sequence_only=False
    )
    sids_with_exomes = set(
        [seq['sample_id'] for seq in sequences if seq['type'] == 'exome']
    )
    if len(sids_with_exomes) == 0:
        logger.warning(f'{dataset} :: no exomes')
        return []

    cram_blobs = _list_gcs_directory(f'gs://cpg-{dataset}-main/cram')
    cram_bucket_contents = set(cram_blobs.keys())

    potential_cram_paths = set(
        [f'gs://cpg-{dataset}-main/cram/{sid}.cram' for sid in sids_with_exomes]
    )
    bad_crams = potential_cram_paths.intersection(cram_bucket_contents)

    if not bad_crams:
        logger.warning(f'{dataset} :: No bad crams')
        return []

    logger.warning(f'{dataset} :: Had {len(bad_crams)} bad crams')
    return [[cram, f'{cram_blobs[cram].size / 2**30} GiB'] for cram in bad_crams]


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(process_datasets(seqr_only=True))
