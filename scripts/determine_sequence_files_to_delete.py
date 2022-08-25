#!/usr/bin/env python3
# pylint: disable=W0703

import os
import asyncio
import logging
from collections import defaultdict
from typing import Set, Dict, Any, List

from google.cloud import storage
from sample_metadata.apis import SampleApi, SequenceApi, ProjectApi, AnalysisApi

# Global vars
from sample_metadata.model.analysis_query_model import AnalysisQueryModel
from sample_metadata.model.analysis_status import AnalysisStatus
from sample_metadata.model.analysis_type import AnalysisType

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)

client = storage.Client()

projapi = ProjectApi()
sampapi = SampleApi()
seqapi = SequenceApi()
aapi = AnalysisApi()

CPG_SAMPLES_TO_SKIP = {
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
                    logging.error(f'Got string read: {read}')
                    continue
                all_filenames.add(read['location'])

    return all_filenames


async def get_sequences_for_which_crams_exist(
    cpg_project, sequence_types_to_remove: set[str]
):
    """
    Get all sequence files for a project
    """
    try:
        resp = await sampapi.get_all_sample_id_map_by_internal_async(cpg_project)
        if len(resp) == 0:
            return []

        crams = await aapi.query_analyses_async(
            AnalysisQueryModel(
                projects=[cpg_project],
                type=AnalysisType('cram'),
                status=AnalysisStatus('completed'),
            )
        )

        crams_with_missing_seq_type = [
            analysis for analysis in crams if 'sequencing_type' not in analysis['meta']
        ]
        if crams_with_missing_seq_type:
            raise ValueError(
                f'CRAMs from dataset {cpg_project} did not have sequencing_type'
            )

        sapi_seqtype_with_crams = set(
            (sample_id, analysis['meta']['sequencing_type'])
            for analysis in crams
            for sample_id in analysis['sample_ids']
        )

        sapis_without_crams = set(resp.keys()) - set(
            s[0] for s in sapi_seqtype_with_crams
        )
        if sapis_without_crams:
            logging.warning(
                f'{cpg_project} :: {len(sapis_without_crams)} samples missing crams: {sapis_without_crams}'
            )

        sapis_to_remove = [
            sid
            for sid, seq_type in sapi_seqtype_with_crams
            if sid not in CPG_SAMPLES_TO_SKIP and seq_type in sequence_types_to_remove
        ]
        if len(sapis_to_remove) == 0:
            return []

        sequences = await seqapi.get_sequences_by_sample_ids_async(
            get_latest_sequence_only=False, request_body=sapis_to_remove
        )

        sequences_to_remove = [
            seq
            for seq in sequences
            if (seq['sample_id'], str(seq['type'])) in sapi_seqtype_with_crams
        ]

        return sequences_to_remove
    except Exception as err:
        logging.error(f'{cpg_project} :: Cannot get sequences {repr(err)}')

    return []


async def get_sm_sequences_with_crams():
    """
    Get all the sequences across all the projects from sample_metadata
    """
    sequence_types_to_remove = {
        'genome',
        # 'exome',
    }
    projects_to_ignore = {'ohmr4-epilepsy', 'validation'}
    my_projects_raw, seqr_projects_raw = await asyncio.gather(
        projapi.get_my_projects_async(), projapi.get_seqr_projects_async()
    )

    # I can only action projects I'm a part of, and I only want to

    my_projects = set(p.replace('-test', '') for p in my_projects_raw)
    seqr_projects = set(p['dataset'] for p in seqr_projects_raw)
    missing_projects = seqr_projects - my_projects
    projects = my_projects.intersection(seqr_projects) - projects_to_ignore
    if missing_projects:
        logging.info(
            f'FYI, you are missing ({len(missing_projects)}) projects from this count: '
            + ', '.join(missing_projects)
        )

    names = sorted(projects)
    print(names)

    jobs = [
        get_sequences_for_which_crams_exist(p, sequence_types_to_remove) for p in names
    ]
    sequences = await asyncio.gather(*jobs)
    sequences = sum(sequences, [])

    if not sequences:
        raise ValueError('Probably an error as no sequences were found to remove')

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
        missing.update(check_existing_files(bucket, files))

    if missing:
        logging.error(f'Found {len(missing)} sequencing files to delete')

        with open('files_to_delete.txt', 'w') as fout:
            fout.write('\n'.join(sorted(missing.values())))

    return sequences, missing


EXTENSIONS = ['.fastq.gz', '.fastq', '.bam', '.cram', '.fq', 'fq.gz']


def check_existing_files(bucket_name: str, filenames: Set[str]) -> Dict[str, str]:
    """
    Find set of missing files compared to list of filenames in a bucket
    """
    bucket = client.get_bucket(bucket_name)
    if not bucket_name.endswith('-main-upload'):
        logging.error(f'Got a non main-upload bucket? {bucket_name}, {filenames}')
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
    files_in_sm = set(files_in_bucket.keys()).intersection(fns)

    return {name: files_in_bucket[name] for name in files_in_sm}


def main():
    """Main from CLI"""
    asyncio.run(get_sm_sequences_with_crams())


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
