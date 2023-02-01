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
from sample_metadata.apis import SampleApi, SequenceApi, ProjectApi, AnalysisApi

from sample_metadata.model.analysis_query_model import AnalysisQueryModel
from sample_metadata.model.analysis_status import AnalysisStatus
from sample_metadata.model.analysis_type import AnalysisType

# Global vars
EXTENSIONS = ['.fastq.gz', '.fastq', '.bam', '.cram', '.fq', 'fq.gz']

logger = logging.getLogger(__file__)
logger.setLevel(level=logging.INFO)

client = storage.Client()

projapi = ProjectApi()
sampapi = SampleApi()
seqapi = SequenceApi()
aapi = AnalysisApi()

# TODO: fetch this from metamist
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


def get_filenames_from_sequences(sequences: list[dict[str, Any]]) -> set[str]:
    """
    Convert sm-formatted files to list of gs:// filenames
    """
    all_filenames = set()
    for sequence in sequences:
        reads = sequence['meta'].get('reads')
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
                    logging.error(
                        f'Got {type(inner_read)} read, expected dict: {inner_read}'
                    )
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
            # crams always have only one sample_id, so take the first element safely
            s[0]
            for s in sapi_seqtype_with_crams
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
            request_body=sapis_to_remove
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
    sequence_types_to_remove: list[str], projects_to_ignore: list[str], output_path: str
):
    """
    Get all the sequences across all the projects from sample_metadata
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
        get_sequences_for_which_crams_exist(p, set(sequence_types_to_remove))
        for p in names
    ]
    sequences = await asyncio.gather(*jobs)
    sequences = sum(sequences, [])

    if not sequences:
        raise ValueError('Probably an error as no sequences were found to remove')

    sequence_filenames = get_filenames_from_sequences(sequences)
    filenames_by_buckets: dict[str, set[str]] = defaultdict(set)

    for file in sequence_filenames:
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
def main(sequence_type: list[str], project_to_ignore: list[str], output_path: str):
    """Main from CLI"""
    asyncio.run(
        find_files_to_delete(
            sequence_types_to_remove=sequence_type,
            projects_to_ignore=project_to_ignore,
            output_path=output_path,
        )
    )


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
