#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,no-self-use,wrong-import-order,unused-argument,too-many-arguments

import os
import subprocess
from typing import Dict, List
import random
import logging
import click
from google.cloud import storage
from sample_metadata import (
    AnalysisApi,
    SequenceApi,
    SampleApi,
    NewSequence,
    NewSample,
    AnalysisModel,
    exceptions,
)

logger = logging.getLogger(__file__)
logging.basicConfig(format='%(levelname)s (%(name)s %(lineno)s): %(message)s')
logger.setLevel(logging.INFO)


sapi = SampleApi()
aapi = AnalysisApi()
seqapi = SequenceApi()


@click.command()
@click.option(
    '--project',
    required=True,
    help='The sample-metadata project ($DATASET)',
)
@click.option(
    '-n',
    required=True,
    type=int,
    help='Number of samples to subset',
)
def main(
    project: str,
    n: int,
):
    """
    Script creates a test subset for a given project.
    A new project with a prefix -test is created, and for any files in sample/meta,
    sequence/meta, or analysis/output a copy in the -test namespace is created.
    """
    samples = sapi.get_samples(
        body_get_samples_by_criteria_api_v1_sample_post={
            'project_ids': [project],
            'active': True,
        }
    )
    logger.info(f'Found {len(samples)} samples')
    random.seed(42)  # for reproducibility
    samples = random.sample(samples, n)
    sample_ids = [s['id'] for s in samples]
    logger.info(f'Subset to {len(samples)} samples: {", ".join(sample_ids)}')

    seq_infos: List[Dict] = seqapi.get_sequences_by_sample_ids(sample_ids)
    seq_info_by_s_id = dict(zip(sample_ids, seq_infos))

    target_project = project + '-test'

    for s in samples:
        logger.info(f'Processing sample {s["id"]}')

        # processing sample entries
        try:
            new_s = sapi.get_sample_by_external_id(s['external_id'], target_project)
        except exceptions.ApiException:
            new_s_id = sapi.create_new_sample(
                project=target_project,
                new_sample=NewSample(
                    external_id=s['external_id'] + '-test',
                    type=s['type'],
                    meta=_copy_files_in_dict(s['meta'], project),
                ),
            )
        else:
            new_s_id = new_s.id

        # processing sequence entries
        seq_info = seq_info_by_s_id.get(s['id'])
        if seq_info:
            new_meta = _copy_files_in_dict(seq_info.get('meta'), project)
            print(new_meta)
            seqapi.create_new_sequence(
                new_sequence=NewSequence(
                    sample_id=new_s_id,
                    meta=new_meta,
                    type=seq_info['type'],
                    status=seq_info['status'],
                )
            )

        # processing analysis entries
        for a_type in ['cram', 'gvcf']:
            try:
                analysis = aapi.get_latest_analysis_for_samples_and_type(
                    project=project,
                    analysis_type=a_type,
                    request_body=[s['id']],
                )
            except exceptions.ApiException:
                pass
            else:
                am = AnalysisModel(
                    type=a_type,
                    output=_copy_files_in_dict(analysis['output'], project),
                    status=analysis['status'],
                    sample_ids=[s['id']],
                )
                aapi.create_new_analysis(project=target_project, analysis_model=am)


def _copy_files_in_dict(d, dataset: str):
    """
    Replaces all `gs://cpg-{project}-main*/` paths
    into `gs://cpg-{project}-test*/` and creates copies if needed
    If `d` is dict or list, recursively calls this function on every element
    If `d` is str, replaces the path
    """
    if not d:
        return d
    if isinstance(d, str) and d.startswith(f'gs://cpg-{dataset}-main'):
        old_path = d
        if not file_exists(old_path):
            logger.warning(f'File {old_path} does not exist')
            return d
        new_path = old_path.replace(
            f'gs://cpg-{dataset}-main', f'gs://cpg-{dataset}-test'
        )
        if not file_exists(new_path):
            cmd = f'gsutil -u {dataset} cp "{old_path}" "{new_path}"'
            logger.info(f'Copying file in metadata: {cmd}')
            subprocess.run(cmd, check=False, shell=True)
        for suf in ['.tbi', '.md5']:
            if file_exists(old_path + suf) and not file_exists(new_path + suf):
                cmd = f'gsutil -u {dataset} cp "{old_path + suf}" "{new_path + suf}"'
                logger.info(f'Copying extra file in metadata: {cmd}')
                subprocess.run(cmd, check=False, shell=True)
        return new_path
    if isinstance(d, list):
        return [_copy_files_in_dict(x, dataset) for x in d]
    if isinstance(d, dict):
        return {k: _copy_files_in_dict(v, dataset) for k, v in d.items()}
    return d


def file_exists(path: str) -> bool:
    """
    Check if the object exists, where the object can be:
        * local file
        * local directory
        * Google Storage object
    :param path: path to the file/directory/object
    :return: True if the object exists
    """
    if path.startswith('gs://'):
        bucket = path.replace('gs://', '').split('/')[0]
        path = path.replace('gs://', '').split('/', maxsplit=1)[1]
        gs = storage.Client()
        return gs.get_bucket(bucket).get_blob(path)
    return os.path.exists(path)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
