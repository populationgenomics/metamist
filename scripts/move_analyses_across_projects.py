"""
This script is used to
  - Copy analysis files from one project's cloud storage bucket to another
  - Copy extra, untracked files from the source bucket to the destination bucket
  - Update analysis records associated with the moved files

It should be run via analysis-runner, using a service account that has access to
both the source and destination project's buckets, using full or test access level.

This script assumes that the project field for all the sequencing group and
analysis records has been updated in the database with the new project id.
"""

import logging
from typing import Any

import asyncio
import click

from cpg_utils.config import config_retrieve
from google.cloud import storage
from metamist.apis import AnalysisApi
from metamist.models import AnalysisUpdateModel, AnalysisStatus
from metamist.graphql import gql, query
from metamist.parser.generic_metadata_parser import run_as_sync

handler = logging.StreamHandler()
formatter = logging.Formatter(
    fmt='%(asctime)s %(levelname)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False

# Define the query to get the analysis records that need to be moved
ANALYSES_QUERY = gql(
    """
    query sgAnalyses($sequencingGroupIds: [String!], $dataset: String!, $analysisTypes: [String!]) {
        project(name: $dataset) {
            sequencingGroups(id: {in_: $sequencingGroupIds}) {
                id
                analyses(type: {in_: $analysisTypes}, status: {eq: COMPLETED}) {
                    id
                    type
                    meta
                    outputs
                }
            }
        }
    }
    """
)

ANALYSIS_TYPES = ['cram', 'gvcf', 'web', 'sv']

UNRECORDED_ANALYSIS_FILES = [
    # Mito CRAM files
    'gs://{bucket_name}/mito/{sg_id}.base_level_coverage.tsv',
    'gs://{bucket_name}/mito/{sg_id}.mito.cram',
    'gs://{bucket_name}/mito/{sg_id}.mito.cram.crai',
    'gs://{bucket_name}/mito/{sg_id}.mito.vcf.bgz',
    'gs://{bucket_name}/mito/{sg_id}.mito.vcf.bgz.tbi',
    'gs://{bucket_name}/mito/{sg_id}.mito.vep.vcf.gz',
    'gs://{bucket_name}/mito/{sg_id}.mito.vep.vcf.gz.tbi',
    'gs://{bucket_name}/mito/{sg_id}.shifted_mito.cram',
    'gs://{bucket_name}/mito/{sg_id}.shifted_mito.cram.crai',
    # CramQc stage files
    'gs://{bucket_name}/qc/{sg_id}.variant_calling_detail_metrics',
    'gs://{bucket_name}/qc/{sg_id}.variant_calling_summary_metrics',
    'gs://{bucket_name}/qc/{sg_id}_fastqc.html',
    'gs://{bucket_name}/qc/{sg_id}_fastqc.zip',
    'gs://{bucket_name}/qc/{sg_id}_picard_wgs_metrics.csv',
    'gs://{bucket_name}/qc/{sg_id}_samtools_stats.txt',
    'gs://{bucket_name}/qc/{sg_id}_verify_bamid.selfSM',
    'gs://{bucket_name}/qc/verify_bamid/{sg_id}.verify-bamid.selfSM',
    'gs://{bucket_name}/qc/samtools_stats/{sg_id}.samtools-stats',
    'gs://{bucket_name}/qc/alignment_summary_metrics/{sg_id}.alignment_summary_metrics',
    'gs://{bucket_name}/qc/base_distribution_by_cycle_metrics/{sg_id}.base_distribution_by_cycle_metrics',
    'gs://{bucket_name}/qc/insert_size_metrics/{sg_id}.insert_size_metrics',
    'gs://{bucket_name}/qc/quality_by_cycle_metrics/{sg_id}.quality_by_cycle_metrics',
    'gs://{bucket_name}/qc/quality_yield_metrics/{sg_id}.quality_yield_metrics',
    'gs://{bucket_name}/qc/picard_wgs_metrics/{sg_id}.picard-wgs-metrics',
    # Single Sample SV stage files
    'gs://{bucket_name}/sv_evidence/{sg_id}.coverage_counts.tsv.gz',
    'gs://{bucket_name}/sv_evidence/{sg_id}.pe.txt.gz',
    'gs://{bucket_name}/sv_evidence/{sg_id}.pe.txt.gz.tbi',
    'gs://{bucket_name}/sv_evidence/{sg_id}.sd.txt.gz',
    'gs://{bucket_name}/sv_evidence/{sg_id}.sd.txt.gz.tbi',
    'gs://{bucket_name}/sv_evidence/{sg_id}.sr.txt.gz',
    'gs://{bucket_name}/sv_evidence/{sg_id}.sr.txt.gz.tbi',
    # Somalier stage files
    'gs://{bucket_name}/cram/{sg_id}.cram.somalier',
    'gs://{bucket_name}/gvcf/{sg_id}.g.vcf.gz.somalier',
    # Stripy stage files
    'gs://{bucket_name}-analysis/stripy/{sg_id}.stripy.json',
    'gs://{bucket_name}-analysis/stripy/{sg_id}.stripy.log.txt',
    # MitoReport stage files
    'gs://{bucket_name}-analysis/mito/{sg_id}.coverage_mean.txt',
    'gs://{bucket_name}-analysis/mito/{sg_id}.coverage_median.txt',
    'gs://{bucket_name}-analysis/mito/{sg_id}.coverage_metrics.txt',
    'gs://{bucket_name}-analysis/mito/{sg_id}.haplocheck.txt',
    'gs://{bucket_name}-analysis/mito/{sg_id}.theoretical_sensitivity.txt',
]

UNRECORDED_ANALYSIS_FOLDERS = [
    # MitoReport stage folder
    'gs://{bucket_name}-web/mito/mitoreport-{sg_id}/',
]


def get_analyses_to_update_and_files_to_move(
    sequencing_group_ids: list[str],
    old_dataset: str,
    new_dataset: str,
) -> tuple[list[dict[str, Any]], list[tuple[str, str]]]:
    """
    Queries Metamist for the analyses that need to be updated
    and extracts the filepaths that need to be moved.
    """
    analysis_query_result = query(
        ANALYSES_QUERY,
        variables={
            'sequencingGroupIds': sequencing_group_ids,
            'dataset': new_dataset,
            'analysisTypes': ANALYSIS_TYPES,
        },
    )

    analyses_to_update = []
    files_to_move = []
    for sg in analysis_query_result['project']['sequencingGroups']:
        for analysis in sg['analyses']:
            if (
                analysis['type'] == 'sv'
                and analysis['meta'].get('stage') != 'GatherSampleEvidence'
            ):
                # Skip the SV analyses that are not the GatherSampleEvidence stage
                continue

            current_outputs: dict = analysis['outputs']

            new_outputs = current_outputs.copy()
            new_outputs['path'] = current_outputs['path'].replace(
                old_dataset, new_dataset
            )
            new_outputs['dirname'] = current_outputs['dirname'].replace(
                old_dataset, new_dataset
            )

            files_to_move.append((current_outputs['path'], new_outputs['path']))

            if analysis['type'] == 'cram':
                files_to_move.append(
                    (current_outputs['path'] + '.crai', new_outputs['path'] + '.crai')
                )
            if analysis['type'] == 'gvcf':
                files_to_move.append(
                    (current_outputs['path'] + '.tbi', new_outputs['path'] + '.tbi')
                )

            old_meta: dict = analysis['meta']
            new_meta = old_meta.copy()
            new_meta['dataset'] = new_dataset

            analyses_to_update.append(
                {
                    'id': analysis['id'],
                    'sg_id': sg['id'],
                    'type': analysis['type'],
                    'outputs': new_outputs,
                    'meta': new_meta,
                }
            )

    return analyses_to_update, files_to_move


def get_unrecorded_analysis_files(
    sequencing_group_id: str,
    old_dataset: str,
    new_dataset: str,
    old_bucket_name: str,
    gcp_project: str,
    storage_client: storage.Client,
):
    """
    Returns the list of filepaths which are unrecorded in the
    analysis records, but still need to be moved
    """
    files_to_move = []
    for old_path_template in UNRECORDED_ANALYSIS_FILES + UNRECORDED_ANALYSIS_FOLDERS:
        old_path = old_path_template.format(
            bucket_name=old_bucket_name,
            sg_id=sequencing_group_id,
        )
        if old_path.endswith('/'):  # If the path is a folder
            old_bucket = storage_client.bucket(
                bucket_name=old_bucket_name, user_project=gcp_project
            )
            blobs = storage_client.list_blobs(
                old_bucket,
                prefix=old_path.removeprefix(f'gs://{old_bucket.name}/'),
            )
            file_paths = [blob.path for blob in blobs]
        else:
            file_paths = [old_path]

        for path in file_paths:
            new_path = path.replace(old_dataset, new_dataset)
            files_to_move.append((old_path, new_path))

    return files_to_move


def move_files(
    files_to_move: list[tuple[str, str]],
    storage_client: storage.Client,
    gcp_project: str,
    unarchive: bool,
    dry_run: bool,
):
    """
    Moves the files from the old path to the new path.

    gcp_project should have access to both the source and destination buckets.

    If unarchive is True, and the file is in COLDLINE or ARCHIVE storage class,
    it will be updated to NEARLINE storage class before moving.
    """
    total_size = 0
    for old_path, new_path in files_to_move:
        source_bucket_name = old_path.split('/')[2]
        destination_bucket_name = new_path.split('/')[2]

        source_bucket = storage_client.bucket(
            source_bucket_name, user_project=gcp_project
        )
        destination_bucket = storage_client.bucket(
            destination_bucket_name, user_project=gcp_project
        )

        source_blob = source_bucket.blob(old_path[len(f'gs://{source_bucket_name}/') :])
        if not source_blob.exists():
            logger.error(f'Blob {old_path} not found, skipping...')
            continue
        source_blob.reload()
        total_size += source_blob.size
        source_blob_size = convert_size(source_blob.size)
        if source_blob.storage_class in ['COLDLINE', 'ARCHIVE'] and unarchive:
            logger.info(
                f'Blob {old_path} ({source_blob_size}) in {source_bucket_name} is in {source_blob.storage_class} storage class'
            )
            if dry_run:
                logger.info(
                    f'DRY RUN :: Would have updated storage class of {old_path} to NEARLINE\n'
                )
                continue
            logger.info(f'Updating storage class of {old_path} to NEARLINE\n')
            source_blob.update_storage_class('NEARLINE')

        if dry_run:
            logger.info(
                f'DRY RUN :: Would have copied {old_path} ({source_blob_size}) to {new_path}\n'
            )
            continue

        destination_blob_name = new_path[len(f'gs://{destination_bucket_name}/') :]
        if destination_bucket.get_blob(destination_blob_name):
            logger.error(
                f'Blob {destination_blob_name} already exists in bucket {destination_bucket_name}\n'
            )
            continue

        try:
            blob_copy = source_bucket.copy_blob(
                source_blob,
                destination_bucket,
                destination_blob_name,
            )
            source_bucket.delete_blob(source_blob)
            logger.info(
                f'Moved {old_path} ({convert_size(blob_copy.size)}) to {new_path} successfully\n'
            )
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'{e}: Blob {source_blob.name} failed to copy.')

    logger.info(f'{len(files_to_move)} files, total size: ({convert_size(total_size)})')
    if not dry_run:
        logger.info(f'{len(files_to_move)} files moved successfully')


def update_analyses(
    analyses_to_update: list[dict[str, Any]],
    dry_run: bool,
):
    """
    Updates the analyses with the new outputs and meta data.
    """
    analysis_api = AnalysisApi()

    promises = []
    for analysis in analyses_to_update:
        logger.info(
            f'Analysis: {analysis["id"]}. SG: {analysis["sg_id"]}. Type: {analysis["type"]}.'
        )
        logger.info(f'    New outputs path: {analysis["outputs"]["path"]}')
        logger.info(f'    New meta dataset: {analysis["meta"]["dataset"]}')
        if dry_run:
            logger.info(f'DRY RUN :: Skipping updating analysis {analysis["id"]}')
            continue
        update_model = AnalysisUpdateModel(
            status=AnalysisStatus('COMPLETED'),
            outputs=analysis['outputs'],
            meta=analysis['meta'],
        )
        promises.append(
            analysis_api.update_analysis_async(analysis['id'], update_model)
        )

    return asyncio.gather(*promises)


def convert_size(
    size: int,  # size in bytes
) -> str:
    """
    Converts the size in bytes to a human readable format.
    """
    if size > 1024**3:
        return f'{size / 1024**3:.3f} GB'
    return f'{size / 1024**2:.2f} MB'


@click.command()
@click.option(
    '--sequencing-group-ids',
    '-s',
    required=True,
    multiple=True,
    help='The sequencing group id(s)',
)
@click.option('--old-dataset', '-o', required=True, help='The old dataset name')
@click.option('--new-dataset', '-n', required=True, help='The new dataset name')
@click.option('--unarchive', '-u', is_flag=True, help='Unarchive the source files')
@click.option('--dry-run', '-d', is_flag=True, help='Dry run mode')
@run_as_sync
async def main(
    sequencing_group_ids: tuple[str],
    old_dataset: str,
    new_dataset: str,
    unarchive: bool,
    dry_run: bool,
):
    """
    Moves analysis files from one project to another and update the analysis records.
    """
    if dry_run:
        logger.info('Dry run mode enabled. No changes will be made.\n')
    logger.info(
        f'Moving analyses across projects for sequencing group(s): {sorted(sequencing_group_ids)}'
    )
    logger.info(f'From dataset: {old_dataset}')
    logger.info(f'To dataset:   {new_dataset}\n')

    storage_client = storage.Client()
    gcp_project = config_retrieve(['workflow', 'dataset_gcp_project'])
    access_level = config_retrieve(['workflow', 'access_level'])
    bucket_access_level = 'main' if access_level == 'full' else 'test'

    old_bucket_name = f'cpg-{old_dataset.replace("-test", "")}-{bucket_access_level}'
    new_bucket_name = f'cpg-{new_dataset.replace("-test", "")}-{bucket_access_level}'

    logger.info(f'Moving files from {old_bucket_name} to {new_bucket_name}\n')

    # Get the analyses to update
    analyses_to_update, files_to_move = get_analyses_to_update_and_files_to_move(
        list(sequencing_group_ids), old_dataset, new_dataset
    )

    # Add the unrecorded analysis files to the files to move
    for sg_id in sequencing_group_ids:
        files_to_move += get_unrecorded_analysis_files(
            sg_id,
            old_dataset,
            new_dataset,
            old_bucket_name,
            gcp_project,
            storage_client,
        )

    # Move the files
    move_files(files_to_move, storage_client, gcp_project, unarchive, dry_run)

    # Update the analyses
    await update_analyses(analyses_to_update, dry_run)


if __name__ == '__main__':
    asyncio.run(main())  # pylint: disable=no-value-for-parameter
