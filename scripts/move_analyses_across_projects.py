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
import sys

import asyncio
import click

from cpg_utils.config import config_retrieve
from google.cloud import storage
from metamist.apis import AnalysisApi
from metamist.models import AnalysisUpdateModel, AnalysisStatus
from metamist.graphql import gql, query
from metamist.parser.generic_metadata_parser import run_as_sync

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
    'gs://cpg-{dataset}-{bucket_access_level}/mito/{sg_id}.base_level_coverage.tsv',
    'gs://cpg-{dataset}-{bucket_access_level}/mito/{sg_id}.mito.cram',
    'gs://cpg-{dataset}-{bucket_access_level}/mito/{sg_id}.mito.cram.crai',
    'gs://cpg-{dataset}-{bucket_access_level}/mito/{sg_id}.mito.vcf.bgz',
    'gs://cpg-{dataset}-{bucket_access_level}/mito/{sg_id}.mito.vcf.bgz.tbi',
    'gs://cpg-{dataset}-{bucket_access_level}/mito/{sg_id}.mito.vep.vcf.gz',
    'gs://cpg-{dataset}-{bucket_access_level}/mito/{sg_id}.mito.vep.vcf.gz.tbi',
    'gs://cpg-{dataset}-{bucket_access_level}/mito/{sg_id}.shifted_mito.cram',
    'gs://cpg-{dataset}-{bucket_access_level}/mito/{sg_id}.shifted_mito.cram.crai',
    # CramQc stage files
    'gs://cpg-{dataset}-{bucket_access_level}/qc/{sg_id}.variant_calling_detail_metrics',
    'gs://cpg-{dataset}-{bucket_access_level}/qc/{sg_id}.variant_calling_summary_metrics',
    'gs://cpg-{dataset}-{bucket_access_level}/qc/{sg_id}_fastqc.html',
    'gs://cpg-{dataset}-{bucket_access_level}/qc/{sg_id}_fastqc.zip',
    'gs://cpg-{dataset}-{bucket_access_level}/qc/{sg_id}_picard_wgs_metrics.csv',
    'gs://cpg-{dataset}-{bucket_access_level}/qc/{sg_id}_samtools_stats.txt',
    'gs://cpg-{dataset}-{bucket_access_level}/qc/{sg_id}_verify_bamid.selfSM',
    'gs://cpg-{dataset}-{bucket_access_level}/qc/verify_bamid/{sg_id}.verify-bamid.selfSM',
    'gs://cpg-{dataset}-{bucket_access_level}/qc/samtools_stats/{sg_id}.samtools-stats',
    'gs://cpg-{dataset}-{bucket_access_level}/qc/alignment_summary_metrics/{sg_id}.alignment_summary_metrics',
    'gs://cpg-{dataset}-{bucket_access_level}/qc/base_distribution_by_cycle_metrics/{sg_id}.base_distribution_by_cycle_metrics',
    'gs://cpg-{dataset}-{bucket_access_level}/qc/insert_size_metrics/{sg_id}.insert_size_metrics',
    'gs://cpg-{dataset}-{bucket_access_level}/qc/quality_by_cycle_metrics/{sg_id}.quality_by_cycle_metrics',
    'gs://cpg-{dataset}-{bucket_access_level}/qc/quality_yield_metrics/{sg_id}.quality_yield_metrics',
    'gs://cpg-{dataset}-{bucket_access_level}/qc/picard_wgs_metrics/{sg_id}.picard-wgs-metrics',
    # Single Sample SV stage files
    'gs://cpg-{dataset}-{bucket_access_level}/sv_evidence/{sg_id}.coverage_counts.tsv.gz',
    'gs://cpg-{dataset}-{bucket_access_level}/sv_evidence/{sg_id}.pe.txt.gz',
    'gs://cpg-{dataset}-{bucket_access_level}/sv_evidence/{sg_id}.pe.txt.gz.tbi',
    'gs://cpg-{dataset}-{bucket_access_level}/sv_evidence/{sg_id}.sd.txt.gz',
    'gs://cpg-{dataset}-{bucket_access_level}/sv_evidence/{sg_id}.sd.txt.gz.tbi',
    'gs://cpg-{dataset}-{bucket_access_level}/sv_evidence/{sg_id}.sr.txt.gz',
    'gs://cpg-{dataset}-{bucket_access_level}/sv_evidence/{sg_id}.sr.txt.gz.tbi',
    # Somalier stage files
    'gs://cpg-{dataset}-{bucket_access_level}/cram/{sg_id}.cram.somalier',
    'gs://cpg-{dataset}-{bucket_access_level}/gvcf/{sg_id}.g.vcf.gz.somalier',
    # Stripy stage files
    'gs://cpg-{dataset}-{bucket_access_level}-analysis/stripy/{sg_id}.stripy.json',
    'gs://cpg-{dataset}-{bucket_access_level}-analysis/stripy/{sg_id}.stripy.log.txt',
    # MitoReport stage files
    'gs://cpg-{dataset}-{bucket_access_level}-analysis/mito/{sg_id}.coverage_mean.txt',
    'gs://cpg-{dataset}-{bucket_access_level}-analysis/mito/{sg_id}.coverage_median.txt',
    'gs://cpg-{dataset}-{bucket_access_level}-analysis/mito/{sg_id}.coverage_metrics.txt',
    'gs://cpg-{dataset}-{bucket_access_level}-analysis/mito/{sg_id}.haplocheck.txt',
    'gs://cpg-{dataset}-{bucket_access_level}-analysis/mito/{sg_id}.theoretical_sensitivity.txt',
]

UNRECORDED_ANALYSIS_FOLDERS = [
    # MitoReport stage folder
    'gs://cpg-{dataset}-{bucket_access_level}-web/mito/mitoreport-{sg_id}/',
]


def get_analyses_to_update_and_files_to_move(
    sequencing_group_ids: list[str],
    old_dataset: str,
    new_dataset: str,
) -> tuple[list[dict[str, Any]], list[tuple[str, str]]]:
    """
    Returns the analyses to update and the files to move.
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
                    'status': 'COMPLETED',
                    'new_outputs': new_outputs,
                    'new_meta': new_meta,
                    'old_outputs': current_outputs,
                    'old_meta': old_meta,
                }
            )

    return analyses_to_update, files_to_move


def move_files(
    files_to_move: list[tuple[str, str]],
    dry_run: bool,
):
    """
    Moves the files from the old path to the new path.
    """
    storage_client = storage.Client()

    for old_path, new_path in files_to_move:
        if dry_run:
            logging.info(f'DRY RUN :: Would have copied {old_path} to {new_path}')
            continue
        logging.info(f'Copying {old_path} to {new_path}')

        source_bucket_name = old_path.split('/')[2]
        destination_bucket_name = new_path.split('/')[2]

        source_bucket = storage_client.bucket(source_bucket_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)

        source_blob = source_bucket.blob(old_path[len(f'gs://{source_bucket_name}/') :])
        destination_blob_name = new_path[len(f'gs://{destination_bucket_name}/') :]

        if destination_bucket.get_blob(destination_blob_name):
            logging.error(
                f'Blob {destination_blob_name} already exists in bucket {destination_bucket_name}'
            )
            continue

        # Assume the destination blob does not exist
        destination_generation_match_precondition = 0

        try:
            blob_copy = source_bucket.copy_blob(
                source_blob,
                destination_bucket,
                destination_blob_name,
                if_generation_match=destination_generation_match_precondition,
            )
            source_bucket.delete_blob(source_blob)
            print(
                'Blob {} in bucket {} moved to blob {} in bucket {}.'.format(  # pylint: disable=consider-using-f-string
                    source_blob.name,
                    source_bucket.name,
                    blob_copy.name,
                    destination_bucket.name,
                )
            )
        except Exception as e:  # pylint: disable=broad-except
            logging.error(f'{e}: Blob {source_blob.name} failed to copy.')


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
        logging.info(f'Analysis {analysis["id"]} - new outputs and meta fields:')
        logging.info(f'    New outputs path: {analysis["outputs"]["path"]}')
        logging.info(f'    New meta dataset: {analysis["meta"]["dataset"]}')
        if dry_run:
            logging.info(f'DRY RUN :: Skipping updating analysis {analysis["id"]}')
            continue
        update_model = AnalysisUpdateModel(
            status=AnalysisStatus(analysis['status']),
            outputs=analysis['new_outputs'],
            meta=analysis['new_meta'],
        )
        promises.append(
            analysis_api.update_analysis_async(analysis['id'], update_model)
        )

    return asyncio.gather(*promises)


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
@click.option('--dry-run', '-d', is_flag=True, help='Dry run mode')
@run_as_sync
async def main(
    sequencing_group_ids: tuple[str],
    old_dataset: str,
    new_dataset: str,
    dry_run: bool,
):
    """
    Moves analysis files from one project to another and update the analysis records.
    """
    if dry_run:
        logging.info('Dry run mode enabled. No changes will be made.\n')
    logging.info(
        f'Move analyses across projects for sequencing group(s): {sorted(sequencing_group_ids)}'
    )
    logging.info(f'Moving analyses from dataset {old_dataset} to dataset {new_dataset}')

    access_level = config_retrieve(['workflow', 'access_level'])
    bucket_access_level = 'main' if access_level == 'full' else 'test'

    logging.info(
        f'Will copy files from cpg-{old_dataset}-{bucket_access_level} to cpg-{new_dataset}-{bucket_access_level}'
    )

    # Get the analyses to update
    analyses_to_update, files_to_move = get_analyses_to_update_and_files_to_move(
        list(sequencing_group_ids), old_dataset, new_dataset
    )

    storage_client = storage.Client()

    # Add the unrecorded analysis files to the files to move
    for sequencing_group_id in sequencing_group_ids:
        for old_path_template in UNRECORDED_ANALYSIS_FILES:
            old_path = old_path_template.format(
                dataset=old_dataset,
                bucket_access_level=bucket_access_level,
                sg_id=sequencing_group_id,
            )
            new_path = old_path_template.format(
                dataset=new_dataset,
                bucket_access_level=bucket_access_level,
                sg_id=sequencing_group_id,
            )

            files_to_move.append((old_path, new_path))

    # Add the unrecored analysis folders to the files to move
    for sequencing_group_id in sequencing_group_ids:
        for old_folder_template in UNRECORDED_ANALYSIS_FOLDERS:
            old_folder = old_folder_template.format(
                dataset=old_dataset,
                bucket_access_level=bucket_access_level,
                sg_id=sequencing_group_id,
            )

            old_bucket = storage_client.bucket(old_folder.split('/')[2])
            blobs = storage_client.list_blobs(
                old_bucket,
                prefix=old_folder.removeprefix(f'gs://{old_bucket.name}/'),
            )
            for source_blob in blobs:
                files_to_move.append(
                    (
                        source_blob.name,
                        source_blob.name.replace(old_dataset, new_dataset),
                    )
                )

    # Move the files
    move_files(files_to_move, dry_run)
    logging.info(f'{len(files_to_move)} Files moved successfully')

    # Update the analyses
    await update_analyses(analyses_to_update, dry_run)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stderr,
    )

    main()  # pylint: disable=no-value-for-parameter
