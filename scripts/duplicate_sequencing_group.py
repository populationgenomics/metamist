"""
For creating duplicate sequencing groups in a different dataset.

Useful for maintaining the original dataset while permitting re-analysis
of the sequencing group, but from a different project/dataset.

Steps:
1. User provides original sequencing group ID, source dataset, new dataset. Optionally,
    user can provide a sample ID and participant ID to associate with the new sequencing group.
2. Script queries the original dataset for the sequencing group and its associated analysis files.
3. Script creates a new sequencing group in the new dataset, copying over the analysis files to
    the new dataset's storage bucket(s), replacing the source sequencing group ID with the new one.
4. Script then invokes batch jobs to update the copied analysis files to reflect the new sequencing group ID.
    This includes samtools/bcftools reheader operations within the file headers for crams and vcfs,
    and find-and-replace operations within the text based analysis files and web reports.
5. Finally, the script registers the copied analysis files under the new sequencing group, completing the duplication process.
6. (Optional) If sample ID and participant ID were provided, the script associates the new sequencing group
    with the specified sample and participant in the new dataset. (This will be checked for validity on script run.)
"""

import argparse
import logging
import os
import sys


from typing import Any, Tuple

from google.cloud import storage

from cpg_utils import Path, to_path
from cpg_utils.config import config_retrieve, image_path
from cpg_utils.hail_batch import get_batch

from metamist.apis import AnalysisApi, SampleApi, SequencingGroupApi
from metamist.graphql import gql, query_async
from metamist.models import (
    Analysis,
    AnalysisStatus,
    AssayUpsert,
    SampleUpsert,
    SequencingGroupUpsert,
)
import asyncio

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

# GraphQL queries
PARTICIPANT_SAMPLES_QUERY = gql(
    """
    query myQuery($dataset: String!) {
        project(name: $dataset) {
            participants {
                id
                externalIds
                samples {
                    id
                    externalIds
                }
            }
        }
    }
    """
)

SG_DATA_QUERY = gql(
    """
    query sgAnalyses($sequencingGroupId: String!, $dataset: String!, $analysisTypes: [String!]) {
        project(name: $dataset) {
            sequencingGroups(id: {eq: $sequencingGroupId}) {
                id
                type
                technology
                platform
                assays {
                    id
                    type
                    meta
                }
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
    'gs://{bucket_name}/sv_evidence/{sg_id}.sd.txt.gz',
    'gs://{bucket_name}/sv_evidence/{sg_id}.sr.txt.gz',
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


async def get_analyses_to_upsert(
    source_dataset: str,
    source_sequencing_group_id: str,
    new_dataset: str,
    new_sequencing_group_id: str,
) -> tuple[list[dict[str, Any]], list[tuple[Path, Path]]]:
    """
    Queries Metamist for the analyses from the source SG that need to be copied,
    and extracts their filepaths to be renamed and copied to the new SG.
    """
    analysis_query_result = await query_async(
        SG_DATA_QUERY,
        variables={
            'sequencingGroupId': source_sequencing_group_id,
            'dataset': new_dataset,
            'analysisTypes': ANALYSIS_TYPES,
        },
    )

    analyses_to_copy = []
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

            # Create the new outputs by replacing the source dataset and SG ID with the new ones
            new_outputs = current_outputs.copy()
            new_outputs['path'] = (
                current_outputs['path']
                .replace(source_dataset, new_dataset)
                .replace(source_sequencing_group_id, new_sequencing_group_id)
            )
            new_outputs['dirname'] = (
                current_outputs['dirname']
                .replace(source_dataset, new_dataset)
                .replace(source_sequencing_group_id, new_sequencing_group_id)
            )

            # Build the (source_path, new_path) tuples for moving the files
            files_to_move.append(
                (to_path(current_outputs['path']), to_path(new_outputs['path']))
            )

            # Update the analysis meta with the new dataset and SG ID
            source_meta: dict = analysis['meta']
            new_meta = source_meta.copy()
            for key, value in source_meta.items():
                if isinstance(value, str):
                    new_value = value.replace(source_dataset, new_dataset).replace(
                        source_sequencing_group_id, new_sequencing_group_id
                    )
                    new_meta[key] = new_value

            new_meta['dataset'] = new_dataset
            new_meta['sequencing_group_id'] = new_sequencing_group_id

            analyses_to_copy.append(
                {
                    'sg_id': sg['id'],
                    'type': analysis['type'],
                    'outputs': new_outputs,
                    'meta': new_meta,
                }
            )

    return analyses_to_copy, files_to_move


def get_unrecorded_analysis_files(
    source_dataset: str,
    source_sequencing_group_id: str,
    new_dataset: str,
    new_sequencing_group_id: str,
) -> list[tuple[Path, Path]]:
    """
    Returns the list of filepaths which are unrecorded in the
    analysis records, but still need to be copied over to the new dataset.
    """
    storage_client = storage.Client()
    billing_project = config_retrieve(['workflow', 'dataset_gcp_project'])

    if config_retrieve(['workflow', 'access_level']) == 'test':
        source_bucket_name = f'cpg-{source_dataset}'
    else:
        source_bucket_name = f'cpg-{source_dataset}-main'

    files_to_move = []
    for source_path_template in UNRECORDED_ANALYSIS_FILES + UNRECORDED_ANALYSIS_FOLDERS:
        source_path = source_path_template.format(
            bucket_name=source_bucket_name,
            sg_id=source_sequencing_group_id,
        )
        if source_path.endswith('/'):  # If the path is a folder
            source_bucket = storage_client.bucket(
                bucket_name=source_bucket_name,
                user_project=billing_project,
            )
            blobs = storage_client.list_blobs(
                source_bucket,
                prefix=source_path.removeprefix(f'gs://{source_bucket.name}/'),
            )
            file_paths = [blob.path for blob in blobs]
        else:
            file_paths = [source_path]  # Single file

        for source_path in file_paths:  # Rename and add to move list
            new_path = source_path.replace(source_dataset, new_dataset).replace(
                source_sequencing_group_id, new_sequencing_group_id
            )
            files_to_move.append((to_path(source_path), to_path(new_path)))
    return files_to_move


async def upsert_analyses_records(
    dataset: str,
    analyses_to_upsert: list[dict[str, Any]],
    dry_run: bool,
):
    """
    Updates the analyses with the new outputs and meta data.
    """
    analysis_api = AnalysisApi()

    promises = []
    for analysis in analyses_to_upsert:
        logger.info(
            f'New Analysis for SG: {analysis["sg_id"]}, Type: {analysis["type"]}.'
        )
        logger.info(f'    New outputs path: {analysis["outputs"]["path"]}')
        logger.info(f'    New meta dataset: {analysis["meta"]["dataset"]}')
        if dry_run:
            logger.info(
                f'DRY RUN :: Skipping analysis creation for {analysis["type"]} analysis.'
            )
            continue
        analysis_to_create = Analysis(
            type=analysis['type'],
            status=AnalysisStatus('completed'),
            outputs=analysis['outputs'],
            meta=analysis['meta'],
            sequencing_group_ids=[analysis['sg_id']],
        )
        promises.append(analysis_api.create_analysis_async(dataset, analysis_to_create))

    return asyncio.gather(*promises)


async def get_participant_sample(
    dataset: str, new_participant_id: str | None, new_sample_id: str | None
) -> Tuple[int, str | None]:
    """
    Fetch a participant and sample from the specified dataset and validate their existence.
    """
    response = await query_async(PARTICIPANT_SAMPLES_QUERY, {'dataset': dataset})
    return validate_ids(new_participant_id, new_sample_id, response['project']['participants'])


def validate_ids(
    new_participant_id: str | None,
    new_sample_id: str | None,
    participants: Any,
) -> Tuple[int, str | None]:
    """
    Validate that the provided sample ID and participant ID exist and are consistent.

    Inputs can be either internal IDs or external IDs. Returns the internal IDs.
    """
    for participant in participants:
        if (
            new_participant_id in participant['externalIds'].values()
            or new_participant_id == participant['id']
        ):
            for sample in participant['samples']:
                if (
                    new_sample_id in sample['externalIds'].values()
                    or new_sample_id == sample['id']
                ):
                    return participant['id'], sample['id']

            return participant[
                'id'
            ], None  # Participant found, sample not found, so create new sample

    raise ValueError(
        'Provided sample ID and participant ID are not consistent or do not exist.'
    )


async def get_sample_upsert(
    source_dataset: str,
    original_sg_id: str,
    participant_id: int | None = None,
    sample_id: str | None = None,
) -> SampleUpsert:
    """
    Prepares a SampleUpsert object with optional participant and sample associations.
    Returns with the upsert model with nested sequencing group and assays.
    """
    # Fetch the original sequencing group data using the original_sg_id
    response = await query_async(
        SG_DATA_QUERY, {'dataset': source_dataset, 'sequencingGroupId': original_sg_id, 'analysisTypes': []}
    )
    original_sg_data = response['project']['sequencingGroups'][0]
    assays = [
        AssayUpsert(
            id=None,
            type=assay['type'],
            meta=assay[
                'meta'
            ],  # probably need to update some of these fields, e.g. file paths?
        )
        for assay in original_sg_data['assays']
    ]
    sg = SequencingGroupUpsert(
        id=None,
        type=original_sg_data['type'],
        technology=original_sg_data['technology'],
        platform=original_sg_data['platform'],
        assays=assays,
    )
    sample = SampleUpsert(
        id=sample_id, participant_id=participant_id, sequencing_groups=[sg]
    )
    return sample


def file_size(path: str) -> int:
    """Return the size (in bytes) of the object, which is assumed to exist"""
    if path.startswith('gs://'):
        (bucket, path) = path.removeprefix('gs://').split('/', maxsplit=1)
        return storage.Client().get_bucket(bucket).get_blob(path).size

    return os.path.getsize(path)


def file_reheaderable(path: str) -> bool:
    """Returns whether this is a file that can be reheadered with samtools/bcftools"""
    return any(
        path.endswith(e)
        for e in [
            '.cram',
            '.vcf.gz',
            '.vcf.bgz',
            '.g.vcf.gz',
        ]
    )


def file_tabixable(path: str) -> bool:
    """Returns whether this is a file that should be indexed with tabix"""
    return any(path.endswith(e) for e in ['.pe.txt.gz', '.sd.txt.gz', '.sr.txt.gz'])


async def reheader_analysis_files_in_batch(
    source_sequencing_group_id: str,
    new_sequencing_group_id: str,
    source_paths_new_paths: list[tuple[Path, Path]],
):
    """
    Reheader analysis files in Hail Batch to update the sequencing group IDs within the file headers.

    Uses samtools for CRAM files and bcftools for VCF/GVCF files.
    For other text based files, performs a simple find-and-replace operation with sed.
    """
    batch = get_batch()
    for old_path, new_path in source_paths_new_paths:
        logger.info(
            f'Adding job to replace {source_sequencing_group_id} with {new_sequencing_group_id} in {new_path!s}.'
        )
        if str(old_path).endswith(
            '.somalier'
        ):  # special case for binary somalier files
            somalier_file_commands(
                str(old_path),
                str(new_path),
                (source_sequencing_group_id, new_sequencing_group_id),
            )
            continue
        job = batch.new_job(f'Replace SG ID in {new_path!s}')
        if file_reheaderable(str(old_path)):
            job.storage(file_size(old_path) + 2 * 1024**3)  # Allow 2 GiB extra
            main_file_commands(
                batch,
                job,
                str(old_path),
                str(new_path),
                (source_sequencing_group_id, new_sequencing_group_id),
            )
        elif str(old_path).endswith('.gz'):
            job.storage(file_size(old_path) + 512 * 1024**2)  # Allow 512 MiB extra
            gzipped_file_commands(
                batch,
                job,
                str(old_path),
                str(new_path),
                (source_sequencing_group_id, new_sequencing_group_id),
            )
        else:
            text_file_commands(
                batch,
                job,
                str(old_path),
                str(new_path),
                (source_sequencing_group_id, new_sequencing_group_id),
            )

    batch.run(wait=True)


def main_file_commands(batch, job, old_path: str, new_path: str, sid: tuple[str, str]):
    """
    Adds the commands required to reheader the analysis files.

    Uses samtools/bcftools for CRAM/VCF/GVCF files, also queueing the regeneration of
    associated secondary files, such as .crai and .tbi files.

    Uses sed for find-and-replace in other text based files, gunzipping and gzipping
    as necessary for gzipped text files.

    Handles
    """
    if new_path.endswith('.cram'):
        job.image(image_path('samtools'))
        old_file = batch.read_input(old_path)
        job.declare_resource_group(
            new_file={'outfile': '{root}.cram', 'crai': '{root}.cram.crai'}
        )
        job.command(rf"""
            samtools head {old_file} | sed '/^@RG/ s/{sid[0]}/{sid[1]}/g' > $BATCH_TMPDIR/header.txt
            samtools reheader --no-PG --in-place $BATCH_TMPDIR/header.txt {old_file}
            mv {old_file} {job.new_file.outfile}
        """)
        secondary_file_commands(batch, job, new_path + '.crai')
        batch.write_output(job.new_file.outfile, new_path)

    elif new_path.endswith(('.vcf.gz', '.vcf.bgz', '.g.vcf.gz')):
        job.image(image_path('bcftools'))
        old_file = batch.read_input(old_path)
        if new_path.endswith('.g.vcf.gz'):  # Handling both VCF and GVCF
            job.declare_resource_group(
                new_file={'outfile': '{root}.g.vcf.gz', 'tbi': '{root}.g.vcf.gz.tbi'}
            )
        elif new_path.endswith('.vcf.gz'):
            job.declare_resource_group(
                new_file={'outfile': '{root}.vcf.gz', 'tbi': '{root}.vcf.gz.tbi'}
            )
        elif new_path.endswith('.vcf.bgz'):
            job.declare_resource_group(
                new_file={'outfile': '{root}.vcf.bgz', 'tbi': '{root}.vcf.bgz.tbi'}
            )
        job.command(rf"""
            bcftools head {old_file} | sed '/^#CHROM/ s/{sid[0]}/{sid[1]}/' > $BATCH_TMPDIR/header.txt
            bcftools reheader --header $BATCH_TMPDIR/header.txt -o {job.new_file.outfile} {old_file}
        """)
        secondary_file_commands(batch, job, new_path + '.tbi')
        batch.write_output(job.new_file.outfile, new_path)


def secondary_file_commands(batch, job, new_path: str):
    """Adds the commands required to regenerate the various associated secondary files"""
    if new_path.endswith('.crai'):
        job.command(rf'samtools index -o {job.new_file.crai} {job.new_file.outfile}')
        batch.write_output(job.new_file.crai, new_path)

    elif new_path.endswith('.tbi'):
        job.command(rf"""
            tabix {job.new_file.outfile}
            # Mention {job.new_file.tbi} for hail as tabix has no -o option
        """)
        batch.write_output(job.new_file.tbi, new_path)

    elif new_path.endswith('.md5'):
        job.command(rf"md5sum {job.new_file.outfile} | cut -d' ' -f1 > {job.new_md5}")
        batch.write_output(job.new_md5, new_path)


def somalier_file_commands(old_path: str, new_path: str, sid: tuple[str, str]):
    """Uses python to read and rewrite the binary somalier files with updated SG IDs"""
    logger.info(f'Reheadering somalier file for {old_path}, writing to {new_path}')
    data = to_path(old_path).read_bytes()
    # Extract the old length and sample ID
    sample_L_old = int.from_bytes(data[1:2], byteorder='little')
    sample_id_start_idx = 2
    sample_id_end_idx = sample_id_start_idx + sample_L_old
    current_sample_id = data[sample_id_start_idx:sample_id_end_idx].decode()

    # Check that the current sample ID matches the expected old SG ID
    if current_sample_id != sid[0]:
        raise ValueError(
            f'Unexpected sample ID in somalier file: {current_sample_id}, expected: {sid[0]}'
        )

    # Create the new sample ID bytes
    new_sample_bytes = sid[1].encode()
    sample_L_new = len(new_sample_bytes)

    # The file is built by concatenating four parts:
    # 1. Fixed header: Version byte (1 byte)
    header_version = data[:1]
    # 2. Variable header: New sample ID length (1 byte)
    header_length_new = sample_L_new.to_bytes(1, byteorder='little')
    # 3. Variable data: New sample ID string (N bytes)
    data_new_sample_id = new_sample_bytes
    # 4. Remaining file data (from after the old ID)
    remaining_data = data[sample_id_end_idx:]

    # Combine the pieces
    new_data = header_version + header_length_new + data_new_sample_id + remaining_data

    # --- 4. Write the New File ---
    to_path(new_path).write_bytes(new_data)
    logger.info(f"Successfully replaced ID '{sid[0]}' with '{sid[1]}' in {new_path}")


def gzipped_file_commands(
    batch, job, old_path: str, new_path: str, sid: tuple[str, str]
):
    """Adds the commands required to regenerate gzipped files"""
    job.image(image_path('driver_image'))
    old_file = batch.read_input(old_path)
    # Simple gunzip and gzip to regenerate the gzipped file after sed replacement
    job.command(rf"""
        gunzip -c {old_file} | sed "s/{{{sid[0]}}}/{{{sid[1]}}}/g" | gzip > {job.new_file}
    """)
    if file_tabixable(new_path):
        job.command(rf"""
            tabix {job.new_file}
            # Mention {job.new_file}.tbi for hail as tabix has no -o option
        """)
        batch.write_output(job.new_file.tbi, new_path + '.tbi')
    batch.write_output(job.new_file.outfile, new_path)


def text_file_commands(batch, job, old_path: str, new_path: str, sid: tuple[str, str]):
    """Adds the commands required to regenerate the various text based files"""
    # Simple find and replace for all other text based files
    job.image(image_path('driver_image'))
    old_file = batch.read_input(old_path)
    job.command(rf"""
        old_sg_id="{sid[0]}"
        new_sg_id="{sid[1]}"
        sed "s/{{${{old_sg_id}}}}/{{${{new_sg_id}}}}/g" {old_file} > {job.new_file}
    """)
    batch.write_output(job.new_file, new_path)


async def main(
    original_sg_id: str,
    source_dataset: str,
    new_dataset: str,
    new_sample_id: str = None,
    new_participant_id: str = None,
    deactivate_original_sg: bool = False,
) -> None:
    """
    Duplicates a sequencing group from one dataset to another, optionally using a new sample and participant ID.

    Copies and reheaders analysis files accordingly, replacing the original sequencing group ID with the new one.

    Args:
        original_sg_id (str): The original sequencing group ID to duplicate.
        source_dataset (str): The dataset of the original sequencing group.
        new_dataset (str): The dataset to create the new sequencing group in.
        new_sample_id (str, optional): Optional sample ID (or external sample ID) to associate with the new sequencing group.
        new_participant_id (str, optional): Optional participant ID (or external participant ID) to associate with the new sequencing group.
        deactivate_original_sg (bool, optional): Optional flag to deactivate the original sequencing group.
    """
    if new_sample_id or new_participant_id:
        # Validate and save these values for later input into the upsert call if they are provided.
        participant_id, sample_id = await get_participant_sample(
            new_dataset, new_participant_id, new_sample_id
        )
    else:
        participant_id, sample_id = None, None

    # Prepare the sample upsert for the new dataset.
    sample_upsert = await get_sample_upsert(
        source_dataset, original_sg_id, participant_id, sample_id
    )

    # Upsert the new sequencing group (nested within the sample upsert) and get the new SG ID.
    logger.info(
        f'Prepared sample upsert for new dataset {new_dataset}: {sample_upsert}'
    )
    new_sample = await SampleApi().upsert_samples_async(new_dataset, [sample_upsert])
    new_sequencing_group_id = new_sample[0]['sequencing_groups'][0]['id']

    # Fetch the analysis file paths
    logger.info(
        f'Fetching analyses to copy from dataset {source_dataset} for SG ID {original_sg_id} to new SG ID {new_sequencing_group_id} in dataset {new_dataset}.'
    )
    analyses_to_upsert, source_paths_new_paths = await get_analyses_to_upsert(
        source_dataset,
        original_sg_id,
        new_dataset,
        new_sequencing_group_id,
    )
    logger.info(f'Found {len(analyses_to_upsert)} analyses to copy.')

    # Fetch unrecorded analysis file paths
    unrecorded_file_moves = get_unrecorded_analysis_files(
        source_dataset,
        original_sg_id,
        new_dataset,
        new_sequencing_group_id,
    )
    source_paths_new_paths.extend(unrecorded_file_moves)

    logger.info(f'Total files to move: {len(source_paths_new_paths)}')

    # Reheader and update analysis files with hail batch, replacing SG IDs within the files
    logger.info('Reheadering analysis files in batch to update SG IDs.')
    await reheader_analysis_files_in_batch(
        source_sequencing_group_id=original_sg_id,
        new_sequencing_group_id=new_sequencing_group_id,
        source_paths_new_paths=source_paths_new_paths,
    )

    # Upsert the analyses with the new outputs and meta data
    logger.info(f'Upserting analyses to new dataset {new_dataset}.')
    await upsert_analyses_records(
        dataset=new_dataset,
        analyses_to_upsert=analyses_to_upsert,
        dry_run=False,
    )

    # Optionally deactivate the original sequencing group
    if deactivate_original_sg:
        logger.info(
            f'Deactivating original sequencing group {original_sg_id} in dataset {source_dataset}.'
        )
        SequencingGroupApi().archive_sequencing_groups([original_sg_id])
        logger.info(f'Original sequencing group {original_sg_id} deactivated.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Duplicate a sequencing group from one dataset to another, copying analysis files accordingly.'
    )
    parser.add_argument(
        'original_sg_id', type=str, help='Original sequencing group ID to duplicate.'
    )
    parser.add_argument(
        'source_dataset', type=str, help='Dataset of the original sequencing group.'
    )
    parser.add_argument(
        'new_dataset', type=str, help='Dataset to create the new sequencing group in.'
    )
    parser.add_argument(
        '--new_sample_id',
        type=str,
        default=None,
        help='Optional sample ID (or external sample ID) to associate with the new sequencing group.',
    )
    parser.add_argument(
        '--new_participant_id',
        type=str,
        default=None,
        help='Optional participant ID (or external participant ID) to associate with the new sequencing group. Must be consistent with the new sample if provided.',
    )
    parser.add_argument(
        '--deactivate_original_sg',
        type=bool,
        default=False,
        help='Optional flag to deactivate the original sequencing group.',
    )
    args = parser.parse_args()
    asyncio.run(
        main(
            original_sg_id=args.original_sg_id,
            source_dataset=args.source_dataset,
            new_dataset=args.new_dataset,
            new_sample_id=args.new_sample_id,
            new_participant_id=args.new_participant_id,
            deactivate_original_sg=bool(args.deactivate_original_sg),
        )
    )
