#!/usr/bin/env python3

"""
For creating duplicate sequencing groups in a different dataset.

Useful for maintaining the original dataset while permitting re-analysis
of the sequencing group, but from a different project/dataset.

Steps:
1. User provides original sequencing group ID, source dataset, new dataset. Optionally,
    user can provide a sample ID and participant ID to associate with the new sequencing group.
2. Script queries the original dataset for the sequencing group and its associated analysis files.
    Only single sequencing group analyses are copied, cohort-level analyses are skipped.
    Use --dry-run to log everything that would be created and copied.
3. Script creates a new sequencing group in the new dataset, copying over the analysis files to
    the new dataset's storage bucket(s). This is also done for unrecorded analysis files that are not
    listed in the analysis records.
4. Script then invokes batch jobs to update the copied analysis files to reflect the new sequencing group ID.
    This includes samtools/bcftools reheader operations within the file headers for crams and vcfs,
    and find-and-replace operations within the text based analysis files and web reports.
5. Finally, the script registers the copied analysis files under the new sequencing group, completing the duplication process.
"""

import argparse
import asyncio
import fnmatch
import logging
import sys
from typing import Any

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


logger = logging.getLogger(__file__)
logging.basicConfig(
    format='%(levelname)s (%(name)s %(lineno)s): %(message)s',
    stream=sys.stderr,
    level=logging.INFO,
)
# gql logs every GraphQL request and response at INFO, which buries the dry run output
logging.getLogger('gql').setLevel(logging.WARNING)

# ============================================================================
# CONSTANTS AND CONFIGURATION
# ============================================================================

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
                sample {
                    type
                }
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
                    sequencingGroups {
                        id
                    }
                }
            }
        }
    }
    """
)

# Stands in for the new SG ID in dry runs, as no sequencing group is created
DRY_RUN_SG_ID = 'NEW_SG_ID'

ANALYSIS_TYPES = ['cram', 'gvcf', 'web', 'sv', 'mito-cram', 'somalier', 'exomiser']

# Copied as-is rather than run through sed, which could corrupt them
BINARY_FILE_EXTENSIONS = (
    '.png',
    '.jpg',
    '.jpeg',
    '.gif',
    '.ico',
    '.pdf',
    '.woff',
    '.woff2',
    '.ttf',
)

# File paths that are not recorded in the analysis records but need to be copied.
# A '*' matches any characters, e.g. a tool version directory. Files recorded by an
# analysis as well are only copied once.
UNRECORDED_ANALYSIS_FILES = [
    # Mito CRAM files
    'gs://{bucket_name}/mito/{sg_id}.base_level_coverage.tsv',
    'gs://{bucket_name}/mito/{sg_id}.mito.cram',
    'gs://{bucket_name}/mito/{sg_id}.mito.vcf.bgz',
    'gs://{bucket_name}/mito/{sg_id}.mito.vep.vcf.gz',
    'gs://{bucket_name}/mito/{sg_id}.shifted_mito.cram',
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
    'gs://{bucket_name}/qc/markduplicates_metrics/{sg_id}.markduplicates-metrics',
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
    # Versioned Stripy stage files
    'gs://{bucket_name}/stripy/*/{sg_id}.log',
    'gs://{bucket_name}-web/stripy/*/{sg_id}__default.html',
    # Exomiser stage files
    'gs://{bucket_name}-analysis/exomiser_inputs/{sg_id}.ped',
    'gs://{bucket_name}-analysis/exomiser_inputs/{sg_id}_phenopacket.json',
    'gs://{bucket_name}-analysis/exomiser_results/{sg_id}.json',
    'gs://{bucket_name}-analysis/exomiser_results/{sg_id}.tsv',
    'gs://{bucket_name}-analysis/exomiser_results/{sg_id}.variants.tsv',
    'gs://{bucket_name}-analysis/exomiser_results/{sg_id}.yaml',
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


# ============================================================================
# DATABASE QUERY AND VALIDATION FUNCTIONS
# ============================================================================


async def get_participant_sample(
    dataset: str,
    new_participant_id: str | None,
    new_sample_id: str | None,
    new_sample_external_id: str,
) -> tuple[int, str | None]:
    """
    Fetch a participant and sample from the specified dataset and validate their existence.
    """
    response = await query_async(PARTICIPANT_SAMPLES_QUERY, {'dataset': dataset})
    return validate_ids(
        new_participant_id,
        new_sample_id,
        new_sample_external_id,
        response['project']['participants'],
    )


def validate_ids(
    new_participant_id: str | None,
    new_sample_id: str | None,
    new_sample_external_id: str,
    participants: Any,
) -> tuple[int, str | None]:
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
                    or new_sample_external_id in sample['externalIds'].values()
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
    external_sample_id: str,
    participant_id: int | None = None,
    sample_id: str | None = None,
) -> SampleUpsert:
    """
    Prepares a SampleUpsert object with optional participant and sample associations.
    Returns with the upsert model with nested sequencing group and assays.
    """
    # Fetch the original sequencing group data using the original_sg_id
    response = await query_async(
        SG_DATA_QUERY,
        {
            'dataset': source_dataset,
            'sequencingGroupId': original_sg_id,
            'analysisTypes': [],
        },
    )
    if not response['project']['sequencingGroups']:
        raise ValueError(
            f'Sequencing group {original_sg_id} not found in {source_dataset}. '
            'Only active sequencing groups can be duplicated.'
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
        meta={
            'original_sequencing_group_id': original_sg_id,
            'original_dataset': source_dataset,
        },
        platform=original_sg_data['platform'],
        assays=assays,
    )
    sample = SampleUpsert(
        id=sample_id,
        external_ids={'': external_sample_id},
        # Only set the type on new samples, so an existing sample is left unchanged
        type=None if sample_id else original_sg_data['sample']['type'],
        participant_id=participant_id,
        sequencing_groups=[sg],
    )
    return sample


# ============================================================================
# ANALYSIS DISCOVERY AND PREPARATION FUNCTIONS
# ============================================================================


def analysis_output_path(analysis: dict[str, Any]) -> str | None:
    """Returns the primary output path of an analysis, if one is recorded"""
    outputs = analysis['outputs']
    if isinstance(outputs, str):
        return outputs
    return (outputs or {}).get('path')


def analysis_skip_reason(analysis: dict[str, Any]) -> str | None:
    """
    Returns why an analysis should not be copied to the new SG, or None to copy it.
    Expects an analysis from SG_DATA_QUERY, already filtered to completed analyses
    of the types in ANALYSIS_TYPES.
    """
    n_sequencing_groups = len(analysis['sequencingGroups'])
    if n_sequencing_groups > 1:
        # e.g. dataset index pages, which would leak other samples into the new dataset
        return f'cohort-level analysis covering {n_sequencing_groups} sequencing groups'
    if analysis['type'] == 'sv' and analysis['meta'].get('stage') != (
        'GatherSampleEvidence'
    ):
        return f'sv analysis from stage {analysis["meta"].get("stage")}'
    if not analysis_output_path(analysis):
        return 'no recorded outputs path'
    return None


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
            'dataset': source_dataset,
            'analysisTypes': ANALYSIS_TYPES,
        },
    )

    analyses_to_copy = []
    files_to_move = []
    for sg in analysis_query_result['project']['sequencingGroups']:
        for analysis in sg['analyses']:
            if skip_reason := analysis_skip_reason(analysis):
                logger.info(
                    f'Skipping {analysis["type"]} analysis {analysis["id"]}: {skip_reason}'
                )
                continue

            # Create the new outputs by replacing the source dataset and SG ID with the new ones
            source_path = analysis_output_path(analysis)
            new_path = source_path.replace(source_dataset, new_dataset).replace(
                source_sequencing_group_id, new_sequencing_group_id
            )

            # Build the (source_path, new_path) tuples for moving the files
            if (
                to_path(source_path),
                to_path(new_path),
            ) in files_to_move:
                continue  # Avoid duplicates

            files_to_move.append((to_path(source_path), to_path(new_path)))

            # Update the analysis meta with the new dataset and SG ID
            source_meta: dict = analysis['meta']
            new_meta = source_meta.copy()
            new_meta['dataset'] = new_dataset
            for key, value in source_meta.items():
                if isinstance(value, str):
                    new_value = value.replace(source_dataset, new_dataset).replace(
                        source_sequencing_group_id, new_sequencing_group_id
                    )
                    new_meta[key] = new_value

            new_meta['original_dataset'] = source_dataset
            new_meta['original_analysis_id'] = analysis['id']
            new_meta['original_sequencing_group_id'] = source_sequencing_group_id

            analyses_to_copy.append(
                {
                    'sg_id': new_sequencing_group_id,
                    'type': analysis['type'],
                    'output': new_path,
                    'meta': new_meta,
                }
            )
            logger.info(
                f'Copying {analysis["type"]} analysis: {source_path} -> {new_path}'
            )

    return analyses_to_copy, files_to_move


def get_bucket_names(
    source_dataset: str, new_dataset: str, access_level: str
) -> tuple[str, str]:
    """Returns the (source, destination) main bucket names"""
    if access_level == 'test':
        source_bucket_name = f'cpg-{source_dataset}'
    else:
        source_bucket_name = f'cpg-{source_dataset}-main'

    if new_dataset.endswith('-test'):
        destination_bucket_name = f'cpg-{new_dataset}'
    else:
        destination_bucket_name = f'cpg-{new_dataset}-main'
    return source_bucket_name, destination_bucket_name


def resolve_source_paths(
    storage_client: storage.Client, path: str, billing_project: str | None
) -> list[str]:
    """
    Returns the existing files for an unrecorded path: every file under a folder
    path ending in '/', every file matching a path containing '*', otherwise the
    file itself if it exists.
    """
    bucket_name, _, name = path.removeprefix('gs://').partition('/')
    bucket = storage_client.bucket(bucket_name, user_project=billing_project)
    if name.endswith('/') or '*' in name:
        return [
            f'gs://{bucket_name}/{blob.name}'
            for blob in storage_client.list_blobs(bucket, prefix=name.split('*')[0])
            if name.endswith('/') or fnmatch.fnmatchcase(blob.name, name)
        ]
    return [path] if bucket.blob(name).exists() else []


def get_unrecorded_analysis_files(
    source_bucket_name: str,
    destination_bucket_name: str,
    source_sequencing_group_id: str,
    new_sequencing_group_id: str,
    billing_project: str | None,
    storage_client: storage.Client | None = None,
) -> list[tuple[Path, Path]]:
    """
    Returns the list of filepaths which are unrecorded in the
    analysis records, but still need to be copied over to the new dataset.
    """
    storage_client = storage_client or storage.Client()
    files_to_move = []
    for source_path_template in UNRECORDED_ANALYSIS_FILES + UNRECORDED_ANALYSIS_FOLDERS:
        path = source_path_template.format(
            bucket_name=source_bucket_name,
            sg_id=source_sequencing_group_id,
        )
        for source_path in resolve_source_paths(storage_client, path, billing_project):
            new_path = source_path.replace(
                source_bucket_name, destination_bucket_name
            ).replace(source_sequencing_group_id, new_sequencing_group_id)
            files_to_move.append((to_path(source_path), to_path(new_path)))

    return files_to_move


def dedupe_file_moves(
    source_paths_new_paths: list[tuple[Path, Path]],
) -> list[tuple[Path, Path]]:
    """Drops repeated source files, e.g. ones both recorded and in the unrecorded list"""
    seen = set()
    unique = []
    for source_path, new_path in source_paths_new_paths:
        if str(source_path) not in seen:
            seen.add(str(source_path))
            unique.append((source_path, new_path))
    return unique


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
        logger.info(f'    New output path: {analysis["output"]}')
        logger.info(f'    New meta dataset: {analysis["meta"]["dataset"]}')
        if dry_run:
            logger.info(
                f'DRY RUN :: Skipping analysis creation for {analysis["type"]} analysis.'
            )
            continue
        analysis_to_create = Analysis(
            type=analysis['type'],
            status=AnalysisStatus('completed'),
            outputs={
                'basename': analysis['output']
            },  # this is the correct syntax for autofilling outputs dict
            meta=analysis['meta'],  # add some provenance info here?
            sequencing_group_ids=[analysis['sg_id']],
        )
        promises.append(analysis_api.create_analysis_async(dataset, analysis_to_create))

    return await asyncio.gather(*promises)


# ============================================================================
# FILE UTILITY FUNCTIONS
# ============================================================================


def file_size(path: Path) -> int:
    """Return the size (in bytes) of the object, which is assumed to exist"""
    if path.exists():
        return path.stat().st_size
    return 0


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


def file_is_binary(path: str) -> bool:
    """Returns whether this is a binary file that should be copied without edits"""
    return path.lower().endswith(BINARY_FILE_EXTENSIONS)


def file_update_method(path: str) -> str:
    """Returns how a copied file is updated to carry the new SG ID"""
    if file_is_binary(path):
        return 'binary copy'
    if path.endswith('.somalier'):
        return 'somalier rewrite'
    if file_reheaderable(path):
        return 'reheader'
    if path.endswith('.gz'):
        return 'bgzip edit'
    return 'text edit'


def file_gatk_check(path: str):
    """
    Check if this is a file from GATK-SV CollectSVEvidence that needs bgzip reheadering,
    and return the (1-based) column index of the sample ID to replace.
    """
    file_dict = {
        # paired-end evidence (sample ID in column 3)
        # chr1    10000   SAMPLE_ID       174     0       0       3
        '.sd.txt.gz': 3,
        # split-read evidence (sample ID in column 5)
        # chr1    10000   left    4       SAMPLE_ID
        '.sr.txt.gz': 5,
        # discordant paired-end evidence (sample ID in column 7)
        # chr1    10000   -       chr5    10495   +       SAMPLE_ID
        '.pe.txt.gz': 7,
    }
    for suffix, sample_id_col in file_dict.items():
        if path.endswith(suffix):
            return sample_id_col
    return None


# ============================================================================
# BATCH PROCESSING AND FILE REHEADERING FUNCTIONS
# ============================================================================


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
    n_jobs = 0
    for old_path, new_path in source_paths_new_paths:
        if new_path.exists():
            logger.info(f'Skipping update for existing file: {new_path!s}')
            continue
        method = file_update_method(str(old_path))
        if method == 'binary copy':
            # e.g. report images, which don't contain the SG ID, so copy as-is
            logger.info(f'Copying binary file {old_path!s} to {new_path!s}')
            old_path.copy(new_path)
            continue
        if method == 'somalier rewrite':  # binary .somalier files, run locally
            somalier_file_commands(
                str(old_path),
                str(new_path),
                (source_sequencing_group_id, new_sequencing_group_id),
            )
            continue
        job = batch.new_job(f'Update SG ID in {old_path!s}')
        n_jobs += 1
        if method == 'reheader':
            job.storage(file_size(old_path) + 2 * 1024**3)  # Allow 2 GiB extra
            main_file_commands(
                batch,
                job,
                str(old_path),
                str(new_path),
                (source_sequencing_group_id, new_sequencing_group_id),
            )
        elif method == 'bgzip edit':
            job.storage(file_size(old_path) + 512 * 1024**2)  # Allow 512 MiB extra
            bgzipped_file_commands(
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

    if n_jobs:
        batch.run(wait=True)
    else:
        logger.info('No files need updating in batch.')


def log_file_updates(
    source_sequencing_group_id: str,
    new_sequencing_group_id: str,
    source_paths_new_paths: list[tuple[Path, Path]],
) -> None:
    """Logs how each file would be copied and updated, without writing anything"""
    total_size = 0
    for old_path, new_path in source_paths_new_paths:
        if new_path.exists():
            logger.info(f'DRY RUN :: Would skip existing file: {new_path!s}')
            continue
        method = file_update_method(str(old_path))
        size = file_size(old_path)
        total_size += size
        logger.info(
            f'DRY RUN :: {method} ({size / 1024**2:.1f} MiB): {old_path!s} -> {new_path!s}'
        )
        if method == 'somalier rewrite':
            # Check the header now, as a mismatch would stop the real run part way
            somalier_file_commands(
                str(old_path),
                str(new_path),
                (source_sequencing_group_id, new_sequencing_group_id),
                dry_run=True,
            )
    logger.info(f'DRY RUN :: {total_size / 1024**3:.1f} GiB would be copied')


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


def somalier_file_commands(
    old_path: str, new_path: str, sid: tuple[str, str], dry_run: bool = False
):
    """Uses python to read and rewrite the binary somalier files with updated SG IDs"""
    logger.info(f'Reheadering somalier file for {old_path}, writing to {new_path}')
    data = to_path(old_path).read_bytes()
    # Extract the old length and sample ID
    sample_L_old = int.from_bytes(data[1:2], byteorder='little')  # noqa: N806
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
    sample_L_new = len(new_sample_bytes)  # noqa: N806

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

    if dry_run:
        logger.info(f'DRY RUN :: Somalier sample ID {current_sample_id} checked')
        return

    # --- 4. Write the New File ---
    to_path(new_path).write_bytes(new_data)
    logger.info(f"Successfully replaced ID '{sid[0]}' with '{sid[1]}' in {new_path}")


def bgzipped_file_commands(
    batch, job, old_path: str, new_path: str, sid: tuple[str, str]
):
    """Adds the commands required to regenerate bgzipped files originally produced by GATK-SV"""
    job.image(image_path('bcftools'))
    old_file = batch.read_input(old_path)
    job.declare_resource_group(
        new_file={'outfile': '{root}.gz', 'tbi': '{root}.gz.tbi'}
    )

    if column_index := file_gatk_check(new_path):
        # The CollectSVEvidence files (pe.txt.gz, sd.txt.gz, sr.txt.gz)
        job.command(rf"""
            bgzip -c --decompress {old_file} | \
            awk -v OFS='\t' -v col={column_index} '{{{{$col="{sid[1]}"}}; print}}' | \
            bgzip -c > {job.new_file.outfile}
        """)

        # Use the tabix -0 -s1 -b2 -e2 options to index the standard 0-based coordinate files
        # See: https://github.com/populationgenomics/gatk-sv/blob/890582922bccb4b651275506a34311c949417f6c/wdl/PETestChromosome.wdl#L227
        job.command(rf"""
            tabix -0 -s1 -b2 -e2 {job.new_file.outfile}
            # Mention {job.new_file.tbi} for hail as tabix has no -o option
        """)
        batch.write_output(job.new_file.tbi, new_path + '.tbi')
    else:
        # Generic bgzipped text file find-and-replace
        job.command(rf"""
            bgzip -c --decompress {old_file} | sed "s/{sid[0]}/{sid[1]}/g" | bgzip -c > {job.new_file.outfile}
        """)

    batch.write_output(job.new_file.outfile, new_path)


def text_file_commands(batch, job, old_path: str, new_path: str, sid: tuple[str, str]):
    """Adds the commands required to regenerate the various text based files"""
    # Simple find and replace for all other text based files
    job.image(config_retrieve(['workflow', 'driver_image']))
    old_file = batch.read_input(old_path)
    job.command(rf"""
        sed "s/{sid[0]}/{sid[1]}/g" {old_file} > {job.new_file}
    """)
    batch.write_output(job.new_file, new_path)


async def main(  # noqa: D417
    original_sg_id: str,
    source_dataset: str,
    new_dataset: str,
    new_sample_external_id: str,
    new_sample_id: str = None,
    new_participant_id: str = None,
    reuse_sequencing_group_id: str = None,
    deactivate_original_sg: bool = False,
    dry_run: bool = False,
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
        reuse_sequencing_group_id (str, optional): Optional existing sequencing group ID to reuse instead of creating a new one.
        deactivate_original_sg (bool, optional): Optional flag to deactivate the original sequencing group.
        dry_run (bool, optional): Log what would be created and copied, without writing anything.
    """
    if new_sample_id or new_participant_id:
        # Validate and save these values for later input into the upsert call if they are provided.
        participant_id, sample_id = await get_participant_sample(
            new_dataset, new_participant_id, new_sample_id, new_sample_external_id
        )
    else:
        participant_id, sample_id = None, None

    if reuse_sequencing_group_id:
        logger.info(
            f'Reusing existing sequencing group ID {reuse_sequencing_group_id} in new dataset {new_dataset}.'
        )
        new_sequencing_group_id = reuse_sequencing_group_id
    else:
        logger.info(
            f'Creating new sequencing group in dataset {new_dataset} from original SG ID {original_sg_id} in dataset {source_dataset}.'
        )
        # Prepare the sample upsert for the new dataset.
        sample_upsert = await get_sample_upsert(
            source_dataset,
            original_sg_id,
            new_sample_external_id,
            participant_id,
            sample_id,
        )

        # Upsert the new sequencing group (nested within the sample upsert) and get the new SG ID.
        logger.info(
            f'Prepared sample upsert for new dataset {new_dataset}: {sample_upsert}'
        )
        if dry_run:
            logger.info(
                f'DRY RUN :: Skipping sample upsert, using {DRY_RUN_SG_ID} for the new SG ID.'
            )
            new_sequencing_group_id = DRY_RUN_SG_ID
        else:
            new_sample = await SampleApi().upsert_samples_async(
                new_dataset, [sample_upsert]
            )
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
    source_bucket_name, destination_bucket_name = get_bucket_names(
        source_dataset,
        new_dataset,
        config_retrieve(['workflow', 'access_level']),
    )
    unrecorded_file_moves = get_unrecorded_analysis_files(
        source_bucket_name,
        destination_bucket_name,
        original_sg_id,
        new_sequencing_group_id,
        billing_project=config_retrieve(['workflow', 'dataset_gcp_project']),
    )
    source_paths_new_paths = dedupe_file_moves(
        source_paths_new_paths + unrecorded_file_moves
    )

    logger.info(f'Total files to move: {len(source_paths_new_paths)}')

    # Reheader and update analysis files with hail batch, replacing SG IDs within the files
    if dry_run:
        log_file_updates(
            original_sg_id, new_sequencing_group_id, source_paths_new_paths
        )
    else:
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
        dry_run=dry_run,
    )

    # Optionally deactivate the original sequencing group
    if deactivate_original_sg and dry_run:
        logger.info(
            f'DRY RUN :: Would deactivate original sequencing group {original_sg_id}.'
        )
    elif deactivate_original_sg:
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
        '--new_sample_external_id',
        type=str,
        help='External sample ID to associate with the new sequencing group.',
    )
    parser.add_argument(
        '--new_sample_id',
        type=str,
        default=None,
        help='Optional internal sample ID to associate with the new sequencing group.',
    )
    parser.add_argument(
        '--new_participant_id',
        type=str,
        default=None,
        help='Optional participant ID (or external participant ID) to associate with the new sequencing group. Must be consistent with the new sample if provided.',
    )
    parser.add_argument(
        '--reuse_sequencing_group_id',
        type=str,
        default=None,
        help='Optional existing sequencing group ID to reuse instead of creating a new one.',
    )
    parser.add_argument(
        '--deactivate_original_sg',
        action='store_true',
        help='Optional flag to deactivate the original sequencing group.',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Log what would be created and copied, without writing anything.',
    )
    args = parser.parse_args()
    asyncio.run(
        main(
            original_sg_id=args.original_sg_id,
            source_dataset=args.source_dataset,
            new_dataset=args.new_dataset,
            new_sample_external_id=args.new_sample_external_id,
            new_sample_id=args.new_sample_id,
            new_participant_id=args.new_participant_id,
            reuse_sequencing_group_id=args.reuse_sequencing_group_id,
            deactivate_original_sg=args.deactivate_original_sg,
            dry_run=args.dry_run,
        )
    )
