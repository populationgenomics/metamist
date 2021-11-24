# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,no-self-use,wrong-import-order
"""
Prepare populate sample metadata for NAGIM project.
"""
import dataclasses
import logging
import os
import subprocess
import sys
from os.path import join, exists
from typing import List, Dict, Union
import click
import pandas as pd

from cpg_pipes.pipeline import setup_batch
from sample_metadata.parser.generic_parser import GenericParser, GroupedRow
from sample_metadata import SampleApi

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


SRC_BUCKETS_TEST = [
    # Australian Terra workspace
    'gs://fc-7d762f69-bb45-48df-901b-b3bcec656ee0/2232b739-5183-4935-bb84-452a631c31ea',
    # The US Terra workspace
    'gs://fc-bda68b2d-bed3-495f-a63c-29477968feff/1a9237ff-2e6e-4444-b67d-bd2715b8a156',
]

SRC_BUCKETS_MAIN = []

assert all(b.startswith('gs://fc-') for b in SRC_BUCKETS_MAIN + SRC_BUCKETS_TEST)

PROJECT = 'nagim'

# Mapping the KCCG project IDs to internal CPG project IDs
PROJECT_ID_MAP = {
    '1KB': 'thousand-genomes',
    'ALS': PROJECT,  # we don't have a stack for ALS, so just using nagim
    'AMP-PD': 'amp-pd',
    'HGDP': 'hgdp',
    'MGRB': 'mgrb',
    'TOB': 'tob-wgs',
    'acute_care': 'acute-care',
}


@dataclasses.dataclass
class Sample:
    """
    Represent a parsed sample so we can populate and fix all the IDs transparently
    """

    nagim_id: str = None
    cpg_id: str = None
    ext_id: str = None
    project_id: str = None
    gvcf: str = None
    cram: str = None


@click.group()
def cli():
    """
    Click group to handle multiple CLI commands defined further
    """


@cli.command()
@click.option('--prod', 'prod', is_flag=True)
@click.option('--tmp-dir', 'tmp_dir')
@click.option(
    '--transfer-gvcfs',
    is_flag=True,
    help='Transfer GVCF and TBI files from gs://fc-... to gs://cpg-...-upload',
)
@click.option(
    '--transfer-crams',
    is_flag=True,
    help='Transfer CRAM and CRAI from gs://fc-... to gs://cpg-...-upload',
)
@click.option('--use-batch', is_flag=True, help='Use a Batch job to transfer data')
def transfer(
    prod,
    tmp_dir,
    transfer_gvcfs: bool,
    transfer_crams: bool,
    use_batch: bool,
):
    """
    Transfer data from the Terra workspaces to the GCP bucket. Must be run with
    a personal account, because the read permissions to Terra buckets match
    to the Terra user emails for whom the workspace is sharred, so Hail service
    acounts won't work here.
    """
    if not transfer_gvcfs and not transfer_crams:
        raise click.BadParameter(
            'Please, specify at leat one of --transfer-crams or --transfer-gvcfs'
        )

    namespace = 'main' if prod else 'test'

    # Putting to -upload because we need to rename them after CPG IDs, and because
    # we need to run ReblockGVCFs on GVCFs
    upload_gvcf_bucket = f'gs://cpg-{PROJECT}-{namespace}-upload/gvcf'
    upload_cram_bucket = f'gs://cpg-{PROJECT}-{namespace}-upload/cram'

    if use_batch:
        hbatch = setup_batch(
            title='Transferring NAGIM data',
            keep_scratch=False,
            tmp_bucket=f'cpg-{PROJECT}-{namespace}-tmp',
            analysis_project_name=PROJECT,
        )
    else:
        hbatch = None

    if transfer_gvcfs:
        _transfer_gvcf_from_terra(upload_gvcf_bucket, prod, hbatch)
    if transfer_crams:
        _transfer_cram_from_terra(upload_cram_bucket, prod, hbatch)

    if use_batch:
        hbatch.run(wait=True)

    # Finds GVCFs and CRAMs after transferring, and checks that all of them have
    # corresponding TBI/CRAM, and initializing the sample list.
    _find_upload_files(
        upload_gvcf_bucket,
        upload_cram_bucket,
        tmp_dir,
        overwrite=True,  # Force finding files
    )


@cli.command()
@click.argument('sample_to_project_path')
@click.option('--prod', 'prod', is_flag=True)
@click.option('--tmp-dir', 'tmp_dir')
@click.option(
    '--confirm', is_flag=True, help='Confirm with user input before updating server'
)
@click.option('--dry-run', 'dry_run', is_flag=True)
@click.option(
    '--skip-checking-objects',
    'skip_checking_objects',
    is_flag=True,
    help='Do not check objects on buckets (existence, size, md5)',
)
def parse(
    sample_to_project_path: str,
    prod,
    tmp_dir,
    confirm: bool,
    dry_run: bool,
    skip_checking_objects: bool,
):
    """
    Assuming the data is transferred to the CPG bucket, populat the SM project.
    """
    namespace = 'main' if prod else 'test'

    upload_gvcf_bucket = f'gs://cpg-{PROJECT}-{namespace}-upload/gvcf'
    upload_cram_bucket = f'gs://cpg-{PROJECT}-{namespace}-upload/cram'

    # Finds GVCFs and CRAMs after transferring, and checks that all of them have
    # corresponding TBI/CRAM, and initializing the sample list.
    samples = _find_upload_files(upload_gvcf_bucket, upload_cram_bucket, tmp_dir)

    # Checks sample and project IDs.
    # Some samples processed with Terra use CPG IDs, checking if we already
    # have them in the SMDB and fixing the external IDs.
    samples = _fix_ids(samples, sample_to_project_path, prod=prod)

    # Creating a parser for each project separately
    for proj_id in PROJECT_ID_MAP.values():
        sm_proj_id = proj_id if prod else f'{proj_id}-test'
        sample_tsv_file = join(tmp_dir, f'samples-{sm_proj_id}.csv')
        df = pd.DataFrame(
            dict(
                cpg_id=s.cpg_id,
                ext_id=s.ext_id,
                gvcf=s.gvcf,
                cram=s.cram,
            )
            for s in samples
            if s.project_id == proj_id
        )
        if len(df) == 0:
            logger.info(f'No samples for project {sm_proj_id} found, skipping')
            continue

        df.to_csv(sample_tsv_file, index=False)
        logger.info(
            f'Processing {len(df)} samples for project {sm_proj_id}, '
            f'sample manifest: {sample_tsv_file}'
        )

        parser = NagimParser(
            path_prefix=None,
            sample_metadata_project=sm_proj_id,
            skip_checking_gcs_objects=skip_checking_objects,
            confirm=confirm,
            verbose=False,
        )
        with open(sample_tsv_file) as f:
            parser.parse_manifest(f, dry_run=dry_run)


def _fix_ids(
    samples_from_found_files: List[Sample],
    sample_to_project_path: str,
    prod: bool,
) -> List[Sample]:
    sapi = SampleApi()

    proj_ids = PROJECT_ID_MAP.values()
    sm_proj_ids = [f'{proj}-test' if not prod else proj for proj in proj_ids]
    sm_sample_dicts = sapi.get_samples(
        body_get_samples_by_criteria_api_v1_sample_post={
            'project_ids': sm_proj_ids,
            'active': True,
        }
    )

    cpgid_to_extid = {s['id']: s['external_id'] for s in sm_sample_dicts}
    extid_to_cpgid = {s['external_id']: s['id'] for s in sm_sample_dicts}

    # Parse KCCG metadata (two columns: sample and project)
    proj_by_nagim_id = dict()
    with open(sample_to_project_path) as f:
        for line in f:
            if line.strip():
                nagim_id, proj = line.strip().split('\t')
                nagim_id = nagim_id.split('.')[
                    0
                ]  # AAACD__ST-E00141_HKJ2MCCXX.3 -> AAACD__ST-E00141_HKJ2MCCXX
                proj_by_nagim_id[nagim_id] = PROJECT_ID_MAP[proj]

    # Setting up project IDs
    for sample in samples_from_found_files:
        if sample.nagim_id not in proj_by_nagim_id:
            logger.critical(
                f'Sample {sample.nagim_id} not found in {sample_to_project_path}'
            )
            sys.exit(1)

        sample.project_id = proj_by_nagim_id[sample.nagim_id]

    # Fixing sample IDs. Some samples (tob-wgs and acute-care)
    # have CPG IDs as nagim ids, some don't
    for sample in samples_from_found_files:
        if sample.nagim_id in extid_to_cpgid:
            sample.ext_id = sample.nagim_id
            sample.cpg_id = extid_to_cpgid[sample.nagim_id]
        elif sample.nagim_id in cpgid_to_extid:
            sample.ext_id = cpgid_to_extid[sample.nagim_id]
            sample.cpg_id = sample.nagim_id
        else:
            sample.ext_id = sample.nagim_id

    return samples_from_found_files


def _transfer_cram_from_terra(upload_bucket: str, prod: bool, hbatch=None):
    for bucket in SRC_BUCKETS_TEST if not prod else SRC_BUCKETS_MAIN:
        # Find all crams and crais and copy them in parallel
        cmd = (
            f'gsutil ls '
            f"'{bucket}/WholeGenomeReprocessingMultiple/*/call-WholeGenomeReprocessing/shard-*/WholeGenomeReprocessing/*/call-WholeGenomeGermlineSingleSample/WholeGenomeGermlineSingleSample/*/"
            f"call-BamToCram/BamToCram/*/call-ConvertToCram/**/*.cram*'"
            f' | gsutil -m cp -I {upload_bucket}/'
        )
        if hbatch:
            _add_batch_job(hbatch, cmd, f'Transfer CRAMs from {bucket}')
        else:
            _call(cmd)


def _transfer_gvcf_from_terra(upload_bucket: str, prod: bool, hbatch=None):
    for bucket in SRC_BUCKETS_TEST if not prod else SRC_BUCKETS_MAIN:
        # Find all GVCFs and TBIs and copy them in parallel
        cmd = (
            f'gsutil ls '
            f"'{bucket}/WholeGenomeReprocessingMultiple/*/call-WholeGenomeReprocessing/shard-*/WholeGenomeReprocessing/*/call-WholeGenomeGermlineSingleSample/WholeGenomeGermlineSingleSample/*/"
            f"call-BamToGvcf/VariantCalling/*/call-MergeVCFs/**/*.hard-filtered.g.vcf.gz*'"
            f' | gsutil -m cp -I {upload_bucket}/'
        )
        if hbatch:
            _add_batch_job(hbatch, cmd, f'Transfer GVCFs from {bucket}')
        else:
            _call(cmd)


def _add_batch_job(hbatch, cmd, job_name):
    j = hbatch.new_job(job_name)
    j.cpu(32)
    j.memory('lowmem')
    j.image('australia-southeast1-docker.pkg.dev/cpg-common/images/aspera:v1')
    j.command('export GOOGLE_APPLICATION_CREDENTIALS=/gsa-key/key.json')
    j.command(
        'gcloud -q auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS'
    )
    j.command(cmd)
    return j


def _call(cmd):
    print(cmd)
    subprocess.run(cmd, shell=True, check=True)


class NagimParser(GenericParser):
    """
    Inherits from sample_metadata's GenericParser class and implements parsing
    logic specific to the NAGIM project
    """

    def get_sample_meta(self, sample_id: str, row: GroupedRow) -> Dict[str, any]:
        return dict()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_sample_id(self, row: Dict[str, any]) -> str:
        return row['ext_id']

    def get_sequence_meta(self, sample_id: str, row: GroupedRow) -> Dict[str, any]:
        sequence_meta = dict()
        if 'gvcf' in row and row['gvcf'] and row['gvcf'] != '-':
            gvcf, variants_type = self.parse_file([row['gvcf']])
            sequence_meta['gvcf'] = gvcf
            sequence_meta['gvcf_type'] = variants_type
        if 'cram' in row and row['cram'] and row['cram'] != '-':
            reads, reads_type = self.parse_file([row['cram']])
            sequence_meta['reads'] = reads
            sequence_meta['reads_type'] = reads_type
        return sequence_meta

    def get_sequence_status(self, sample_id: str, row: GroupedRow) -> str:
        return 'uploaded'


def _get_bucket_ls(
    ext: Union[str, List[str]],
    source_bucket,
    tmp_dir,
    overwrite,
) -> List[str]:
    label = ext.replace('.', '-')
    output_path = join(tmp_dir, f'gs-ls{label}.txt')
    if overwrite or not exists(output_path):
        _call(f'test -e {output_path} && rm {output_path}')
        _call(f'touch {output_path}')
        if isinstance(ext, str):
            ext = [ext]
        for e in ext:
            _call(f'gsutil ls "{source_bucket}/*{e}" >> {output_path}')
    with open(output_path) as f:
        return [line.strip() for line in f.readlines() if line.strip()]


def _find_upload_files(
    upload_gvcf_bucket,
    upload_cram_bucket,
    tmp_dir,
    overwrite=False,
) -> List[Sample]:
    gvcf_paths = _get_bucket_ls(
        ext='.vcf.gz',
        source_bucket=upload_gvcf_bucket,
        tmp_dir=tmp_dir,
        overwrite=overwrite,
    )
    tbi_paths = _get_bucket_ls(
        ext='.vcf.gz.tbi',
        source_bucket=upload_gvcf_bucket,
        tmp_dir=tmp_dir,
        overwrite=overwrite,
    )
    cram_paths = _get_bucket_ls(
        ext='.cram',
        source_bucket=upload_cram_bucket,
        tmp_dir=tmp_dir,
        overwrite=overwrite,
    )
    crai_paths = _get_bucket_ls(
        ext='.cram.crai',
        source_bucket=upload_cram_bucket,
        tmp_dir=tmp_dir,
        overwrite=overwrite,
    )

    file_by_type_by_sid = {
        'gvcf': dict(),
        'tbi': dict(),
        'cram': dict(),
        'crai': dict(),
    }

    for fname in gvcf_paths:
        sid = os.path.basename(fname).replace('.hard-filtered.g.vcf.gz', '')
        file_by_type_by_sid['gvcf'][sid] = fname

    for fname in tbi_paths:
        sid = os.path.basename(fname).replace('.hard-filtered.g.vcf.gz.tbi', '')
        if sid not in file_by_type_by_sid['gvcf']:
            logger.info(f'Found TBI without GVCF: {fname}')
        else:
            file_by_type_by_sid['tbi'][sid] = fname

    for fname in cram_paths:
        sid = os.path.basename(fname).replace('.cram', '')
        file_by_type_by_sid['cram'][sid] = fname

    for fname in crai_paths:
        sid = os.path.basename(fname).replace('.cram.crai', '')
        if sid not in file_by_type_by_sid['cram']:
            logger.info(f'Found CRAI without CRAM: {fname}')
        else:
            file_by_type_by_sid['crai'][sid] = fname

    samples = []
    for sid in file_by_type_by_sid['gvcf']:
        samples.append(
            Sample(
                nagim_id=sid,
                gvcf=file_by_type_by_sid['gvcf'][sid],
                cram=file_by_type_by_sid.get('cram', {}).get(sid),
            )
        )
    return samples


cli.add_command(transfer)
cli.add_command(parse)

if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    cli(obj={})  # pylint: disable=unexpected-keyword-arg
