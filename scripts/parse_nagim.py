# pylint: disable=too-many-lines
"""
Taking Terra results, populate sample-metadata for NAGIM project.

Has 3 commands: transfer, parse, and move_uploads.

The following command transfers CRAM, GVCF files along with corresponding indices,
and QC files from a Terra workspace to the GCP upload bucket.

```
python scripts/parse_nagim.py \
    transfer \
    --tmp-dir nagim-parse-tmp \
    --use-batch
```

This can be run through the analysis runner with the `nagim` dataset, because
the hail nagim service accounts were added as readers to the Terra workspace using
[Terra tools](https://github.com/broadinstitute/terra-tools/tree/master/scripts/register_service_account)
as follows:

```
git clone https://github.com/broadinstitute/terra-tools
python terra-tools/scripts/register_service_account/register_service_account.py \
    --json_credentials nagim-test-133-hail.json \
    --owner_email vladislav.savelyev@populationgenomics.org.au
```

(Assuming the `-j` value is the JSON key for the hail "test" service account - so
should repeat the same command for the "standard" one - and the `-e` value is
the email where notifications will be sent to.)

Now, assuming CRAMs and GVCFs are transferred, the following command populates the
sample metadata DB objects:

```
python scripts/parse_nagim.py \
    parse \
    --tmp-dir nagim-parse-tmp \
    --skip-checking-objects \
    --confirm
```

It would create a sample entry at each corresponding project (e.g. mrgb, tob-wgs, etc).

To populate test projects, you can add `--test` flag (and optionally
`--clean-up-test` to remove all existing "test" samples):

```
python scripts/parse_nagim.py \
    parse \
    --tmp-dir nagim-parse-tmp \
    --test \
    --clean-up-test \
    --skip-checking-objects \
```

The `--skip-checking-objects` tells the parser to skip checking the existence of
objects on buckets, which is useful to speed up the execution as long as we trust
the transfer hat happened in the previous `transfer` command. It would also
disable md5 and file size checks.

`parse` would find files in any possible standard locations
(-upload, -main, -archive buckets), prioritizing the most final versions. It will
add flags into Analysis.meta dictionary indicating the status of the file:
"is_upload", "is_staging", "is_archive".

Finally, files from upload can be moved to -main and -archive buckets, with file
renamed to use newly created CPG IDs:

```
python scripts/parse_nagim.py \
    move_uploads \
    --tmp-dir nagim-parse-tmp
```

After completing `move_uploads, run `parse` again to populate new analysis entries.

"transfer", "parse", "move_uploads" scripts have to be run under "nagim-standard" access
levels, "parse" with the --test flag should be used with "nagim-test".
"""

import logging
import os.path
import subprocess
import tempfile
from dataclasses import dataclass, field
from os.path import join, exists, basename
from typing import List, Dict, Any, Optional, Tuple, Callable, Union
import json
import gcsfs

import click
import pandas as pd
from cpg_pipes.pipeline import setup_batch
from cpg_pipes.resources import DRIVER_IMAGE
from cpg_pipes.utils import can_reuse
import hail as hl
from hailtop.batch.job import Job

from sample_metadata.models import (
    AnalysisStatus,
    AnalysisType,
    AnalysisModel,
)
from sample_metadata.apis import SampleApi
from sample_metadata.models import SampleUpdateModel
from sample_metadata.parser.generic_parser import GenericParser, GroupedRow, run_as_sync


logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


NAGIM_PROJ_ID = 'nagim'

# Mapping the KCCG project IDs to internal CPG project IDs
PROJECT_ID_MAP = {
    '1KB': 'thousand-genomes',
    'HGDP': 'hgdp',
    'ALS': 'csiro-als',
    'AMP-PD': 'amp-pd',
    'MGRB': 'mgrb',
    'TOB': 'tob-wgs',
    'acute_care': 'acute-care',
}

# For debugging purposes: when we only want to process following projects.
# If empty, everything will be processed.
PROJECTS_TO_PROCESS: List[str] = []

# For debugging purposes: when we only want to process following sources.
SOURCES_TO_PROCESS = [
    'QC',
    'GVCF',
    'CRAM',
]

# Metrics we extract from MultiQC and put into Sequence.meta and Analysis
QC_METRICS = [
    # id, multiqc id
    ('freemix', 'FREEMIX'),
    ('median_coverage', 'MEDIAN_COVERAGE'),
    ('pct_chimeras', 'PCT_CHIMERAS'),
    ('pct_30x', 'PCT_30X'),
    ('pct_reads_aligned_in_pairs', 'PCT_READS_ALIGNED_IN_PAIRS'),
    ('percent_duplication', 'PERCENT_DUPLICATION'),
    ('median_insert_size', 'summed_median'),
]


# 2 columns: sample IDs used in the NAGIM run, and a project ID.
SAMPLE_TO_PROJECT_TSV_PATH = 'gs://cpg-nagim-main/metadata/nagim-terra-samples.tsv'

SRC_BUCKETS = {
    'test': {
        'Australia': [  # Australian Terra workspace
            'gs://fc-7d762f69-bb45-48df-901b-b3bcec656ee0/2232b739-5183-4935-bb84-452a631c31ea',
        ],
        'US': [  # The US Terra workspace
            'gs://fc-bda68b2d-bed3-495f-a63c-29477968feff/1a9237ff-2e6e-4444-b67d-bd2715b8a156',
        ],
    },
    'main': {
        'Australia': [
            'gs://fc-7d762f69-bb45-48df-901b-b3bcec656ee0/8b5c4805-a08c-4b22-9521-f003e1e02153',
            'gs://fc-975676a8-4e21-46af-bc02-816044ad7448/1e968324-0d1d-4061-86d5-2f2678363e5a',
            'gs://fc-7d762f69-bb45-48df-901b-b3bcec656ee0/376b7e6e-3e9a-4608-899b-3ae56f42b8ae',
            'gs://fc-fa51701d-03df-4ca7-8408-5c859458759d/1c6b5f64-1b83-4f98-9ba8-0cc7918677a9',
            'gs://fc-10674f84-3eed-440a-b6fd-f6b0a7a3f3d0/a521fa83-0974-4b0b-8ffd-de8bb7363adc',
            'gs://fc-7d762f69-bb45-48df-901b-b3bcec656ee0/95b12dea-5d83-4e19-9a9d-4616d69ec9a3',
            'gs://fc-7d762f69-bb45-48df-901b-b3bcec656ee0/4ee1f6ce-8045-49c5-8fd0-6409b3bd063f',
            'gs://fc-7d762f69-bb45-48df-901b-b3bcec656ee0/bc178a03-ad33-4eba-8581-a5ee441d1370',
            'gs://fc-f42ce9c2-17c2-4ae9-ac49-657ad9783280/2a991598-d7bc-4aea-af81-ff376d131c3b',
            'gs://fc-30c132a7-2e19-4b73-9d70-e23c405740a2/9585ddb4-fa1c-499a-b424-32cf9def33a5',
            'gs://fc-79767284-d7a5-4565-9816-61c6e28e9f7f/37959029-3ed9-4415-aa0a-f4c2337b9c14',
            'gs://fc-7d762f69-bb45-48df-901b-b3bcec656ee0/ceaed9aa-9e17-4b19-9926-a320ee614d6e',
            'gs://fc-7312af9d-7217-4eef-a6c0-c3637ade1662/d0bbd0be-3f66-4308-9376-34844d520073',
            'gs://fc-79767284-d7a5-4565-9816-61c6e28e9f7f/65bca9dc-99b5-4eac-9e29-a82ef94c542c',
            'gs://fc-fa51701d-03df-4ca7-8408-5c859458759d/fe652736-53aa-4fab-bc24-8fec9f7cea8e',
            'gs://fc-ddb2e6d7-319a-4dc2-aa79-f640c2f889d3/defa7f3c-b04d-4a2d-ae80-16379be145e8',
            'gs://fc-79cf62c1-c8c6-4934-93cd-dcd792d905d8/e47071c6-cc81-4c77-a860-56bd5fb75fff',
            'gs://fc-3a36f1b1-761b-4d24-ba78-f8f72a55daab/d57f15fb-c7ae-45e2-bf17-f305493efa4a',
        ],
        'US': [
            'gs://fc-bda68b2d-bed3-495f-a63c-29477968feff/153e4788-1c48-4a51-864e-9707dbae5c59',
            'gs://fc-bda68b2d-bed3-495f-a63c-29477968feff/b4a00407-f6c6-4fd0-b71f-820e047f792c',
            'gs://fc-bda68b2d-bed3-495f-a63c-29477968feff/914b7deb-9156-4cc8-8eb0-b13a6d008e2b',
            'gs://fc-bda68b2d-bed3-495f-a63c-29477968feff/bfa7f93d-06c8-40d5-b1da-de68b390d8cf',
            'gs://fc-bda68b2d-bed3-495f-a63c-29477968feff/b9fab668-3b28-4e58-8af2-5d443d7aae2f',
            'gs://fc-bda68b2d-bed3-495f-a63c-29477968feff/884b65af-adba-4cbf-a068-48ea9e948524',
        ],
    },
}


class FileType:
    """
    Represents file type/extension.
    """

    def __init__(
        self,
        # File extension/ending that would be added to the CPG ID
        ending: str,
        # Search pattern to find files of this type on Terra workspace bucket
        terra_search_pattern: str,
        # Ending to strip from file name to get sample ID used within NAGIM (s.nagim_id),
        # in case if it's different from `ending`
        terra_file_ending: Optional[str] = None,
    ):
        self.ending = ending
        self.terra_search_pattern = terra_search_pattern
        self.terra_file_ending = terra_file_ending or ending


class Source:
    """
    Type of files we pull (e.g. CRAM, GVCF, QC)
    """

    def __init__(
        self,
        name: str,
        file_types: List[FileType],
        upload_bucket: Union[str, Callable],
    ):
        self.name = name
        self.id = name.lower()
        self.file_types = file_types
        self._upload_bucket = upload_bucket

    def get_upload_bucket(self, ending=None):
        """
        Returns the full path in the "upload" bucket ("gs://cpg-*-upload/")
        """
        return self._upload_bucket.format(ending=ending)

    def __repr__(self):
        return self.name

    def transfer_to_upload(self, hbatch, namespace: str):
        """
        Search files in buckets using search patterns and copy to CPG upload buckets
        """
        for region, buckets in SRC_BUCKETS[namespace].items():
            for bucket in buckets:
                for file_type in self.file_types:
                    j = _add_batch_job(
                        hbatch=hbatch,
                        job_name=(
                            f'{region}: transfer {self.name} {file_type.ending} files '
                            f'from {bucket}'
                        ),
                        ncpu=32,
                    )
                    j.command(
                        f"gsutil ls '{bucket}/{file_type.terra_search_pattern}'"
                        f' | gsutil -m cp -I '
                        f'{self.get_upload_bucket(file_type.ending)}/'
                    )

    def get_archive_bucket(self, stack: str, namespace: str, ending=None) -> str:
        """
        Path to the archive bucket for this file source
        """
        res = f'gs://cpg-{stack}-archive/{self.id}/{NAGIM_PROJ_ID}'
        if self.id == 'qc':
            assert ending is not None
            return join(res, ending)
        if namespace == 'test':  # we don't have a separate archive for test
            res = res.replace('-archive/', '-archive/test/')
        return res

    def get_staging_bucket(self, stack: str, namespace: str) -> Optional[str]:
        """
        Path to the bucket with staging files (defined for gvcf)
        """
        if self.id == 'gvcf':
            return f'gs://cpg-{stack}-{namespace}/{self.id}/staging/{NAGIM_PROJ_ID}'
        return None

    def get_staging_archive_bucket(self, stack: str, namespace: str) -> Optional[str]:
        """
        Path to the archive bucket with staging files (defined for gvcf)
        """
        if self.id == 'gvcf':
            res = f'gs://cpg-{stack}-archive/{self.id}/staging/{NAGIM_PROJ_ID}'
            if namespace == 'test':  # we don't have a separate archive for test
                res = res.replace('-archive/', '-archive/test/')
            return res
        return None

    def get_final_bucket(self, stack: str, namespace: str, ending=None) -> str:
        """
        Final location of files

        Note that keep all qc files in the "main" namespace: we are not interested
        in qc files after this script (relevant metrics are populated into type=qc
        analyses meta field), so no need to move them for a test subset.
        """
        if self.id == 'qc':
            assert ending is not None
            return f'gs://cpg-{NAGIM_PROJ_ID}-main/qc/{ending}'
        return f'gs://cpg-{stack}-{namespace}/{self.id}/{NAGIM_PROJ_ID}'


# Instantiating file sources
SOURCES = {
    s.name: s
    for s in [
        Source(
            name='CRAM',
            file_types=[
                FileType('cram', '**/call-ConvertToCram/**/*.cram'),
                FileType('cram.crai', '**/call-ConvertToCram/**/*.cram.crai'),
                FileType('cram.md5', '**/call-ConvertToCram/**/*.cram.md5'),
            ],
            upload_bucket=f'gs://cpg-{NAGIM_PROJ_ID}-main-upload/cram',
        ),
        Source(
            name='GVCF',
            file_types=[
                FileType(
                    'g.vcf.gz',
                    '**/call-MergeVCFs/**/*.hard-filtered.g.vcf.gz',
                    terra_file_ending='hard-filtered.g.vcf.gz',
                ),
                FileType(
                    'g.vcf.gz.tbi',
                    '**/call-MergeVCFs/**/*.hard-filtered.g.vcf.gz.tbi',
                    terra_file_ending='hard-filtered.g.vcf.gz.tbi',
                ),
            ],
            upload_bucket=f'gs://cpg-{NAGIM_PROJ_ID}-main-upload/gvcf',
        ),
        Source(
            name='QC',
            upload_bucket=f'gs://cpg-{NAGIM_PROJ_ID}-main-upload/qc/{{ending}}',
            file_types=[
                FileType(e, f'**/*.{e}')
                for e in [
                    'alignment_summary_metrics',
                    'duplicate_metrics',
                    'insert_size_metrics',
                    'wgs_metrics',
                    'preBqsr.selfSM',
                    # #Uncomment if you want other types of QC:
                    # 'bait_bias_detail_metrics',
                    # 'bait_bias_summary_metrics',
                    # 'detail_metrics',
                    # 'pre_adapter_detail_metrics',
                    # 'pre_adapter_summary_metrics',
                    # 'quality_distribution_metrics',
                    # 'raw_wgs_metrics',
                    # 'summary_metrics',
                    # 'variant_calling_detail_metrics',
                    # 'variant_calling_summary_metrics',
                ]
            ],
        ),
    ]
}


class File:
    """
    Represents a file that can be located in different buckets
    """

    def __init__(
        self,
        # Path to this file in -upload bucket
        upload_path: Optional[str] = None,
        # Path to this file in a final location (e.g. -archive or -main)
        moved_path: Optional[str] = None,
    ):
        self.upload_path = upload_path
        self.moved_path = moved_path
        self.is_staging = False
        self.is_archive = False

    def is_moved(self) -> bool:
        """
        File was found in a final location, no need to check -upload bucket
        """
        return self.moved_path is not None

    def is_upload(self) -> bool:
        """
        File was not found in a final location, bit found in an -upload bucket
        """
        return not self.is_moved()

    def get_path(self) -> str:
        """
        Get path to the file, prioritizing the final location
        """
        path = self.moved_path or self.upload_path
        assert path is not None
        return str(path)


@dataclass
class Sample:  # pylint: disable=too-many-instance-attributes
    """
    Represent a parsed sample, so we can check that all required files for
    a sample exist, and also populate and fix sample IDs.
    """

    # as used in the Terra run; can be external ID, or match the internal ID
    # as for TOB-WGS and Acute-care projects.
    nagim_id: str

    # "test" CPG ID is different from the "main" CPG ID for the same sample,
    # so we want to track both, so we know what files to move from "main"
    # into "test" when creating a test subset.
    test_cpg_id: Optional[str] = None
    main_cpg_id: Optional[str] = None
    # For "main", cpg_id==main_cpg_id; for "test", cpg_id==test_cpg_id
    cpg_id: Optional[str] = None

    ext_id: Optional[str] = None
    project_id: Optional[str] = None

    # File paths indexed by (Source.id, file ending)
    files: Dict[Tuple[str, str], File] = field(default_factory=dict)

    gvcf: Optional[File] = None
    tbi: Optional[File] = None

    cram: Optional[File] = None
    crai: Optional[File] = None
    cram_md5: Optional[File] = None

    # File paths indexed by ending
    qc_files: Dict[str, File] = field(default_factory=dict)

    # QC stats indexed by ending
    qc_values: Dict[str, str] = field(default_factory=dict)

    meta: Dict[str, Any] = field(default_factory=dict)


def _find_moved_files(  # pylint: disable=too-many-nested-blocks
    samples: List[Sample],
    tmp_dir: str,
    namespace: str,
    overwrite: bool = False,
):
    """
    Find files in final locations, populate file fields for each sample,
    and verify that every sample has an expected set of files.

    Assumning s.cpg_id is populated.
    """
    sample_by_cpgid = {s.cpg_id: s for s in samples}
    sample_by_nagim_id = {s.nagim_id: s for s in samples}

    for source_name in SOURCES_TO_PROCESS:
        source = SOURCES[source_name]
        for file_type in source.file_types:
            # Finding files moved to final locations
            for proj in PROJECT_ID_MAP.values():
                stack = _get_stack_id(proj)

                # Checking different locations in an order so that the most final
                # locations go last and override previous locations.
                staging_archive_bucket = source.get_staging_archive_bucket(
                    stack, namespace
                )
                archive_bucket = source.get_archive_bucket(
                    stack, namespace, ending=file_type.ending
                )
                staging_bucket = source.get_staging_bucket(stack, namespace)
                final_bucket = source.get_final_bucket(
                    stack, namespace, ending=file_type.ending
                )
                for location_type, bucket in [
                    ('staging_archive', staging_archive_bucket),
                    ('archive', archive_bucket),
                    ('staging', staging_bucket),
                    ('final', final_bucket),
                ]:
                    if not bucket:
                        continue
                    paths = _cache_bucket_ls(
                        ending_to_search=file_type.ending,
                        source_bucket=bucket,
                        tmp_dir=join(tmp_dir, location_type + '-' + stack),
                        overwrite=overwrite,
                    )
                    for path in paths:
                        assert path.endswith(f'.{file_type.ending}')
                        sample_id = basename(path)[: -len(f'.{file_type.ending}')]
                        if sample_id in sample_by_cpgid:
                            s = sample_by_cpgid[sample_id]
                        elif sample_id in sample_by_nagim_id:
                            s = sample_by_nagim_id[sample_id]
                        else:
                            continue

                        key = source.name, file_type.ending
                        file = s.files.get(key)
                        if not file:
                            file = File(moved_path=path)
                        else:
                            file.moved_path = path
                        if location_type in ['archive', 'staging_archive']:
                            file.is_archive = True
                        if location_type in ['staging', 'staging_archive']:
                            file.is_staging = True
                        s.files[key] = file


def _find_upload_files(
    samples: List[Sample],
    tmp_dir: str,
    overwrite: bool = False,
):
    """
    Find files in upload locations, populate file fields for each sample,
    and verify that every sample has an expected set of files.

    Assumning s.nagim_id is populated.
    """
    sample_by_nagimid = {s.nagim_id: s for s in samples}

    for source_name in SOURCES_TO_PROCESS:
        source = SOURCES[source_name]
        for file_type in source.file_types:
            paths = _cache_bucket_ls(
                ending_to_search=file_type.terra_file_ending,
                source_bucket=source.get_upload_bucket(file_type.terra_file_ending),
                tmp_dir=join(tmp_dir, 'uploads'),
                overwrite=overwrite,
            )
            for path in paths:
                assert path.endswith(f'.{file_type.terra_file_ending}')
                nagim_id = basename(path)[: -len(f'.{file_type.terra_file_ending}')]
                if nagim_id not in sample_by_nagimid:
                    continue

                key = (source.name, file_type.ending)
                file = sample_by_nagimid[nagim_id].files.get(key)
                if not file:
                    file = File(upload_path=path)
                else:
                    file.upload_path = path
                sample_by_nagimid[nagim_id].files[key] = file


def _check_found_files(samples):
    """
    Tally found files and populate s.gvcf, s.cram, s.qc_files
    """
    for source_name in SOURCES_TO_PROCESS:
        source = SOURCES[source_name]
        for file_type in source.file_types:
            found_samples = len(
                [s for s in samples if (source.name, file_type.ending) in s.files]
            )
            logger.info(
                f'Found {found_samples}/{len(samples)} '
                f'{source.name}/{file_type.ending} files'
            )

    # For each sample, verify that the set of found files is consistent
    for s in samples:
        if 'GVCF' in SOURCES_TO_PROCESS:
            s.gvcf = s.files.get(('GVCF', 'g.vcf.gz'))
            s.tbi = s.files.get(('GVCF', 'g.vcf.gz.tbi'))

            if s.gvcf and not s.tbi:
                logger.warning(
                    f'{s.project_id} Found GVCF without TBI for {s.nagim_id}'
                )
            elif s.tbi and not s.gvcf:
                logger.warning(
                    f'{s.project_id}: Found TBI without GVCF for {s.nagim_id}'
                )
            elif not s.gvcf:
                logger.warning(f'{s.project_id}: Not found GVCF for {s.nagim_id}')

        if 'CRAM' in SOURCES_TO_PROCESS:
            s.cram = s.files.get((SOURCES['CRAM'].name, 'cram'))
            s.crai = s.files.get((SOURCES['CRAM'].name, 'cram.crai'))
            s.cram_md5 = s.files.get((SOURCES['CRAM'].name, 'cram.md5'))

            if s.cram and not s.crai:
                logger.warning(
                    f'{s.project_id}: Found CRAM without CRAI for {s.nagim_id}'
                )
            if s.cram and not s.cram_md5:
                logger.warning(
                    f'{s.project_id}: Found CRAM without md5 for {s.nagim_id}'
                )
            if s.crai and not s.cram:
                logger.warning(
                    f'{s.project_id}: Found CRAI without CRAM for {s.nagim_id}'
                )

        if 'QC' in SOURCES_TO_PROCESS:
            for qc_ending in [
                'alignment_summary_metrics',
                'duplicate_metrics',
                'insert_size_metrics',
                'wgs_metrics',
                'preBqsr.selfSM',
            ]:
                no_qc = 0
                key = (SOURCES['QC'].name, qc_ending)
                if not s.files.get(key):
                    if s.gvcf:
                        logger.warning(
                            f'{s.project_id}: Found GVCF without QC {qc_ending}: {s.nagim_id}'
                        )
                    no_qc += 1
                    continue
                if no_qc:
                    logger.warning(f'Not found QC {qc_ending} for {no_qc} samples')
                s.qc_files[qc_ending] = s.files[key]


@click.group()
def cli():
    """
    Click group to handle multiple CLI commands defined further
    """


@cli.command()
@click.option('--tmp-dir', 'tmp_dir')
@click.option('--use-batch', is_flag=True, help='Use a Batch job to transfer data')
@click.option('--dry-run', 'dry_run', is_flag=True)
def transfer(
    tmp_dir,
    use_batch: bool,
    dry_run: bool,
):
    """
    Transfer data from the Terra workspaces to the GCP bucket. Must be run with
    a personal account, because the read permissions to Terra buckets match
    to the Terra user emails for whom the workspace is sharred, so Hail service
    acounts won't work here.
    """
    if not tmp_dir:
        tmp_dir = tempfile.gettempdir()

    namespace = 'main'

    if use_batch:
        hbatch = setup_batch(
            title='Transferring NAGIM data',
            keep_scratch=False,
            tmp_bucket=f'cpg-{NAGIM_PROJ_ID}-{namespace}-tmp',
            analysis_project_name=NAGIM_PROJ_ID,
        )
    else:
        hbatch = None

    for source in SOURCES_TO_PROCESS:
        SOURCES[source].transfer_to_upload(hbatch, namespace)

    if use_batch:
        hbatch.run(wait=True, dry_run=dry_run)
        if dry_run:
            return

    samples = _parse_sample_project_map(SAMPLE_TO_PROJECT_TSV_PATH)

    # Find GVCFs, CRAMs and other files after transferring, and checks that all
    # of them have corresponding tbi/crai/md5.
    _find_upload_files(
        samples,
        tmp_dir,
        overwrite=True,  # Force finding files
    )
    _check_found_files(samples)


@cli.command()
@click.option('--tmp-dir', 'tmp_dir')
@click.option(
    '--confirm', is_flag=True, help='Confirm with user input before updating server'
)
@click.option('--dry-run', 'dry_run', is_flag=True)
@click.option(
    '--overwrite-multiqc',
    'overwrite_multiqc',
    is_flag=True,
    help='Redo MultiQC even if report/json exist',
)
@click.option(
    '--skip-checking-objects',
    'skip_checking_objects',
    is_flag=True,
    help='Do not check objects on buckets (existence, size, md5)',
)
@click.option(
    '--clean-up-test',
    'clean_up_test',
    is_flag=True,
    help='Remove all existing samples in "test" before prior to parsing',
)
@click.option('--test', 'test', is_flag=True)
@run_as_sync
async def parse(  # pylint: disable=too-many-locals
    tmp_dir,
    confirm: bool,
    dry_run: bool,
    overwrite_multiqc: bool,
    skip_checking_objects: bool,
    clean_up_test: bool,
    test: bool,
):
    """
    Assuming the data is transferred to the CPG bucket, populate the SM projects.
    """
    if not tmp_dir:
        tmp_dir = tempfile.gettempdir()

    namespace = 'test' if test else 'main'

    sapi = SampleApi()

    if clean_up_test and test:
        for proj in PROJECT_ID_MAP.values():
            smdb_proj = _get_sm_proj_id(proj, 'test')
            existing_samples = sapi.get_samples(
                body_get_samples_by_criteria_api_v1_sample_post={
                    'project_ids': [smdb_proj],
                    'active': True,
                }
            )
            logger.info(
                f'{smdb_proj}: Removing {len(existing_samples)} existing test samples'
            )
            for i, s in enumerate(existing_samples):
                logger.info(
                    f'{smdb_proj}: #{i+1}: removing {s["id"]}/{s["external_id"]}'
                )
                sapi.update_sample(s['id'], SampleUpdateModel(active=False))

    samples = _build_sample_list(tmp_dir, namespace, test=test)

    _add_hgdp_1kg_metadata(samples, tmp_dir)

    multiqc_html_path, multiqc_json_path = _add_qc(
        samples, namespace, overwrite_multiqc
    )

    # Creating a parser for each project separately, because `sample_metadata_project`
    # is an initialization parameter, and we want to write to multiple projects.
    for proj in PROJECT_ID_MAP.values():
        sm_proj = _get_sm_proj_id(proj, namespace=namespace)
        sample_tsv_file = join(tmp_dir, f'sm-nagim-parser-samples-{sm_proj}.csv')

        rows = []
        for s in samples:
            if s.project_id != proj:
                continue

            row = dict(
                cpg_id=s.cpg_id,
                ext_id=s.ext_id,
                project=s.project_id,
            )
            if 'GVCF' in SOURCES_TO_PROCESS:
                if not s.gvcf:
                    logger.error(
                        f'Not found GVCF for sample '
                        f'{s.project_id}/{s.cpg_id}/{s.ext_id}'
                    )
                else:
                    row['gvcf'] = s.gvcf.get_path()
                    row['gvcf_upload'] = s.gvcf.is_upload()
                    row['gvcf_staging'] = s.gvcf.is_staging
                    row['gvcf_archive'] = s.gvcf.is_archive
            if 'CRAM' in SOURCES_TO_PROCESS:
                if not s.cram:
                    logger.error(
                        f'Not found CRAM for sample '
                        f'{s.project_id}/{s.cpg_id}/{s.ext_id}'
                    )
                else:
                    row['cram'] = s.cram.get_path()
                    row['cram_upload'] = s.cram.is_upload()
                    row['cram_staging'] = s.cram.is_staging
                    row['cram_archive'] = s.cram.is_archive
            for metric, val in s.qc_values.items():
                row[f'qc_value_{metric}'] = val
            for k, v in s.meta.items():
                row[f'meta_{k}'] = v
            rows.append(row)

        if len(rows) == 0:
            logger.info(f'No samples for project {sm_proj} found, skipping')
            continue

        df = pd.DataFrame(rows)
        df.to_csv(sample_tsv_file, index=False)
        logger.info(
            f'Processing {len(df)} samples for project {sm_proj}, '
            f'sample manifest: {sample_tsv_file}'
        )

        parser = NagimParser(
            path_prefix=None,
            sample_metadata_project=sm_proj,
            skip_checking_gcs_objects=skip_checking_objects,
            verbose=test,
            multiqc_html_path=multiqc_html_path,
            multiqc_json_path=multiqc_json_path,
        )
        with open(sample_tsv_file) as f:
            await parser.parse_manifest(f, dry_run=dry_run, confirm=confirm)


def _add_qc(
    samples: List[Sample], namespace: str, overwrite_multiqc: bool
) -> Tuple[str, str]:
    """
    Populates s.qc_values for each Sample object. Returns paths to MultiQC
    html and json files.
    """
    multiqc_html_path = join(
        f'gs://cpg-{NAGIM_PROJ_ID}-{namespace}-web/qc/multiqc.html'
    )
    multiqc_json_path = join(
        f'gs://cpg-{NAGIM_PROJ_ID}-{namespace}-analysis/qc/multiqc_data.json'
    )
    if 'QC' in SOURCES_TO_PROCESS:
        logger.info('Running MultiQC on QC files')
        parsed_json_fpath = _run_multiqc(
            samples,
            multiqc_html_path,
            multiqc_json_path,
            tmp_bucket=f'gs://cpg-{NAGIM_PROJ_ID}-{namespace}-tmp/qc',
            namespace=namespace,
            overwrite=overwrite_multiqc,
        )
        gfs = gcsfs.GCSFileSystem()
        with gfs.open(parsed_json_fpath) as f:
            row_by_sample = json.load(f)
        for s in samples:
            if s.nagim_id in row_by_sample:
                s.qc_values = row_by_sample[s.nagim_id]

    return multiqc_html_path, multiqc_json_path


def _build_sample_list(
    tmp_dir: str, namespace: str, overwrite: bool = False, test: bool = False
) -> List[Sample]:
    """
    Build list of Sample objects, given populated upload buckets
    """
    samples = _parse_sample_project_map(SAMPLE_TO_PROJECT_TSV_PATH)
    if test:
        samples = _subset_to_test(samples)

    # Some samples processed with Terra use CPG IDs, checking if we already
    # have them in the SMDB and fixing the external IDs.
    _fix_sample_ids(samples, namespace=namespace)

    # Find GVCFs, CRAMs and other files after transferring, and checks that all
    # of them have corresponding tbi/crai/md5. First checking -upload buckets,
    # then final buckets with moved files.
    _find_upload_files(samples, tmp_dir, overwrite=overwrite)
    _find_moved_files(samples, tmp_dir, overwrite=overwrite, namespace=namespace)
    _check_found_files(samples)

    return samples


def _run_multiqc(
    samples: List[Sample],
    html_fpath: str,
    json_fpath: str,
    tmp_bucket: str,
    namespace: str,
    overwrite: bool = False,
) -> str:
    """
    Runs MultiQC on QC files from Picard and VerifyBAMID.

    Generates an HTML report and puts in into nagim web bucket.

    Generates a JSON with metrics, extracts useful metrics into another JSON
    indexed by sample, and returns path to this JSON.
    """
    row_by_sample_json_path = f'{tmp_bucket}/parsed-qc.json'
    if can_reuse(row_by_sample_json_path, overwrite):
        return row_by_sample_json_path

    b = setup_batch(
        title='Run MultiQC on NAGIM',
        keep_scratch=False,
        tmp_bucket=join(tmp_bucket, 'hail'),
        analysis_project_name=NAGIM_PROJ_ID + ('-test' if namespace == 'test' else ''),
    )

    if not can_reuse([json_fpath, html_fpath], overwrite):
        j = b.new_job('Run MultiQC')
        j.image(DRIVER_IMAGE)

        qc_endings = set()
        qc_paths = []
        for s in samples:
            for qc_ending, qc_file in s.qc_files.items():
                qc_paths.append(qc_file.get_path())
                qc_endings.add(qc_ending)

        file_list_path = f'{tmp_bucket}/multiqc-file-list.txt'
        df = pd.DataFrame({'_': path} for path in qc_paths)
        df.to_csv(file_list_path, header=None, index=None)
        file_list = b.read_input(file_list_path)

        j.env('GOOGLE_APPLICATION_CREDENTIALS', '/gsa-key/key.json')
        j.command(f'pip install multiqc')
        j.cpu(16)
        j.storage('100G')
        j.command(
            f'gcloud -q auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS'
        )
        j.command(f'mkdir inputs')
        j.command(f'cat {file_list} | gsutil -m cp -I inputs/')

        ending_list = ', '.join(f'.{ending}' for ending in qc_endings)
        mqc_conf = f'extra_fn_clean_exts: [{ending_list}]'
        j.command(
            f'multiqc inputs -o output -f --fn_as_s_name --cl_config "{mqc_conf}"'
        )
        j.command(f'cp output/multiqc_report.html {j.report_html}')
        j.command(f'cp output/multiqc_data/multiqc_data.json {j.json}')

        b.write_output(j.report_html, html_fpath)
        b.write_output(j.json, json_fpath)
        logger.info(f'Written MultiQC reports to {html_fpath}')
        multiqc_json = j.json
    else:
        multiqc_json = b.read_input(json_fpath)

    def _parse_multiqc_json(json_fpath) -> Dict:
        with open(json_fpath) as f:
            d = json.load(f)
        row_by_sample = {}
        for tool_d in d['report_general_stats_data']:
            for sample, val_by_metric in tool_d.items():
                if sample not in row_by_sample:
                    row_by_sample[sample] = dict(s=sample)
                row = row_by_sample[sample]
                for metric, multiqc_metric in QC_METRICS:
                    if multiqc_metric in val_by_metric:
                        row[metric] = val_by_metric[multiqc_metric]
        return row_by_sample

    parse_j = b.new_python_job('Parse MultiQC JSON')
    row_by_sample_resource = parse_j.call(_parse_multiqc_json, multiqc_json)
    b.write_output(row_by_sample_resource.as_json(), row_by_sample_json_path)
    b.run(wait=True)
    return row_by_sample_json_path


def _get_stack_id(proj: str):
    """
    Matching the project ID to CPG stack/dataset
    """
    if proj == 'csiro-als':  # We don't have a project for ALS yet
        return 'nagim'
    return proj


def _get_sm_proj_id(proj: str, namespace: str):
    """
    Matching the project ID to a sample-metadata project.
    """
    stack = _get_stack_id(proj)
    if namespace != 'main':
        return f'{stack}-test'
    return stack


def _fix_sample_ids(samples: List[Sample], namespace: str):
    """
    Some samples processed with Terra use CPG IDs, so checking if we already
    have them in the SMDB, and fixing the external IDs.
    """
    sapi = SampleApi()

    def _find_sm_samples(namespace):
        sm_proj_ids = [
            _get_sm_proj_id(proj, namespace) for proj in PROJECT_ID_MAP.values()
        ]
        return sapi.get_samples(
            body_get_samples_by_criteria_api_v1_sample_post={
                'project_ids': sm_proj_ids,
                'active': True,
            }
        )

    # Setting in main namespace here, because TOB-WGS Nagim IDs match the main CPG IDs
    # test namespace will be processed separately further down
    sm_dicts = _find_sm_samples('main')
    cpgid_to_extid = {s['id']: s['external_id'] for s in sm_dicts}
    extid_to_cpgid = {s['external_id']: s['id'] for s in sm_dicts}

    # Fixing sample IDs. Some samples (tob-wgs and acute-care)
    # have CPG IDs as nagim ids, some don't
    for sample in samples:
        if sample.nagim_id in extid_to_cpgid:
            sample.ext_id = sample.nagim_id
            sample.cpg_id = extid_to_cpgid[sample.nagim_id]
        elif sample.nagim_id in cpgid_to_extid:
            sample.ext_id = cpgid_to_extid[sample.nagim_id]
            sample.cpg_id = sample.nagim_id
        else:
            sample.ext_id = sample.nagim_id

    if namespace == 'test':
        # Setting test sample IDs
        test_sm_dicts = _find_sm_samples('test')
        extid_to_test_cpgid = {s['external_id']: s['id'] for s in test_sm_dicts}
        for sample in samples:
            sample.main_cpg_id = sample.cpg_id
            sample.test_cpg_id = extid_to_test_cpgid.get(sample.ext_id)
            # if not sample.test_cpg_id:
            #     sample.test_cpg_id = extid_to_test_cpgid.get(sample.cpg_id)
            sample.cpg_id = sample.test_cpg_id


def _parse_sample_project_map(tsv_path: str) -> List[Sample]:
    """
    Initialize list of Sample object and set project IDs.
    """
    sample_by_nagim_id = {}
    df = pd.read_csv(tsv_path, sep='\t', header=None, names=['nagim_id', 'proj'])
    for (nagim_id, proj) in zip(df.nagim_id, df.proj):
        cpg_proj = PROJECT_ID_MAP.get(proj)
        if cpg_proj is None:
            raise ValueError(
                f'Unknown project {proj}. Known project IDs: {PROJECT_ID_MAP}'
            )
        if PROJECTS_TO_PROCESS and cpg_proj not in PROJECTS_TO_PROCESS:
            continue
        sample_by_nagim_id[nagim_id] = Sample(
            nagim_id=nagim_id,
            project_id=cpg_proj,
        )
    logger.info(f'Read {len(sample_by_nagim_id)} samples from {tsv_path}')
    return list(sample_by_nagim_id.values())


def _subset_to_test(samples: List[Sample]) -> List[Sample]:
    test_samples = []
    for proj, cnt in {
        'thousand-genomes': 30,
        'hgdp': 20,
        'csiro-als': 5,
        'amp-pd': 10,
        'mgrb': 15,
        'tob-wgs': 10,
        'acute-care': 10,
    }.items():
        proj_samples = [
            s
            for s in samples
            if _get_sm_proj_id(s.project_id, 'main') == _get_sm_proj_id(proj, 'main')
        ]
        test_samples.extend(proj_samples[:cnt])
    return test_samples


def _add_hgdp_1kg_metadata(samples, tmp_dir, overwrite=False):
    """
    Use gnomAD to pull metadata for 1KG and HGDP samples from gnomAD.
    """
    cache_path = join(tmp_dir, 'hgdp-1kg-meta.csv')
    if not can_reuse(cache_path, overwrite):
        mt = hl.read_matrix_table(
            'gs://gcp-public-data--gnomad/release/3.1/mt/genomes/'
            'gnomad.genomes.v3.1.hgdp_1kg_subset.mt/'
        )
        ht = mt.cols()
        ht = ht.select(
            ht.labeled_subpop,
            ht.population_inference.pop,
            ht.sex_imputation.sex_karyotype,
        )
        ht.to_pandas().to_csv(cache_path, index=False)

    df = pd.read_csv(cache_path)

    row_by_sid = {}
    for _, row in df.iterrows():
        row_by_sid[row['s']] = row

    for s in samples:
        row = row_by_sid.get(s.ext_id)
        if row is not None:
            s.meta['continental_pop'] = row['pop']
            s.meta['subcontinental_pop'] = row['labeled_subpop']
            s.meta['sex'] = {'XY': 1, 'XX': 2}.get(row['sex_karyotype'], 0)


def _add_batch_job(hbatch, job_name: str, ncpu: int = 2) -> Job:
    """
    Add cmd as a Batch job.
    """
    j = hbatch.new_job(job_name)
    j.cpu(ncpu)
    if ncpu > 16:
        # only lowmem machines support 32 cores
        j.memory('lowmem')
    j.image('australia-southeast1-docker.pkg.dev/cpg-common/images/aspera:v1')
    j.command('export GOOGLE_APPLICATION_CREDENTIALS=/gsa-key/key.json')
    j.command(
        'gcloud -q auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS'
    )
    return j


def _call(cmd):
    """
    Call subprocess command locally.
    """
    logger.info(cmd)
    subprocess.run(cmd, shell=True, check=True)


class NagimParser(GenericParser):
    """
    Inherits from sample_metadata's GenericParser class and implements parsing
    logic specific to the NAGIM project.
    """

    async def get_sample_meta(self, sample_id: str, row: GroupedRow) -> Dict[str, Any]:
        if isinstance(row, dict):
            row = [row]
        meta = {}
        for r in row:
            meta['project'] = r['project']
            for key in [
                'continental_pop',
                'subcontinental_pop',
                'sex',
            ]:
                val = r.get(f'meta_{key}')
                if val:
                    meta[key] = val
        return meta

    def __init__(self, multiqc_html_path, multiqc_json_path, **kwargs):
        super().__init__(**kwargs)
        self.multiqc_html_path = multiqc_html_path
        self.multiqc_json_path = multiqc_json_path

    def get_sample_id(self, row: Dict[str, Any]) -> str:
        return row['ext_id']

    async def get_analyses(
        self,
        sample_id: str,
        row: GroupedRow,
        cpg_id: Optional[str],
    ) -> List[AnalysisModel]:
        """
        Creating "staging" analyses for uploaded GVCFs and CRAMs.
        """
        assert not isinstance(row, list)
        results = []

        for analysis_type in ['gvcf', 'cram']:
            if analysis_type not in [s.lower() for s in SOURCES_TO_PROCESS]:
                continue

            file_path = row.get(analysis_type)
            if not file_path:
                continue

            results.append(
                AnalysisModel(
                    sample_ids=['<none>'],
                    type=AnalysisType(analysis_type),
                    status=AnalysisStatus('completed'),
                    output=file_path,
                    meta={
                        'project': row.get('project'),
                        # To distinguish TOB processed on Terra as part from NAGIM
                        # from those processed at the KCCG:
                        'source': NAGIM_PROJ_ID,
                        # Indicating that files are in the -upload bucket:
                        'upload': row.get(f'{analysis_type}_upload'),
                        # Indicating that files need to be renamed to use CPG IDs,
                        # and moved from -upload to -test/-main/-archvie.
                        # (For gvcf, also need to reblock):
                        'staging': row.get(f'{analysis_type}_staging'),
                        # Indicating that files are in the -archive bucket:
                        'archive': row.get(f'{analysis_type}_archive'),
                    },
                )
            )
        return results

    async def get_qc_meta(
        self, sample_id: str, row: GroupedRow
    ) -> Optional[Dict[str, Any]]:
        """
        Create a QC analysis entry for found QC files.
        """
        assert not isinstance(row, list)

        if 'QC' not in SOURCES_TO_PROCESS:
            return None

        qc_data = {}
        for metric, _ in QC_METRICS:
            value = row.get(f'qc_value_{metric}')
            if not value:
                continue
            qc_data[metric] = value

        return {
            'metrics': qc_data,
            'html_file': self.multiqc_html_path,
            'json_file': self.multiqc_json_path,
            # To distinguish TOB processed on Terra as part from NAGIM
            # from those processed at the KCCG:
            'source': NAGIM_PROJ_ID,
            'project': row.get('project'),
        }

    async def get_sequence_meta(
        self, sample_id: str, row: GroupedRow
    ) -> Dict[str, Any]:
        if isinstance(row, list):
            row = row[0]

        result = {}
        for metric, _ in QC_METRICS:
            if f'qc_value_{metric}' in row:
                result[metric] = row[f'qc_value_{metric}']
        return result


def _cache_bucket_ls(
    ending_to_search: str,
    source_bucket: str,
    tmp_dir: str,
    overwrite: bool,
) -> List[str]:
    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)
    output_path = join(tmp_dir, f'sm-nagim-parser-gs-ls-{ending_to_search}.txt')
    if overwrite or not exists(output_path):
        _call(f'test ! -e {output_path} || rm {output_path}')
        _call(f'touch {output_path}')
        try:
            _call(f'gsutil ls "{source_bucket}/*.{ending_to_search}" >> {output_path}')
        except subprocess.CalledProcessError:
            pass
    with open(output_path) as f:
        return [line.strip() for line in f.readlines() if line.strip()]


@cli.command()
@click.option('--tmp-dir', 'tmp_dir')
@click.option('--dry-run', 'dry_run', is_flag=True)
def move_uploads(
    tmp_dir: str,
    dry_run: bool,
):
    """
    Assuming data is transferred to the upload buckets with transfer(),
    and samples are populated with parse(), move data from upload buckets
    to main and archive buckets, and update the SM analysis records.
    """
    if not tmp_dir:
        tmp_dir = tempfile.gettempdir()

    namespace = 'main'

    # Listing files that exist in the upload bucket.
    samples = _build_sample_list(tmp_dir, namespace, overwrite=False)

    hbatch = setup_batch(
        title='Process NAGIM uploads',
        keep_scratch=False,
        tmp_bucket=f'cpg-{NAGIM_PROJ_ID}-{namespace}-tmp',
        analysis_project_name=NAGIM_PROJ_ID,
    )

    for proj in PROJECT_ID_MAP.values():
        stack = _get_stack_id(proj)
        smdb_proj = _get_sm_proj_id(proj, namespace)

        def _move(j, file):
            j.command(
                f"""\
if gsutil -q stat "{file.moved_path}"
then
    echo "target file already exists"
else
    if gsutil -q stat "{file.upload_path}"
    then
        echo "source file exists, target does not, safely moving"
        gsutil mv "{file.upload_path}" "{file.moved_path}"
    else
        echo "source file does not exist"
        exit 1
    fi
fi"""
            )

        logger.info(f'{proj}: adding jobs to move CRAM uploads')
        new_bucket = SOURCES['CRAM'].get_final_bucket(stack, namespace)
        for s in samples:
            if s.project_id != proj:
                continue
            if s.cram:
                j = _add_batch_job(hbatch, f'{smdb_proj}/{s.cpg_id}: Move CRAM uploads')
                s.cram.moved_path = join(new_bucket, f'{s.cpg_id}.cram')
                s.crai.moved_path = s.cram.moved_path + '.crai'
                s.cram_md5.moved_path = s.cram.moved_path + '.md5'
                _move(j, s.cram)
                _move(j, s.crai)
                _move(j, s.cram_md5)

        logger.info(f'{proj}: adding jobs to move GVCF uploads')
        new_bucket = SOURCES['GVCF'].get_staging_bucket(stack, namespace)
        for s in samples:
            if s.project_id != proj:
                continue
            if s.gvcf:
                j = _add_batch_job(hbatch, f'{smdb_proj}/{s.cpg_id}: Move GVCF uploads')
                s.gvcf.moved_path = join(new_bucket, f'{s.cpg_id}.g.vcf.gz')
                s.tbi.moved_path = s.gvcf.moved_path + '.tbi'
                _move(j, s.gvcf)
                _move(j, s.tbi)

    hbatch.run(wait=True, dry_run=dry_run)
    if dry_run:
        return


cli.add_command(transfer)
cli.add_command(parse)
cli.add_command(move_uploads)

if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    cli()  # pylint: disable=unexpected-keyword-arg
