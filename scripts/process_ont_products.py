import os
import csv
import asyncio
import logging
from io import StringIO
from collections import namedtuple

from cpg_utils.hail_batch import dataset_path

import click

from sample_metadata.apis import SampleApi, AnalysisApi
from sample_metadata.model.sequence_type import SequenceType
from sample_metadata.model.analysis_model import AnalysisModel
from sample_metadata.model.analysis_type import AnalysisType
from sample_metadata.model.analysis_status import AnalysisStatus
from sample_metadata.parser.cloudhelper import CloudHelper
from sample_metadata.parser.generic_metadata_parser import (
    GenericMetadataParser,
    run_as_sync,
)
from sample_metadata.parser.generic_parser import chunk

OntAnalysesPreparer = namedtuple(
    'OntAnalysesPreparer',
    ('atype', 'fn_column', 'output_path', 'meta'),
)

logging.basicConfig(level=logging.DEBUG)


class Columns:
    """Columns for"""

    EXPERIMENT_NAME = 'Experiment name'
    SAMPLE_ID = 'Sample ID'
    ALIGNMENT_FILE = 'Alignment_file'
    ALIGNMENT_INDEX = 'Alignment_index'
    ALIGNMENT_SOFTWARE = 'Alignment_software'
    SV_FILE = 'SV_file'
    SV_INDEX = 'SV_index'
    SV_SOFTWARE = 'SV_software'
    SNV_FILE = 'SNV_file'
    SNV_INDEX = 'SNV_index'
    SNV_SOFTWARE = 'SNV_software'
    INDEL_FILE = 'Indel_file'
    INDEL_INDEX = 'Indel_index'
    INDEL_SOFTWARE = 'Indel_software'


class OntProductParser(
    CloudHelper
):  # pylint: disable=too-many-public-methods,too-many-arguments
    """Parser for ONT products (BAMS / GVCFs)"""

    def __init__(
        self,
        search_paths: list[str],
        project: str,
        dry_run: bool = True,
        confirm: bool = True,
    ):
        super().__init__(search_paths)

        self.project = project
        self.dry_run = dry_run
        self.confirm = confirm

        self.sapi = SampleApi()
        self.aapi = AnalysisApi()

    async def parse_manifest(self, file_pointer, delimiter):
        """Parse manifest"""
        reader = csv.DictReader(file_pointer, delimiter=delimiter)

        ds_by_ex_sample_id = {}

        for row in reader:
            ds_by_ex_sample_id[self.get_sample_id(row)] = row

        sid_map = self.sapi.get_sample_id_map_by_external(
            self.project, list(ds_by_ex_sample_id.keys())
        )

        analyses = []
        for grp in chunk(ds_by_ex_sample_id.items(), 20):
            processed = await asyncio.gather(
                *[self.process_row(row, sid_map[ex_sid]) for ex_sid, row in grp]
            )
            for p in processed:
                analyses.extend(p)

        if self.dry_run:
            logging.info(f'DRY_RUN: Inserting {len(analyses)} entries')
            return analyses

        m = f'Inserting {len(analyses)} entries for {len(sid_map)} samples'
        if self.confirm and not click.confirm(m):
            logging.info('Terminating early')
            return analyses

        for grp in chunk(analyses, 10):
            logging.info(f'Inserting {len(grp)} analyses')
            await asyncio.gather(
                *[self.aapi.create_new_analysis(self.project, a) for a in analyses]
            )

        return analyses

    def get_sample_id(self, row: dict[str, str]) -> str | None:
        """Get external sample ID from row"""
        return row[Columns.SAMPLE_ID]

    async def process_row(self, row: dict[str, str], cpg_sample_id: str):
        """Process ONT rows with list of analyses"""
        analyses = await self.get_analysis_objects_from_row(cpg_sample_id, row)
        return analyses

    async def get_analysis_objects_from_row(
        self, cpg_sample_id: str, row: dict[str, str]
    ):
        """
        Process row by moving files + generating list of analyses objects
        """
        analyses = []

        files = [
            OntAnalysesPreparer(
                atype='cram',
                fn_column=Columns.ALIGNMENT_FILE,
                output_path='crams/ont/',
                meta={'aligner': row[Columns.ALIGNMENT_SOFTWARE]},
            ),
            OntAnalysesPreparer(
                atype='custom',
                fn_column=Columns.SV_FILE,
                output_path='vcfs/ont/sv/',
                meta={'vcf_type': 'sv', 'caller': row[Columns.SV_SOFTWARE]},
            ),
            OntAnalysesPreparer(
                atype='custom',
                fn_column=Columns.SNV_FILE,
                output_path='vcfs/ont/snv/',
                meta={'vcf_type': 'snv', 'caller': row[Columns.SNV_SOFTWARE]},
            ),
            OntAnalysesPreparer(
                atype='custom',
                fn_column=Columns.INDEL_FILE,
                output_path='vcfs/ont/indels/',
                meta={'vcf_type': 'indel', 'caller': row[Columns.INDEL_SOFTWARE]},
            ),
        ]

        for preparer in files:
            fn = row[preparer.fn_column]
            src = self.file_path(fn, raise_exception=False)

            if not await self.file_exists(src):
                logging.info(f'File {src} does not exist, skipping analysis item')
                continue

            dest = os.path.join(dataset_path(preparer.output_path), fn)
            secondaries = self.get_secondaries_from_filename(fn)
            file_size = await self.file_size(src)

            self.move_gcs_file(src, dest)
            for secondary in secondaries:
                self.move_gcs_file(src + secondary, dest + secondary)

            meta = {
                'size': file_size,
                'sequence_type': SequenceType('ont'),
                **preparer.meta,
            }

            analyses.append(
                AnalysisModel(
                    type=AnalysisType(preparer.atype),
                    meta=meta,
                    status=AnalysisStatus('completed'),
                    sample_ids=[cpg_sample_id],
                )
            )

        return analyses

    @staticmethod
    def get_secondaries_from_filename(fn: str) -> list[str]:
        """
        Get secondary patterns from filename
        (by looking at extension)
        """
        ext_map = {
            '.bam': ['.bai'],
            '.cram': ['.crai'],
            '.vcf': ['.idx'],
            '.vcf.gz': ['.tbi'],
        }

        for ext, secondaries in ext_map.items():
            if fn.endswith(ext):
                return secondaries

        return []

    def move_gcs_file(self, src: str, dest: str):
        """Move GCS file by first copying, then deleting"""
        assert src.startswith('gs://') and dest.startswith('gs://')

        if self.dry_run:
            return logging.info(f'DRY_RUN: Moving {src} -> {dest}')

        src_blob = self.get_gcs_blob(src)
        src_bucket_name, src_rest = src.split('/', maxsplit=1)
        dest_bucket_name, dest_rest = src.split('/', maxsplit=1)
        src_bucket = self.get_gcs_bucket(src_bucket_name)
        dest_blob = src_bucket.copy_blob(
            src_blob, self.get_gcs_bucket(dest_bucket_name), dest_rest
        )
        src_bucket.delete_blob(src_rest)

        return dest_blob


@click.command(help='')
@click.option(
    '--dataset',
    required=True,
    help='The sample-metadata project ($DATASET) to import manifest into',
)
@click.option(
    '--confirm', is_flag=True, help='Confirm with user input before updating server'
)
@click.option('--dry-run', is_flag=True)
@click.option('--search-path', multiple=True, required=True)
@click.argument('manifests', nargs=-1)
@run_as_sync
async def main(
    dataset: str,
    search_path: list[str],
    dry_run: bool,
    confirm: bool,
    manifests: list[str],
):
    """Main driver function"""
    parser = OntProductParser(
        search_paths=search_path, project=dataset, dry_run=dry_run, confirm=confirm
    )

    for filename in manifests:
        delimiter = GenericMetadataParser.guess_delimiter_from_filename(filename)
        result = await parser.parse_manifest(
            StringIO(await parser.file_contents(filename)), delimiter=delimiter
        )
        if result:
            print(result)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
