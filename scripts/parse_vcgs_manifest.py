# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,wrong-import-order
import logging
import os
import re
from typing import Any, Dict, List

import click

from metamist.parser.generic_metadata_parser import (
    GenericMetadataParser,
    SingleRow,
    run_as_sync,
)

rmatch = re.compile(r'_[Rr]\d')

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class Columns:
    """Column keys for VCGS manifest"""

    PHS_ACCESSION = 'phs_accession'
    SAMPLE_NAME = 'sample_name'
    LIBRARY_ID = 'library_ID'
    TITLE = 'title'
    LIBRARY_STRATEGY = 'library_strategy'
    LIBRARY_SOURCE = 'library_source'
    LIBRARY_SELECTION = 'library_selection'
    LIBRARY_LAYOUT = 'library_layout'
    PLATFORM = 'platform'
    INSTRUMENT_MODEL = 'instrument_model'
    DESIGN_DESCRIPTION = 'design_description'
    REFERENCE_GENOME_ASSEMBLY = 'reference_genome_assembly'
    ALIGNMENT_SOFTWARE = 'alignment_software'
    FORWARD_READ_LENGTH = 'forward_read_length'
    REVERSE_READ_LENGTH = 'reverse_read_length'
    FILETYPE = 'filetype'
    FILENAME = 'filename'

    @staticmethod
    def participant_meta_map():
        """Participant meta map, default return nothing for vcgs"""
        return {}

    @staticmethod
    def sequence_meta_map():
        """Columns that will be put into sequence.meta"""
        fields = [
            Columns.LIBRARY_ID,
            Columns.LIBRARY_STRATEGY,
            Columns.LIBRARY_SOURCE,
            Columns.LIBRARY_SELECTION,
            Columns.LIBRARY_LAYOUT,
            Columns.PLATFORM,
            Columns.INSTRUMENT_MODEL,
            Columns.DESIGN_DESCRIPTION,
            Columns.REFERENCE_GENOME_ASSEMBLY,
            Columns.FORWARD_READ_LENGTH,
            Columns.REVERSE_READ_LENGTH,
        ]
        return {k: k for k in fields}

    @staticmethod
    def sample_meta_map():
        """Columns that will be put into sample.meta"""
        fields = [Columns.PHS_ACCESSION]
        return {k: k for k in fields}


class VcgsManifestParser(GenericMetadataParser):
    """Parser for VCGS manifest"""

    def __init__(
        self,
        project: str,
        search_locations: List[str],
        default_sequencing_type='wgs',
        default_sample_type='blood',
        allow_extra_files_in_search_path=False,
    ):
        super().__init__(
            project=project,
            search_locations=search_locations,
            default_sequencing_type=default_sequencing_type,
            default_sample_type=default_sample_type,
            sample_primary_eid_column=Columns.SAMPLE_NAME,
            reads_column=Columns.FILENAME,
            participant_meta_map=Columns.participant_meta_map(),
            sample_meta_map=Columns.sample_meta_map(),
            assay_meta_map=Columns.sequence_meta_map(),
            qc_meta_map={},
            allow_extra_files_in_search_path=allow_extra_files_in_search_path,
        )

    def get_primary_sample_id(self, row: Dict[str, Any]) -> str:
        """Get external sample ID from row"""
        external_id = row[self.sample_primary_eid_column]
        if '-' in external_id:
            external_id = external_id.split('-')[0]
        return external_id

    def get_sequencing_type(self, row: SingleRow) -> str:
        """
        Parse sequencing type (wgs / single-cell, etc)
        """
        sample_id = self.get_primary_sample_id(row)

        types = row.get(Columns.LIBRARY_STRATEGY, None)
        if isinstance(types, str):
            types = [types]
        if len(types) <= 0:
            if (
                self.default_sequencing.seq_type is None
                or self.default_sequencing.seq_type.lower() == 'none'
            ):
                raise ValueError(
                    f"Couldn't detect sequence type for sample {sample_id}, and "
                    'no default was available.'
                )
            return self.default_sequencing.seq_type
        if len(types) > 1:
            raise ValueError(
                f'Multiple library types for same sample {sample_id}, '
                f'maybe there are multiples types of sequencing in the same '
                f'manifest? If so, please raise an issue with mfranklin to '
                f'change the group_by to include {Columns.LIBRARY_STRATEGY}.'
            )

        type_ = types[0].lower()
        if type_ == 'wgs':
            return 'genome'
        if type_ in ('single-cell', 'ss'):
            return 'single-cell'

        raise ValueError(f'Unrecognised sequencing type {type_}')


@click.command(help='GCS path to manifest file')
@click.option(
    '--project',
    help='The metamist project to import manifest into (probably "seqr")',
)
@click.option('--default-sample-type', default='blood')
@click.option('--default-sequence-type', default='wgs')
@click.option(
    '--confirm', is_flag=True, help='Confirm with user input before updating server'
)
@click.option('--search-path', multiple=True, required=False)
@click.option(
    '--allow-extra-files-in-search_path',
    is_flag=True,
    help='By default, this parser will fail if there are crams, bams, fastqs '
    'in the search path that are not covered by the sample map.',
)
@click.argument('manifests', nargs=-1)
@run_as_sync
async def main(
    manifests,
    project,
    default_sample_type='blood',
    default_sequencing_type='wgs',
    confirm=False,
    search_path: List[str] = None,
    allow_extra_files_in_search_path=False,
):
    """Run script from CLI arguments"""
    _search_locations = search_path or list(set(os.path.basename(s) for s in manifests))
    parser = VcgsManifestParser(
        default_sample_type=default_sample_type,
        default_sequencing_type=default_sequencing_type,
        project=project,
        search_locations=_search_locations,
        allow_extra_files_in_search_path=allow_extra_files_in_search_path,
    )
    for manifest in manifests:
        logger.info(f'Importing {manifest}')
        resp = await parser.from_manifest_path(
            manifest=manifest,
            confirm=confirm,
            delimiter='\t',
        )
        print(resp)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
