#!/usr/bin/env python3
import logging

import click

from metamist.parser.generic_metadata_parser import GenericMetadataParser, run_as_sync
from metamist.parser.generic_parser import DefaultSequencing, SingleRow

PARTICIPANT_COL_NAME = 'individual_id'
SAMPLE_ID_COL_NAME = 'sample_id'
READS_COL_NAME = 'filenames'
SEQ_TYPE_COL_NAME = 'type'
CHECKSUM_COL_NAME = 'checksum'
SEQ_FACILITY_COL_NAME = 'sequencing_facility'
SEQ_LIBRARY_COL_NAME = 'sequencing_library'
READ_END_TYPE_COL_NAME = 'read_end_type'
READ_LENGTH_COL_NAME = 'read_length'

KeyMap = {
    PARTICIPANT_COL_NAME: [
        'individual id',
        'individual',
        'individual_id',
        'participant',
        'participant-id',
        'participant_id',
        'participant id',
        'agha_study_id',
    ],
    SAMPLE_ID_COL_NAME: ['sample_id', 'sample', 'sample id'],
    READS_COL_NAME: ['filename', 'filenames', 'files', 'file'],
    SEQ_TYPE_COL_NAME: ['type', 'types', 'sequencing type', 'sequencing_type'],
    SEQ_FACILITY_COL_NAME: ['facility', 'sequencing facility', 'sequencing_facility'],
    SEQ_LIBRARY_COL_NAME: ['library', 'library_prep', 'library prep', 'library type', 'library_type', 'sequencing_library', 'sequencing library'],
    READ_END_TYPE_COL_NAME: ['read_end_type', 'read end type', 'read_end_types', 'read end types', 'end type', 'end_type', 'end_types', 'end types'],
    READ_LENGTH_COL_NAME: ['length', 'read length', 'read_length', 'read lengths', 'read_lengths'],
    CHECKSUM_COL_NAME: ['md5', 'checksum'],
}

required_keys = [SAMPLE_ID_COL_NAME, READS_COL_NAME]

__DOC = """
The SampleFileMapParser is used for parsing files with format:

- ['Individual ID']
- 'Sample ID'
- 'Filenames'
- ['Type']
- 'Checksum'
- ['Sequencing Facility'] - needed for exome & rna samples
- ['Library Type'] - needed for exome & rna samples
- ['End Type'] - needed for rna samples
- ['Read Length'] - needed for rna samples

e.g.
    Sample ID       Filenames
    <sample-id>     <sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz
    # OR
    <sample-id2>    <sample-id2>.filename-R1.fastq.gz
    <sample-id2>    <sample-id2>.filename-R2.fastq.gz

Example with optional columns
Note: Individual ID column must contain values in every row
Note: Any missing values in Type will default to the default_sequencing_type ('genome')
e.g.
    Individual ID	Sample ID	    Filenames	                                                                    Type
    Demeter	        sample_id001	sample_id001.filename-R1.fastq.gz,sample_id001.filename-R2.fastq.gz	            WGS
    Demeter	        sample_id001	sample_id001.exome.filename-R1.fastq.gz,sample_id001.exome.filename-R2.fastq.gz	WES
    Apollo	        sample_id002	sample_id002.filename-R1.fastq.gz	                                            WGS
    Apollo	        sample_id002	sample_id002.filename-R2.fastq.gz	                                            WGS
    Athena	        sample_id003	sample_id003.filename-R1.fastq.gz
    Athena	        sample_id003	sample_id003.filename-R2.fastq.gz
    Apollo	        sample_id004	sample_id004.filename-R1.fastq.gz
    Apollo	        sample_id004	sample_id004.filename-R2.fastq.gz

Example with optional columns for RNA samples
e.g.
    Individual ID	Sample ID	    Filenames	                                                            Type        Facility  Library    End Type  Read Length
    Hera            sample_id001	sample_id001_TSStrtRNA_R1.fastq.gz,sample_id001_TSStrtRNA_R2.fastq.gz	totalrna    VCGS      TSStrtRNA  paired    151
    Hestia          sample_id002	sample_id002_TSStrmRNA_R1.fastq.gz,sample_id002_TSStrmRNA_R2.fastq.gz	polyarna    VCGS      TSStrmRNA  paired    151


This format is useful for ingesting filenames for the seqr loading pipeline
"""

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class SampleFileMapParser(GenericMetadataParser):
    """Parser for SampleFileMap"""

    def __init__(
        self,
        search_locations: list[str],
        project: str,
        default_sample_type='blood',
        default_sequencing=DefaultSequencing(
            seq_type='genome', technology='short-read', platform='illumina'
        ),
        default_read_end_type: str = None,
        default_read_length: str | int = None,
        allow_extra_files_in_search_path=False,
        default_reference_assembly_location: str | None = None,
        verbose=True,
        **kwargs,
    ):
        super().__init__(
            search_locations=search_locations,
            project=project,
            participant_column=PARTICIPANT_COL_NAME,
            sample_name_column=SAMPLE_ID_COL_NAME,
            reads_column=READS_COL_NAME,
            checksum_column=CHECKSUM_COL_NAME,
            seq_type_column=SEQ_TYPE_COL_NAME,
            seq_facility_column=SEQ_FACILITY_COL_NAME,
            seq_library_column=SEQ_LIBRARY_COL_NAME,
            read_end_type_column=READ_END_TYPE_COL_NAME,
            read_length_column=READ_LENGTH_COL_NAME,
            default_sample_type=default_sample_type,
            default_sequencing=default_sequencing,
            default_read_end_type=default_read_end_type,
            default_read_length=default_read_length,
            default_reference_assembly_location=default_reference_assembly_location,
            allow_extra_files_in_search_path=allow_extra_files_in_search_path,
            key_map=KeyMap,
            verbose=verbose,
            **kwargs,
        )

    def get_sample_id(self, row: SingleRow) -> str:
        """Get external sample ID from row"""

        if self.sample_name_column and self.sample_name_column in row:
            return row[self.sample_name_column]

        return self.get_participant_id(row)

    @staticmethod
    def get_info() -> tuple[str, str]:
        """
        Information about parser, including short name and version
        """
        return ('sfmp', 'v1')


@click.command(help=__DOC)
@click.option(
    '--project',
    help='The metamist project to import manifest into',
)
@click.option('--default-sample-type', default='blood')
@click.option('--default-sequencing-type', default='wgs')
@click.option('--default-sequencing-technology', default='short-read')
@click.option('--default-sequencing-facility', default=None)
@click.option('--default-sequencing-library', default=None)
@click.option('--default-read-end-type', default=None)
@click.option('--default-read-length', default=None)
@click.option(
    '--default-reference-assembly',
    required=False,
    help=(
        'CRAMs require a reference assembly to realign. '
        'This must be provided if any of the reads are crams'
    ),
)
@click.option(
    '--search-path',
    multiple=True,
    required=True,
    help='Search path to search for files within',
)
@click.option(
    '--allow-extra-files-in-search-path',
    is_flag=True,
    help='By default, this parser will fail if there are crams, bams, fastqs '
    'in the search path that are not covered by the sample map.',
)
@click.option(
    '--confirm', is_flag=True, help='Confirm with user input before updating server'
)
@click.option(
    '--dry-run', is_flag=True, help='Just prepare the run, without comitting it'
)
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
@click.argument('manifests', nargs=-1)
@run_as_sync
async def main(  # pylint: disable=too-many-arguments
    manifests,
    search_path: list[str],
    project,
    default_sample_type='blood',
    default_sequencing_type='genome',
    default_sequencing_technology='short-read',
    default_sequencing_platform='illumina',
    default_sequencing_facility: str = None,
    default_sequencing_library: str = None,
    default_read_end_type: str = None,
    default_read_length: str = None,
    default_reference_assembly: str = None,
    allow_extra_files_in_search_path=False,
    confirm=False,
    dry_run=False,
    verbose=False,
):
    """Run script from CLI arguments"""
    if not manifests:
        raise ValueError('Expected at least 1 manifest')

    extra_search_paths = [m for m in manifests if m.startswith('gs://')]
    if extra_search_paths:
        search_path = list(set(search_path).union(set(extra_search_paths)))

    parser = SampleFileMapParser(
        project=project,
        default_sample_type=default_sample_type,
        default_sequencing=DefaultSequencing(
            seq_type=default_sequencing_type,
            technology=default_sequencing_technology,
            platform=default_sequencing_platform,
            facility=default_sequencing_facility,
            library=default_sequencing_library,
        ),
        default_read_end_type=default_read_end_type,
        default_read_length=default_read_length,
        default_reference_assembly_location=default_reference_assembly,
        search_locations=search_path,
        allow_extra_files_in_search_path=allow_extra_files_in_search_path,
        verbose=verbose,
    )
    for manifest in manifests:
        logger.info(f'Importing {manifest}')
        resp = await parser.from_manifest_path(
            manifest=manifest,
            confirm=confirm,
            dry_run=dry_run,
        )
        print(resp)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
