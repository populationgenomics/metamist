# pylint: disable=R0904,too-many-instance-attributes,too-many-locals,unused-argument,wrong-import-order,unused-argument,too-many-arguments,unused-import,too-many-lines
import asyncio
import logging
import re
import shlex
from functools import reduce
from typing import Any

import click

from metamist.parser.generic_parser import (  # noqa
    DefaultSequencing,
    GenericParser,
    GroupedRow,
    ParsedAnalysis,
    ParsedAssay,
    ParsedSample,
    ParsedSequencingGroup,
    SingleRow,
    run_as_sync,
)

__DOC = """
Parse CSV / TSV manifest of arbitrary format.
This script allows you to specify HOW you want the manifest
to be mapped onto individual data.

This script loads the WHOLE file into memory

It groups rows by the sample ID, and collapses metadata from rows.

EG:
    Sample ID       sample-collection-date  depth  qc_quality  Fastqs

    <sample-id>     2021-09-16              30x    0.997       <sample-id>.filename-R1.fastq.gz,<sample-id>.filename-R2.fastq.gz

    # OR

    <sample-id2>    2021-09-16              30x    0.997       <sample-id2>.filename-R1.fastq.gz

    <sample-id2>    2021-09-16              30x    0.997       <sample-id2>.filename-R2.fastq.gz

Given the files are in a bucket called 'gs://cpg-upload-bucket/collaborator',
and we want to achieve the following:

- Import this manifest into the "$dataset" project of SM
- Map the following to `sample.meta`:
    - "sample-collection-date" -> "collection_date"
- Map the following to `assay.meta`:
    - "depth" -> "depth"
    - "qc_quality" -> "qc.quality" (ie: {"qc": {"quality": 0.997}})
- Add a qc analysis object with the following mapped `analysis.meta`:
    - "qc_quality" -> "quality"

python parse_generic_metadata.py \
    --project $dataset \
    --sample-name-column "Sample ID" \
    --reads-column "Fastqs" \
    --sample-meta-field-map "sample-collection-date" "collection_date" \
    --assay-meta-field "depth" \
    --assay-meta-field-map "qc_quality" "qc.quality" \
    --qc-meta-field-map "qc_quality" "quality" \
    --search-path "gs://cpg-upload-bucket/collaborator" \
    <manifest-path>
"""


logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

RE_FILENAME_SPLITTER = re.compile('[,;]')


class GenericMetadataParser(GenericParser):
    """Parser for GenericMetadataParser"""

    def __init__(
        self,
        project: str,
        search_locations: list[str],
        # sample columns
        sample_primary_eid_column: str,
        sample_external_id_column_map: dict[str, str] | None = None,
        # Participant columns
        participant_primary_eid_column: str | None = None,
        reported_sex_column: str | None = None,
        reported_gender_column: str | None = None,
        karyotype_column: str | None = None,
        participant_external_id_column_map: dict[str, str] | None = None,
        # Sequencing metadata columns
        seq_type_column: str | None = None,
        seq_technology_column: str | None = None,
        seq_platform_column: str | None = None,
        seq_facility_column: str | None = None,
        seq_library_column: str | None = None,
        # Assay columns
        assay_external_id_column: str | None = None,
        reads_column: str | None = None,
        checksum_column: str | None = None,
        read_end_type_column: str | None = None,
        read_length_column: str | None = None,
        reference_assembly_location_column: str | None = None,
        # GVCF columns
        gvcf_column: str | None = None,
        # Meta field key maps
        participant_meta_map: dict[str, str] | None = None,
        sample_meta_map: dict[str, str] | None = None,
        assay_meta_map: dict[str, str] | None = None,
        qc_meta_map: dict[str, str] | None = None,
        # Default values
        default_reference_assembly_location: str | None = None,
        default_sample_type: str | None = None,
        default_sequencing=DefaultSequencing(
            seq_type='genome', technology='short-read', platform='illumina'
        ),
        default_read_end_type: str | None = None,
        default_read_length: str | int | None = None,
        batch_number: str | None = None,
        allow_extra_files_in_search_path=False,
        **kwargs,
    ):
        """
        Configuration options for a GenericMetadata parser.

        Args:
            project (str): The name of the project to insert into
            search_locations (list[str]):
                A list of locations to search for unqualified files in.

            sample_primary_eid_column (str): The name of the column containing the sample name.
            sample_external_id_column_map (str | None, optional):
                {column: eid_name} mapping for sample external_ids. This should NOT
                include the sample_primary_eid_column.
            sample_meta_map (dict[str, str] | None, optional):
                A mapping of column names to keys for sample.meta.
                {[column]: [metadata_key]}
            default_sample_type (str | None, optional):
                The default sample type to use if not provided in the manifest.

            participant_primary_eid_column (str | None, optional):
                The name of the column containing the participant name. Unsupplied means
                there are no participants represented in the file.
            reported_sex_column (str | None, optional):
                The name of the column containing the reported sex, this will get parsed
                into (0 = unknown, 1 = male, 2 = female), otherwise, ValueError
            reported_gender_column (str | None, optional):
                The name of the column containing the reported gender, no validation.
            karyotype_column (str | None, optional):
                The name of the column containing the karyotype, no validation.
            participant_external_id_column_map (str | None, optional):
                {column: eid_name} mapping for participant external_ids.
                This should NOT include the participant_primary_eid_column.
            participant_meta_map (dict[str, str] | None, optional):
                A mapping of column names to keys for participant.meta.
                {[column]: [metadata_key]}

            default_sequencing (_type_, optional):
                Default sequencing values to use when not found.Defaults to
                DefaultSequencing( seq_type='genome', technology='short-read', platform='illumina' ).
            seq_type_column (str | None, optional):
                The name of the column containing the sequencing type.
            seq_technology_column (str | None, optional):
                The name of the column containing the sequencing technology.
            seq_platform_column (str | None, optional):
                The name of the column containing the sequencing platform.
            seq_facility_column (str | None, optional):
                The name of the column containing the sequencing facility.
            seq_library_column (str | None, optional):
                The name of the column containing the sequencing library.

            assay_id_column (str | None, optional):
                The name of the column containing an external assay ID to group on.
            assay_meta_map (dict[str, str] | None, optional):
                A mapping of column names to keys for assay.meta.
                {[column]: [metadata_key]}
            reads_column (str | None, optional):
                The name of the column containing the reads.
            checksum_column (str | None, optional):
                The name of the column containing the checksum.
            read_end_type_column (str | None, optional):
                The name of the column containing the read end type.
            default_read_end_type (str | None, optional):
                The default read end type to use if not provided in the manifest.
            read_length_column (str | None, optional):
                The name of the column containing the read length.
            default_read_length (str | int | None, optional):
                The default read length to use if not provided in the manifest.
            reference_assembly_location_column (str | None, optional):
                The name of the column containing the reference assembly location.
            default_reference_assembly_location (str | None, optional):
                Path to a reference_assembly where the reads_type is a CRAM

            gvcf_column (str | None, optional):
                The name of the column containing the path to a gvcf

            qc_meta_map (dict[str, str] | None, optional):
                A mapping of column names to keys for QC analysis.meta

            batch_number (str | None, optional): a batch number to assign to the samples
            allow_extra_files_in_search_path (bool, optional):
                Whether to allow files in the search path that are not in the manifest.
                This could be useful to ensure that all files uploaded to some dir
                are covered by the manifest, and there aren't extras.
        """
        super().__init__(
            path_prefix=None,
            search_paths=search_locations,
            project=project,
            default_sample_type=default_sample_type,
            default_sequencing=default_sequencing,
            default_read_end_type=default_read_end_type,
            default_read_length=default_read_length,
            **kwargs,
        )

        if not sample_primary_eid_column:
            raise ValueError('A sample name column MUST be provided')

        self.cpg_id_column = 'Internal CPG Sequencing Group ID'
        self.sample_primary_eid_column = sample_primary_eid_column
        self.sample_external_id_column_map = sample_external_id_column_map or {}

        # Participant columns
        self.participant_primary_eid_column = participant_primary_eid_column
        self.participant_external_id_column_map = (
            participant_external_id_column_map or {}
        )
        self.reported_sex_column = reported_sex_column
        self.reported_gender_column = reported_gender_column
        self.karyotype_column = karyotype_column

        # Sequencing metadata columns
        self.seq_type_column = seq_type_column
        self.seq_technology_column = seq_technology_column
        self.seq_platform_column = seq_platform_column
        self.seq_facility_column = seq_facility_column
        self.seq_library_column = seq_library_column

        # Assay columns
        self.assay_id_column = assay_external_id_column
        self.reads_column = reads_column
        self.checksum_column = checksum_column
        self.read_end_type_column = read_end_type_column
        self.read_length_column = read_length_column
        self.reference_assembly_location_column = reference_assembly_location_column

        # Meta field key maps
        self.participant_meta_map = participant_meta_map or {}
        self.sample_meta_map = sample_meta_map or {}
        self.assay_meta_map = assay_meta_map or {}
        self.qc_meta_map = qc_meta_map or {}

        # GVCF column
        self.gvcf_column = gvcf_column

        # Default values
        self.batch_number = batch_number
        self.default_reference_assembly_location = default_reference_assembly_location
        self.allow_extra_files_in_search_path = allow_extra_files_in_search_path

    def get_primary_sample_id(self, row: SingleRow) -> str:
        """Get external sample ID from row"""
        return row[self.sample_primary_eid_column].strip()

    async def get_cpg_sample_id_from_row(self, row: SingleRow) -> str | None:
        """Get internal cpg id from a row using get_sample_id and an api call"""
        return row.get(self.cpg_id_column, None)

    def get_sample_type(self, row: GroupedRow) -> str:
        """Get sample type from row"""
        if self.default_sample_type:
            return self.default_sample_type
        return None

    def get_sequencing_types(self, row: GroupedRow) -> list[str]:
        """
        Get assay types from grouped row
        if SingleRow: return assay type
        if GroupedRow: return assay types for all rows
        """
        if isinstance(row, dict):
            return [self.get_sequencing_type(row)]
        return [
            str(r.get(self.seq_type_column, self.default_sequencing.seq_type))
            for r in row
        ]

    def get_sequencing_technology(self, row: SingleRow) -> str:
        """Get assay technology for single row"""
        value = (
            row.get(self.seq_technology_column, None)
            or self.default_sequencing.technology
        )
        value = value.lower()

        if value == 'ont':
            value = 'long-read'

        return str(value)

    def get_sequencing_platform(self, row: SingleRow) -> str:
        """Get sequencing platform for single row"""
        value = (
            row.get(self.seq_platform_column, None) or self.default_sequencing.platform
        )
        value = value.lower()

        return str(value)

    def get_sequencing_type(self, row: SingleRow) -> str:
        """Get assay type from row"""
        value = row.get(self.seq_type_column, None) or self.default_sequencing.seq_type
        value = value.lower()

        if value == 'wgs':
            value = 'genome'
        elif value == 'wes':
            value = 'exome'
        elif 'mt' in value:
            value = 'mtseq'
        elif 'polya' in value or 'mrna' in value:
            value = 'polyarna'
        elif 'total' in value:
            value = 'totalrna'
        elif 'single' in value:
            value = 'singlecellrna'

        return str(value)

    def get_sequencing_facility(self, row: SingleRow) -> str:
        """Get sequencing facility from row"""
        value = (
            row.get(self.seq_facility_column, None) or self.default_sequencing.facility
        )
        value = str(value) if value else None
        return value

    def get_sequencing_library(self, row: SingleRow) -> str:
        """Get sequencing library from row"""
        value = (
            row.get(self.seq_library_column, None) or self.default_sequencing.library
        )
        value = str(value) if value else None
        return value

    def get_read_end_type(self, row: SingleRow) -> str:
        """Get read end type from row"""
        value = row.get(self.read_end_type_column, None) or self.default_read_end_type
        value = str(value).lower() if value else None
        return value

    def get_read_length(self, row: SingleRow) -> int:
        """Get read length from row"""
        value = row.get(self.read_length_column, None) or self.default_read_length
        value = int(value) if value else None
        return value

    def get_assay_id(self, row: GroupedRow) -> dict[str, str] | None:
        """Get external assay ID from row. Needs to be implemented per parser.
        NOTE: To be re-thought after assay group changes are applied"""
        return None

    def get_primary_participant_id(self, row: SingleRow) -> str | None:
        """Get external participant ID from row"""
        if (
            not self.participant_primary_eid_column
            or self.participant_primary_eid_column not in row
        ):
            raise ValueError('Participant column does not exist')
        return row[self.participant_primary_eid_column]

    def get_reported_sex(self, row: GroupedRow) -> int | None:
        """Get reported sex from grouped row"""

        if not self.reported_sex_column:
            return None

        reported_sex = row[0].get(self.reported_sex_column, None)

        if reported_sex is None:
            return None
        if reported_sex == '':
            return None
        if reported_sex.lower() == 'female':
            return 2
        if reported_sex.lower() == 'male':
            return 1

        raise ValueError(
            f'{reported_sex} could not be identified as an input for reported_sex'
        )

    def get_reported_gender(self, row: GroupedRow) -> str | None:
        """Get reported gender from grouped row"""
        return row[0].get(self.reported_gender_column, None)

    def get_karyotype(self, row: GroupedRow) -> str | None:
        """Get karyotype from grouped row"""
        return row[0].get(self.karyotype_column, None)

    def has_participants(self, rows: list[SingleRow]) -> bool:
        """Returns True if the file has a Participants column"""
        return self.participant_primary_eid_column in rows[0]

    async def validate_rows(self, rows):
        await super().validate_rows(rows)

        errors = []
        errors.extend(await self.check_files_covered_by_rows(rows))

        if errors:
            raise ValueError(', '.join(errors))

    @staticmethod
    def flatten_irregular_list(irregular_list):
        """
        Flatten an irregular list: [1, [2, 3], 4]

        >>> GenericMetadataParser.flatten_irregular_list([1, [2, 3], [4,5]])
        [1, 2, 3, 4, 5]
        """
        return (
            [
                element
                for item in irregular_list
                for element in GenericMetadataParser.flatten_irregular_list(item)
            ]
            if isinstance(irregular_list, list)
            else [irregular_list]
        )

    async def get_all_files_from_row(self, sample_id: str, row):
        """Get all files from row, to allow subparsers to include other files"""
        fns = await self.get_read_filenames(sample_id, row)

        return self.flatten_irregular_list(fns)

    async def check_files_covered_by_rows(
        self, rows: list[dict[str, Any]]
    ) -> list[str]:
        """
        Check that the files in the search_paths are completely covered by the sample_map
        """
        filename_promises = []
        for grp in rows:
            for r in grp if isinstance(grp, list) else [grp]:
                filename_promises.append(
                    self.get_all_files_from_row(self.get_primary_sample_id(r), r)
                )

        files_from_rows: list[str] = sum(await asyncio.gather(*filename_promises), [])
        filenames_from_rows = set(f.strip() for f in files_from_rows if f and f.strip())
        relevant_extensions = ('.cram', '.fastq.gz', '.fastq', 'fq.gz', '.fq', '.bam')

        # we need to explicitly filter filenames from rows not to include absolute
        # paths, otherwise the below check will flag it as missing
        absolute_path_starts = ('/', 'gs://', 'https://')
        filenames_from_rows = set(
            f
            for f in filenames_from_rows
            if not (any(f.startswith(p) for p in absolute_path_starts))
        )

        def filename_filter(f):
            return any(f.endswith(ext) for ext in relevant_extensions)

        file_from_search_paths = set(filter(filename_filter, self.filename_map.keys()))

        files_in_search_path_not_in_map = file_from_search_paths - filenames_from_rows
        missing_files = filenames_from_rows - file_from_search_paths

        errors = []

        if missing_files:
            errors.append(
                'There are files specified in the map, but not found in '
                f'the search paths: {", ".join(missing_files)}'
            )
        if files_in_search_path_not_in_map:
            m = (
                'There are files in the search path that are NOT covered by the file map: '
                f'{", ".join(files_in_search_path_not_in_map)}'
            )
            if self.allow_extra_files_in_search_path:
                logger.warning(m)
            else:
                errors.append(m)

        return errors

    @staticmethod
    def merge_dicts(a: dict, b: dict):
        """
        Recursively merge two dictionaries:
        - collapse equal values
        - put differing values into a list (not guaranteeing order)
        """
        if b is None:
            return a
        if a is None:
            return b

        res = {}
        for key in set(a.keys()).union(b.keys()):
            a_val = a.get(key)
            b_val = b.get(key)
            if a_val is not None and b_val is not None:
                # combine values
                a_is_dict = isinstance(a_val, dict)
                b_is_dict = isinstance(b_val, dict)

                if a_is_dict and b_is_dict:
                    # merge dict
                    res[key] = GenericMetadataParser.merge_dicts(a_val, b_val)
                elif a_val == b_val:
                    res[key] = a_val
                else:
                    res[key] = [a_val, b_val]
            else:
                res[key] = a_val or b_val

        return res

    @staticmethod
    def collapse_arbitrary_meta(key_map: dict[str, str], row: GroupedRow):
        """
        This is a little bit tricky

        >>> GenericMetadataParser.collapse_arbitrary_meta({'key1': 'new_key'}, {'key1': True})
        {'new_key': True}

        >>> GenericMetadataParser.collapse_arbitrary_meta({'key1': 'new_key'}, [{'key1': True}, {'key1': True}])
        {'new_key': True}

        >>> GenericMetadataParser.collapse_arbitrary_meta({'key1': 'new_key'}, [{'key1': True}, {'key1': None}])
        {'new_key': True}

        >>> GenericMetadataParser.collapse_arbitrary_meta({'key1': 'new_key'}, [{'key1': True}])
        {'new_key': True}

        >>> GenericMetadataParser.collapse_arbitrary_meta({'key1': 'new.key'}, [{'key1': True}])
        {'new': {'key': True}}

        >>> GenericMetadataParser.collapse_arbitrary_meta({'key1': 'new_key'}, [{'key1': [1,2]}, {'key1': [2,3]}])
        {'new_key': [[1, 2], [2, 3]}

        >>> GenericMetadataParser.collapse_arbitrary_meta({'key1': 'new_key'}, [{}])
        {}

        >>> GenericMetadataParser.collapse_arbitrary_meta({'key1': 'new_key'}, [{'key1': None}])
        {}

        >>> GenericMetadataParser.collapse_arbitrary_meta({'key1': 'new.key'}, [{'key1': 1}, {'key1': 2}, {'key1': 3}])
        {'new': {'key': [1, 2, 3]}}

        # multiple keys sometimes is ordered, so check the sorted(dict.items())
        >>> import json; json.dumps(GenericMetadataParser.collapse_arbitrary_meta({'key1': 'new.key', 'key2': 'new.another'}, [{'key1': 1}, {'key1': 2}, {'key2': False}]), sort_keys=True)
        '{"new": {"another": false, "key": [1, 2]}}'
        """
        if not key_map or not row:
            return {}

        def unstring_value(value: Any) -> bool:
            """Convert strings to boolean or number if possible"""
            if not isinstance(value, str):
                return value
            if value.lower() == 'true':
                return True
            if value.lower() == 'false':
                return False
            try:
                return int(value)
            except ValueError:
                try:
                    return float(value)
                except ValueError:
                    return value

        def prepare_dict_from_keys(key_parts: list[str], val):
            """Recursive production of dictionary"""
            if len(key_parts) == 1:
                return {key_parts[0]: val}
            return {key_parts[0]: prepare_dict_from_keys(key_parts[1:], val)}

        dicts = [{}]
        for row_key, dict_key in key_map.items():
            if isinstance(row, list):
                if len(row) == 1:
                    value = unstring_value(row[0].get(row_key))
                else:
                    inner_values = [
                        unstring_value(r[row_key])
                        for r in row
                        if r.get(row_key) is not None
                    ]
                    if any(isinstance(inner, list) for inner in inner_values):
                        # lists are unhashable
                        value = inner_values
                    else:
                        value = sorted(
                            set(inner_values), key=str
                        )  # sorted for unit test consistency
                        if len(value) == 0:
                            continue
                        if len(value) == 1:
                            value = unstring_value(value[0])
            else:
                if row_key not in row:
                    continue
                value = unstring_value(row[row_key])
            dicts.append(prepare_dict_from_keys(dict_key.split('.'), value))

        return reduce(GenericMetadataParser.merge_dicts, dicts)

    @staticmethod
    def process_filename_value(string: str | list[str]) -> list[str]:
        """
        Split on multiple delimiters, ;,

        >>> GenericMetadataParser.process_filename_value('Filename1-fastq.gz;Filename2.fastq.gz')
        ['Filename1-fastq.gz', 'Filename2.fastq.gz']

        >>> GenericMetadataParser.process_filename_value('Filename1-fastq.gz, Filename2.fastq.gz')
        ['Filename1-fastq.gz', 'Filename2.fastq.gz']

        >>> GenericMetadataParser.process_filename_value('Filename1 with spaces fastq.gz')
        ['Filename1 with spaces fastq.gz']

        >>> GenericMetadataParser.process_filename_value(['filename ;filename2, filename3', ' filename4'])
        ['filename', 'filename2', 'filename3', 'filename4']
        """
        if not string:
            return []
        if isinstance(string, list):
            return sorted(
                set(
                    r
                    for f in string
                    for r in GenericMetadataParser.process_filename_value(f)
                )
            )

        filenames = [f.strip() for f in RE_FILENAME_SPLITTER.split(string)]
        filenames = [f for f in filenames if f]

        whitespace_filenames = [f for f in filenames if ' ' in f]
        if whitespace_filenames:
            logger.warning(
                'Whitespace detected in filenames: '
                + ','.join(shlex.quote(str(s)) for s in whitespace_filenames)
            )

        return filenames

    async def get_read_filenames(
        self, sample_id: str | None, row: SingleRow
    ) -> list[str]:
        """Get paths to reads from a row"""
        if not self.reads_column or self.reads_column not in row:
            return []
        # more post-processing
        return self.process_filename_value(row[self.reads_column])

    async def get_checksums_from_row(
        self, sample_id: str | None, row: SingleRow, read_filenames: list[str]
    ) -> list[str | None] | None:
        """
        Get checksums for some row, you must either return:
            - no elements, or
            - number of elements equal to read_filenames

        Each element should be a string or None.
        """
        if not self.checksum_column or self.checksum_column not in row:
            return []

        return self.process_filename_value(row[self.checksum_column])

    async def get_gvcf_filenames(self, sample_id: str, row: GroupedRow) -> list[str]:
        """Get paths to gvcfs from a row"""
        if not self.gvcf_column:
            return []

        gvcf_filenames: list[str] = []
        for r in row if isinstance(row, list) else [row]:
            if self.gvcf_column in r:
                gvcf_filenames.extend(self.process_filename_value(r[self.gvcf_column]))

        return gvcf_filenames

    async def get_sample_meta_from_group(self, rows: GroupedRow):
        """Get sample-metadata from row"""
        return self.collapse_arbitrary_meta(self.sample_meta_map, rows)

    def get_participant_external_ids(self, row: GroupedRow) -> dict[str, str]:
        eids = {}
        for r in row:
            for column, name in self.participant_external_id_column_map.items():
                new_eid = r.get(column)
                if not new_eid:
                    continue
                if name in eids and new_eid != eids[name]:
                    raise ValueError
                eids[name] = new_eid

        return eids

    def get_sample_external_ids(self, row: GroupedRow) -> dict[str, str]:
        eids = {}
        for r in row:
            for column, name in self.sample_external_id_column_map.items():
                new_eid = r.get(column)
                if not new_eid:
                    continue
                if name in eids and new_eid != eids[name]:
                    raise ValueError
                eids[name] = new_eid

        return eids

    async def get_participant_meta_from_group(self, rows: GroupedRow):
        """Get participant-metadata from rows then set it in the ParticipantMetaGroup"""
        return self.collapse_arbitrary_meta(self.participant_meta_map, rows)

    async def get_sequencing_group_meta(
        self, sequencing_group: ParsedSequencingGroup
    ) -> dict:
        """Get sequencing group metadata from variant (vcf) files"""
        meta: dict[str, Any] = {}
        gvcf_filenames: list[str] = []

        for r in sequencing_group.rows:
            if self.gvcf_column and self.gvcf_column in r:
                gvcf_filenames.extend(self.process_filename_value(r[self.gvcf_column]))

        # strip in case collaborator put "file1, file2"
        full_gvcf_filenames: list[str] = []

        if gvcf_filenames:
            full_gvcf_filenames.extend(
                self.file_path(f.strip()) for f in gvcf_filenames if f.strip()
            )

        variant_file_types: dict[str, dict[str, list]] = await self.parse_files(
            sequencing_group.sample.primary_external_id, full_gvcf_filenames, None
        )
        variants: dict[str, list] = variant_file_types.get('variants')

        if variants:
            if 'gvcf' in variants:
                meta['gvcfs'] = variants.get('gvcf')
                meta['gvcf_types'] = 'gvcf'

            if 'vcf' in variants:
                meta['vcfs'] = variants['vcf']
                meta['vcf_type'] = 'vcf'

        return meta

    async def get_read_and_ref_files_and_checksums(
        self, sample_id: str, rows: GroupedRow
    ) -> tuple[list[str], list[str], set[str]]:
        """Get read filenames and checksums from rows."""
        read_filenames: list[str] = []
        read_checksums: list[str] = []
        reference_assemblies: set[str] = set()
        for r in rows:
            _rfilenames = await self.get_read_filenames(sample_id=sample_id, row=r)
            read_filenames.extend(_rfilenames)
            if self.checksum_column and self.checksum_column in r:
                checksums = await self.get_checksums_from_row(sample_id, r, _rfilenames)
                if not checksums:
                    checksums = [None] * len(_rfilenames)
                read_checksums.extend(checksums)
            if self.reference_assembly_location_column:
                ref = r.get(self.reference_assembly_location_column)
                if ref:
                    reference_assemblies.add(ref)
        return read_filenames, read_checksums, reference_assemblies

    async def parse_cram_assays(
        self, sample: ParsedSample, reference_assemblies: set[str]
    ) -> dict[str, Any]:
        """Parse CRAM assays"""
        if len(reference_assemblies) > 1:
            # sorted for consistent testing
            str_ref_assemblies = ', '.join(sorted(reference_assemblies))
            raise ValueError(
                f'Multiple reference assemblies were defined for {sample.primary_external_id}: {str_ref_assemblies}'
            )
        if len(reference_assemblies) == 1:
            ref = next(iter(reference_assemblies))
        else:
            ref = self.default_reference_assembly_location
            if not ref:
                raise ValueError(
                    f'Reads type for {sample.primary_external_id!r} is CRAM, but a reference '
                    f'is not defined, please set the default reference assembly path'
                )

        ref_fp = self.file_path(ref)
        secondary_files = await self.create_secondary_file_objects_by_potential_pattern(
            ref_fp, ['.fai']
        )
        cram_reference = await self.create_file_object(
            ref_fp, secondary_files=secondary_files
        )
        return {'reference_assembly': cram_reference}

    async def get_assays_from_group(
        self, sequencing_group: ParsedSequencingGroup
    ) -> list[ParsedAssay]:
        """Get assays from assay group + rows"""
        sample = sequencing_group.sample
        rows = sequencing_group.rows

        collapsed_assay_meta = self.collapse_arbitrary_meta(self.assay_meta_map, rows)

        assays = []

        (
            read_filenames,
            read_checksums,
            reference_assemblies,
        ) = await self.get_read_and_ref_files_and_checksums(
            sample.primary_external_id, rows
        )

        # strip in case collaborator put "file1, file2"
        full_read_filenames: list[str] = []
        if read_filenames:
            full_read_filenames.extend(
                self.file_path(f.strip()) for f in read_filenames if f.strip()
            )
        read_file_types: dict[str, dict[str, list]] = await self.parse_files(
            sample.primary_external_id, full_read_filenames, read_checksums
        )
        reads: dict[str, list] = read_file_types.get('reads')
        if not reads:
            return []

        keys = list(reads.keys())
        if len(keys) > 1:
            # 2021-12-14 mfranklin: In future we should return multiple
            #       assay meta, and handle that in the generic parser
            raise ValueError(
                f'Multiple types of reads found ({", ".join(keys)}), currently not supported'
            )

        reads_type = keys[0]
        collapsed_assay_meta['reads_type'] = reads_type
        # collapsed_assay_meta['reads'] = reads[reads_type]

        if reads_type == 'cram':
            collapsed_assay_meta.update(
                await self.parse_cram_assays(sample, reference_assemblies)
            )

        if self.batch_number is not None:
            collapsed_assay_meta['batch'] = self.batch_number

        if sequencing_group.sequencing_type in [
            'exome',
            'polyarna',
            'totalrna',
            'singlecellrna',
        ]:
            rows = sequencing_group.rows
            # Exome / RNA should have facility and library, allow missing for exomes - for now
            if self.get_sequencing_facility(rows[0]):
                collapsed_assay_meta['sequencing_facility'] = (
                    self.get_sequencing_facility(rows[0])
                )
            if self.get_sequencing_library(rows[0]):
                collapsed_assay_meta['sequencing_library'] = (
                    self.get_sequencing_library(rows[0])
                )
            # RNA requires read end type and length as well
            if sequencing_group.sequencing_type != 'exome':
                collapsed_assay_meta['read_end_type'] = self.get_read_end_type(rows[0])
                collapsed_assay_meta['read_length'] = self.get_read_length(rows[0])
                # if any of the above fields are not set for an RNA assay, raise an error
                if not all(collapsed_assay_meta.values()):
                    raise ValueError(
                        f'Not all required fields were set for RNA sample {sample.primary_external_id}:\n'
                        f'{collapsed_assay_meta}\n'
                        f'Use the default value arguments if they are not present in the manifest.'
                    )

        for read in reads[reads_type]:
            assays.append(
                ParsedAssay(
                    group=sequencing_group,
                    # we don't determine which set of rows belong to a assay,
                    # as we grab all reads, and then determine assay
                    # grouping from there.
                    rows=sequencing_group.rows,
                    internal_assay_id=None,
                    external_assay_ids={},
                    # unfortunately hard to break them up by row in the current format
                    # assay_status=self.get_assay_status(rows),
                    assay_type='sequencing',
                    meta={
                        **collapsed_assay_meta,
                        'reads': read,
                        'sequencing_type': self.get_sequencing_type(rows[0]),
                        'sequencing_technology': sequencing_group.sequencing_technology,
                        'sequencing_platform': sequencing_group.sequencing_platform,
                    },
                )
            )
        if not sequencing_group.meta:
            sequencing_group.meta = self.get_sequencing_group_meta_from_assays(assays)

        return assays

    async def get_analyses_from_sequencing_group(
        self, sequencing_group: ParsedSequencingGroup
    ) -> list[ParsedAnalysis]:
        if not self.qc_meta_map:
            return []

        sample_id = sequencing_group.sample.primary_external_id

        return [
            ParsedAnalysis(
                sequencing_group=sequencing_group,
                status=self.get_analysis_status(sample_id, sequencing_group.rows),
                type_=self.get_analysis_type(sample_id, sequencing_group.rows),
                meta=self.collapse_arbitrary_meta(
                    self.qc_meta_map, sequencing_group.rows
                ),
                rows=sequencing_group.rows,
                output=None,
            )
        ]

    @staticmethod
    def get_info() -> tuple[str, str]:
        """
        Information about parser, including short name and version
        """
        return ('gmp', 'v1')


@click.command(help=__DOC)
@click.option(
    '--project',
    required=True,
    help='The metamist project ($DATASET) to import manifest into',
)
@click.option('--search-path', multiple=True, required=True)
@click.option('--sample-name-column', required=True)
@click.option(
    '--participant-column',
    help='Column where the external participant id is held',
)
@click.option(
    '--reported-sex-column',
    help='Column where the reported sex is held',
)
@click.option(
    '--reported-gender-column',
    help='Column where the reported gender is held',
)
@click.option(
    '--karyotype-column',
    help='Column where the karyotype is held',
)
@click.option(
    '--reads-column',
    help='Column where the reads information is held, comma-separated if multiple',
)
@click.option(
    '--gvcf-column',
    help='Column where the reads information is held, comma-separated if multiple',
)
@click.option(
    '--participant-meta-field',
    multiple=True,
    help='Single argument, key to pull out of row to put in participant.meta',
)
@click.option(
    '--participant-meta-field-map',
    nargs=2,
    multiple=True,
    help='Two arguments per listing, eg: --participant-meta-field-map "name-in-manifest" "name-in-participant.meta"',
)
@click.option(
    '--sample-meta-field',
    multiple=True,
    help='Single argument, key to pull out of row to put in sample.meta',
)
@click.option(
    '--sample-meta-field-map',
    nargs=2,
    multiple=True,
    help='Two arguments per listing, eg: --sample-meta-field-map "name-in-manifest" "name-in-sample.meta"',
)
@click.option(
    '--assay-meta-field',
    multiple=True,
    help='Single argument, key to pull out of row to put in sample.meta',
)
@click.option(
    '--assay-meta-field-map',
    nargs=2,
    multiple=True,
    help='Two arguments per listing, eg: --assay-meta-field "name-in-manifest" "name-in-assay.meta"',
)
@click.option(
    '--qc-meta-field-map',
    nargs=2,
    multiple=True,
    help='Two arguments per listing, eg: --qc-meta-field "name-in-manifest" "name-in-analysis.meta"',
)
@click.option('--default-sample-type', default='blood')
@click.option(
    '--confirm', is_flag=True, help='Confirm with user input before updating server'
)
@click.argument('manifests', nargs=-1)
@run_as_sync
async def main(
    manifests,
    project,
    search_path: list[str],
    sample_primary_eid_column: str,
    participant_column: str | None = None,
    reported_sex_column: str | None = None,
    reported_gender_column: str | None = None,
    karyotype_column: str | None = None,
    participant_meta_field: list[str] | None = None,
    participant_meta_field_map: list[tuple[str, str]] | None = None,
    sample_meta_field: list[str] | None = None,
    sample_meta_field_map: list[tuple[str, str]] | None = None,
    assay_meta_field: list[str] | None = None,
    assay_meta_field_map: list[tuple[str, str]] | None = None,
    qc_meta_field_map: list[tuple[str, str]] = None,
    reads_column: str | None = None,
    gvcf_column: str | None = None,
    default_sample_type: str | None = None,
    confirm=False,
):
    """Run script from CLI arguments"""
    if not manifests:
        raise ValueError('Expected at least 1 manifest')

    extra_search_paths = [m for m in manifests if m.startswith('gs://')]
    if extra_search_paths:
        search_path = list(set(search_path).union(set(extra_search_paths)))

    participant_meta_map: dict[Any, Any] = {}
    sample_meta_map: dict[Any, Any] = {}
    assay_meta_map: dict[Any, Any] = {}

    qc_meta_map = dict(qc_meta_field_map or {})
    if participant_meta_field_map:
        participant_meta_map.update(dict(participant_meta_map))
    if participant_meta_field:
        participant_meta_map.update({k: k for k in participant_meta_field})
    if sample_meta_field_map:
        sample_meta_map.update(dict(sample_meta_field_map))
    if sample_meta_field:
        sample_meta_map.update({k: k for k in sample_meta_field})
    if assay_meta_field_map:
        assay_meta_map.update(dict(assay_meta_field_map))
    if assay_meta_field:
        assay_meta_map.update({k: k for k in assay_meta_field})

    parser = GenericMetadataParser(
        project=project,
        sample_primary_eid_column=sample_primary_eid_column,
        participant_primary_eid_column=participant_column,
        reported_sex_column=reported_sex_column,
        reported_gender_column=reported_gender_column,
        karyotype_column=karyotype_column,
        participant_meta_map=participant_meta_map,
        sample_meta_map=sample_meta_map,
        assay_meta_map=assay_meta_map,
        qc_meta_map=qc_meta_map,
        reads_column=reads_column,
        gvcf_column=gvcf_column,
        default_sample_type=default_sample_type,
        default_sequencing=DefaultSequencing(
            seq_type='genome', technology='short-read', platform='illumina'
        ),
        search_locations=search_path,
    )
    for manifest in manifests:
        logger.info(f'Importing {manifest}')

        await parser.from_manifest_path(manifest=manifest, confirm=confirm)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
