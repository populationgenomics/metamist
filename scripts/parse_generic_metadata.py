#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,no-self-use,wrong-import-order,unused-argument,too-many-arguments
from typing import Dict, List, Optional, Tuple, Any

import os
import logging
from io import StringIO
from functools import reduce

import click

from parser import GenericParser, GroupedRow

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
- Map the following to `sequence.meta`:
    - "depth" -> "depth"
    - "qc_quality" -> "qc.quality" (ie: {"qc": {"quality": 0.997}})
- Add a qc analysis object with the following mapped `analysis.meta`:
    - "qc_quality" -> "quality"

python parse_generic_metadata.py \
    --sample-metadata-project $dataset \
    --sample-name-column "Sample ID" \
    --reads-column "Fastqs" \
    --sample-meta-field-map "sample-collection-date" "collection_date" \
    --sequence-meta-field "depth" \
    --sequence-meta-field-map "qc_quality" "qc.quality" \
    --qc-meta-field-map "qc_quality" "quality" \
    --search-path "gs://cpg-upload-bucket/collaborator" \
    <manifest-path>
"""

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class GenericMetadataParser(GenericParser):
    """Parser for GenericMetadataParser"""

    def __init__(
        self,
        search_locations: List[str],
        sample_name_column: str,
        sample_meta_map: Dict[str, str],
        sequence_meta_map: Dict[str, str],
        qc_meta_map: Dict[str, str],
        sample_metadata_project: str,
        reads_column: Optional[str] = None,
        gvcf_column: Optional[str] = None,
        default_sequence_type='wgs',
        default_sample_type='blood',
        confirm=False,
    ):
        path_prefix = search_locations[0] if search_locations else None

        super().__init__(
            path_prefix=path_prefix,
            sample_metadata_project=sample_metadata_project,
            default_sequence_type=default_sequence_type,
            default_sample_type=default_sample_type,
            confirm=confirm,
        )
        self.search_locations = search_locations
        self.filename_map = {}
        self.populate_filename_map(self.search_locations)

        if not sample_name_column:
            raise ValueError('A sample name column MUST be provided')

        self.sample_name_column = sample_name_column
        self.sample_meta_map = sample_meta_map or {}
        self.sequence_meta_map = sequence_meta_map or {}
        self.qc_meta_map = qc_meta_map or {}
        self.reads_column = reads_column
        self.gvcf_column = gvcf_column

    def populate_filename_map(self, search_locations: List[str]):
        """
        FileMapParser uses search locations based on the filename,
        so let's prepopulate that filename_map from the search_locations!
        """

        self.filename_map = {}
        for directory in search_locations:
            directory_list = self.list_directory(directory)

            for file in directory_list:
                file = file.strip()
                file_base = os.path.basename(file)
                if file_base in self.filename_map:
                    logger.warning(
                        f'File "{file}" already exists in directory map: {self.filename_map[file_base]}'
                    )
                    continue
                self.filename_map[file_base] = file

    def file_path(self, filename: str) -> str:
        """
        Get complete filepath of filename:
        - Includes gs://{bucket} if relevant
        - Includes path_prefix decided early on
        """
        if filename in self.filename_map:
            return self.filename_map[filename]

        return super().file_path(filename)

    def get_sample_id(self, row: Dict[str, any]) -> str:
        """Get external sample ID from row"""
        external_id = row[self.sample_name_column]
        return external_id

    @staticmethod
    def merge_dicts(a: Dict, b: Dict):
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
    def collapse_arbitrary_meta(key_map: Dict[str, str], row: GroupedRow):
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

        >>> GenericMetadataParser.collapse_arbitrary_meta({'key1': 'new.key'}, [{'key1': 1}, {'key1': 2}, {'key1': 3}])
        {'new': {'key': [1, 2, 3]}}

        # multiple keys sometimes is ordered, so check the sorted(dict.items())
        >>> import json; json.dumps(GenericMetadataParser.collapse_arbitrary_meta({'key1': 'new.key', 'key2': 'new.another'}, [{'key1': 1}, {'key1': 2}, {'key2': False}]), sort_keys=True)
        '{"new": {"another": false, "key": [1, 2]}}'
        """
        if not key_map or not row:
            return {}

        def prepare_dict_from_keys(key_parts: List[str], val):
            """Recursive production of dictionary"""
            if len(key_parts) == 1:
                return {key_parts[0]: val}
            return {key_parts[0]: prepare_dict_from_keys(key_parts[1:], val)}

        is_list = isinstance(row, list)
        dicts = []
        for row_key, dict_key in key_map.items():
            if is_list:
                inner_values = [r[row_key] for r in row if r.get(row_key) is not None]
                if any(isinstance(inner, list) for inner in inner_values):
                    # lists are unhashable
                    value = inner_values
                else:
                    value = list(set(inner_values))
                    if len(value) == 0:
                        continue
                    if len(value) == 1:
                        value = value[0]
            else:
                if row_key not in row:
                    continue
                value = row[row_key]

            dicts.append(prepare_dict_from_keys(dict_key.split('.'), value))

        return reduce(GenericMetadataParser.merge_dicts, dicts)

    def get_sample_meta(self, sample_id: str, row: GroupedRow) -> Dict[str, any]:
        """Get sample-metadata from row"""
        return self.collapse_arbitrary_meta(self.sample_meta_map, row)

    def get_sequence_meta(self, sample_id: str, row: GroupedRow) -> Dict[str, any]:
        """Get sequence-metadata from row"""
        collapsed_sequence_meta = self.collapse_arbitrary_meta(
            self.sequence_meta_map, row
        )

        read_filenames = []
        gvcf_filenames = []
        if isinstance(row, list):
            for r in row:
                if self.reads_column and self.reads_column in r:
                    read_filenames.extend(r[self.reads_column].split(','))
                if self.gvcf_column and self.gvcf_column in r:
                    gvcf_filenames.extend(r[self.gvcf_column].split(','))

        else:
            if self.reads_column and self.reads_column in row:
                read_filenames.extend(row[self.reads_column].split(','))
            if self.gvcf_column and self.gvcf_column in row:
                gvcf_filenames.extend(row[self.gvcf_column].split(','))

        # strip in case collaborator put "file1, file2"
        if read_filenames:
            full_filenames = [self.file_path(f.strip()) for f in read_filenames]
            reads, reads_type = self.parse_file(full_filenames)

            collapsed_sequence_meta['reads'] = reads
            collapsed_sequence_meta['reads_type'] = reads_type

        if gvcf_filenames:
            full_filenames = [self.file_path(f.strip()) for f in gvcf_filenames]
            gvcfs, gvcf_types = self.parse_file(full_filenames)

            collapsed_sequence_meta['gvcfs'] = gvcfs
            collapsed_sequence_meta['gvcf_types'] = gvcf_types

        return collapsed_sequence_meta

    def get_qc_meta(self, sample_id: str, row: GroupedRow) -> Optional[Dict[str, Any]]:
        """Get collapsed qc meta"""
        if not self.qc_meta_map:
            return None

        return self.collapse_arbitrary_meta(self.qc_meta_map, row)

    def get_sequence_status(self, sample_id: str, row: GroupedRow) -> str:
        """Get sequence status from row"""
        return 'uploaded'

    @staticmethod
    def from_manifest_path(
        manifest: str,
        sample_metadata_project: str,
        sample_name_column: str,
        sample_meta_map: Dict[str, str],
        sequence_meta_map: Dict[str, str],
        qc_meta_map: Dict[str, str],
        reads_column: Optional[str] = None,
        gvcf_column: Optional[str] = None,
        default_sequence_type='wgs',
        default_sample_type='blood',
        search_paths=None,
        confirm=False,
        delimiter=',',
    ):
        """Parse manifest from path, and return result of parsing manifest"""
        parser = GenericMetadataParser(
            search_locations=search_paths,
            sample_metadata_project=sample_metadata_project,
            sample_name_column=sample_name_column,
            sample_meta_map=sample_meta_map,
            sequence_meta_map=sequence_meta_map,
            qc_meta_map=qc_meta_map,
            reads_column=reads_column,
            gvcf_column=gvcf_column,
            default_sequence_type=default_sequence_type,
            default_sample_type=default_sample_type,
            confirm=confirm,
        )

        delimiter = GenericMetadataParser.guess_delimiter_from_filename(manifest)

        file_contents = parser.file_contents(manifest)
        return parser.parse_manifest(StringIO(file_contents), delimiter=delimiter)


@click.command(help=__DOC)
@click.option(
    '--sample-metadata-project',
    required=True,
    help='The sample-metadata project ($DATASET) to import manifest into',
)
@click.option('--sample-name-column', required=True)
@click.option(
    '--reads-column',
    help='Column where the reads information is held, comma-separated if multiple',
)
@click.option(
    '--gvcf-column',
    help='Column where the reads information is held, comma-separated if multiple',
)
@click.option(
    '--qc-meta-field-map',
    nargs=2,
    multiple=True,
    help='Two arguments per listing, eg: --qc-meta-field "name-in-manifest" "name-in-analysis.meta"',
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
    '--sequence-meta-field',
    multiple=True,
    help='Single argument, key to pull out of row to put in sample.meta',
)
@click.option(
    '--sequence-meta-field-map',
    nargs=2,
    multiple=True,
    help='Two arguments per listing, eg: --sequence-meta-field "name-in-manifest" "name-in-sequence.meta"',
)
@click.option('--default-sample-type', default='blood')
@click.option('--default-sequence-type', default='wgs')
@click.option(
    '--confirm', is_flag=True, help='Confirm with user input before updating server'
)
@click.option('--search-path', multiple=True, required=True)
@click.argument('manifests', nargs=-1)
def main(
    manifests,
    search_path: List[str],
    sample_metadata_project,
    sample_name_column: str,
    sample_meta_field: List[str],
    sample_meta_field_map: List[Tuple[str, str]],
    sequence_meta_field: List[str],
    sequence_meta_field_map: List[Tuple[str, str]],
    qc_meta_field_map: List[Tuple[str, str]] = None,
    reads_column: Optional[str] = None,
    gvcf_column: Optional[str] = None,
    default_sample_type='blood',
    default_sequence_type='wgs',
    confirm=False,
):
    """Run script from CLI arguments"""
    if not manifests:
        raise ValueError('Expected at least 1 manifest')

    extra_seach_paths = [m for m in manifests if m.startswith('gs://')]
    if extra_seach_paths:
        search_path = list(set(search_path).union(set(extra_seach_paths)))

    sample_meta_map, sequence_meta_map = {}, {}
    qc_meta_map = dict(qc_meta_field_map)
    if sample_meta_field_map:
        sample_meta_map.update(dict(sample_meta_field_map))
    if sample_meta_field:
        sample_meta_map.update({k: k for k in sample_meta_field})
    if sequence_meta_field_map:
        sequence_meta_map.update(dict(sequence_meta_field_map))
    if sequence_meta_field:
        sequence_meta_map.update({k: k for k in sequence_meta_field})

    for manifest in manifests:
        logger.info(f'Importing {manifest}')

        GenericMetadataParser.from_manifest_path(
            manifest=manifest,
            sample_metadata_project=sample_metadata_project,
            sample_name_column=sample_name_column,
            sample_meta_map=sample_meta_map,
            sequence_meta_map=sequence_meta_map,
            qc_meta_map=qc_meta_map,
            reads_column=reads_column,
            gvcf_column=gvcf_column,
            default_sample_type=default_sample_type,
            default_sequence_type=default_sequence_type,
            confirm=confirm,
            search_paths=search_path,
        )


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
