from typing import List, Dict, Union, Optional

import os
import re
import csv
import logging
from io import StringIO
from itertools import groupby
from collections import defaultdict

import click

from sample_metadata.apis import SampleApi, SequenceApi
from sample_metadata.model.sequence_type import SequenceType
from sample_metadata.model.sample_type import SampleType
from sample_metadata.model.sequence_status import SequenceStatus
from sample_metadata.model.new_sample import NewSample
from sample_metadata.model.new_sequence import NewSequence
from sample_metadata.model.sample_update_model import SampleUpdateModel
from sample_metadata.model.sequence_update_model import SequenceUpdateModel


rmatch = re.compile(r'_[Rr]\d')

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

FASTQ_EXTENSIONS = ('.fq', '.fastq', '.fq.gz', '.fastq.gz')
BAM_EXTENSIONS = '.bam'
CRAM_EXTENSIONS = '.cram'


class Columns:
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
    def sequence_columns():
        return [
            Columns.LIBRARY_ID,
            Columns.LIBRARY_STRATEGY,
            Columns.LIBRARY_SOURCE,
            Columns.LIBRARY_SELECTION,
            Columns.LIBRARY_LAYOUT,
            Columns.PLATFORM,
            Columns.INSTRUMENT_MODEL,
            Columns.DESIGN_DESCRIPTION,
            Columns.FORWARD_READ_LENGTH,
            Columns.REVERSE_READ_LENGTH,
        ]

    @staticmethod
    def sample_columns():
        return [
            Columns.PHS_ACCESSION,
        ]


class VcgsManifestParser:
    def __init__(
        self, path_prefix: str, sample_metadata_project: str, project: str, default_sequencing_type='wgs', default_sample_type='blood'
    ):
        super().__init__()

        self.path_prefix = path_prefix

        self.project = project
        self.sample_metadata_project = sample_metadata_project

        self.default_sequencing_type = default_sequencing_type
        self.default_sample_type = default_sample_type

        # gs specific
        self.bucket = None

        self.client = None
        self.bucket_client = None

        if path_prefix.startswith('gs://'):
            from google.cloud import storage

            self.client = storage.Client()
            path_components = path_prefix[5:].split('/')
            self.bucket = path_components[0]
            self.bucket_client = self.client.get_bucket(self.bucket)
            self.path_prefix = '/'.join(path_components[1:])

    @staticmethod
    def from_manifest_path(manifest_path: str, sample_metadata_project):
        path_prefix = os.path.dirname(manifest_path)
        manifest_filename = os.path.basename(manifest_path)
        parser = VcgsManifestParser(path_prefix, sample_metadata_project)
        file_contents = parser.file_contents(manifest_filename)
        resp = parser.parse_vcgs_manifest(StringIO(file_contents))

        return resp

    def file_path(self, filename):
        if self.client:
            return os.path.join('gs://', self.bucket, self.path_prefix, filename)
        return os.path.join(self.path_prefix, filename)

    def file_contents(self, filename):
        path = os.path.join(self.path_prefix, filename)
        if self.client:
            blob = self.bucket_client.get_blob(path)
            return blob.download_as_string().decode()
        else:
            with open(path) as f:
                return f.read()

    def file_exists(self, filename):
        path = os.path.join(self.path_prefix, filename)
        if self.client:
            blob = self.bucket_client.get_blob(path)
            return blob.exists()
        else:
            return os.path.exists(path)

    def file_size(self, filename):
        path = os.path.join(self.path_prefix, filename)
        if self.client:
            blob = self.bucket_client.get_blob(path)
            return blob.size
        else:
            return os.path.getsize(path)

    def parse_vcgs_manifest(self, file_pointer):
        # a sample has many rows
        sample_map = defaultdict(list)

        reader = csv.DictReader(file_pointer, delimiter='\t')
        for row in reader:
            sample_map[row[Columns.SAMPLE_NAME]].append(row)

        # now we can start adding!!
        sapi = SampleApi()
        seqapi = SequenceApi()

        # determine if any samples exist
        external_id_map = sapi.get_sample_id_map_by_external(
            'dev', {'external_ids': list(sample_map.keys())}, allow_missing=True
        )

        samples_to_add: List[NewSample] = []
        # by external_sample_id
        sequencing_to_add: Dict[str, List[NewSequence]] = defaultdict(list)
        
        samples_to_update: Dict[str, SampleUpdateModel] = {}
        sequences_to_update = List[Dict] = []

        for sample_name in sample_map:
            logger.info(f'Preparing {sample_name}')
            rows = sample_map[sample_name]
            reads, reads_type = self.parse_reads([r[Columns.FILENAME] for r in rows])
            # now we have sample / sequencing meta across 4 different rows, so collapse them
            collapsed_sequencing_meta = {
                col: ",".join(set(r[col] for r in rows))
                for col in Columns.sequence_columns()
            }
            collapsed_type = self.parse_sequencing_type(
                sample_name, [r[Columns.LIBRARY_STRATEGY] for r in rows]
            )
            collapsed_sample_meta = {
                col: ",".join(set(r[col] for r in rows))
                for col in Columns.sample_columns()
            }
            collapsed_sample_meta['reads'] = reads
            collapsed_sample_meta['reads_type'] = reads_type
            collapsed_sample_meta['project'] = self.project

            if sample_name in external_id_map:
                # it already exists
                cpgid = external_id_map[sample_name]
                samples_to_update[cpgid] = (SampleUpdateModel(
                    meta=collapsed_sample_meta,
                ))
                sequences_to_update.append(SequenceUpdateModel(
                    sample_id=cpgid,
                    meta=collapsed_sequencing_meta
                ))
            else:
                samples_to_add.append(
                    NewSample(
                        external_id=sample_name,
                        type=self.default_sample_type,
                        meta=collapsed_sample_meta,
                    )
                )
                sequencing_to_add[sample_name].append(
                    NewSequence(
                        sample_id='<None>',  # keep the type initialise happy
                        meta=collapsed_sequencing_meta,
                        type=SequenceType(collapsed_type),
                        status=SequenceStatus('uploaded'),
                    )
                )

        if not self.sample_metadata_project:
            logger.info('No sample-metadata project set, so skipping add into SM-DB')
            return samples_to_add, sequencing_to_add

        logger.info(f'Adding {len(samples_to_add)} samples to SM-DB database')
        external_sample_id_to_internal_id = {}
        for new_sample in samples_to_add:
            sample_id = sapi.create_new_sample(
                project=self.sample_metadata_project, new_sample=new_sample
            )
            external_sample_id_to_internal_id[new_sample.external_id] = sample_id

        for sample_id, sequences_to_add in sequencing_to_add.items():
            for seq in sequences_to_add:
                seq.sample_id = external_sample_id_to_internal_id[sample_id]
                seqapi.create_new_sequence(
                    project=self.sample_metadata_project, new_sequence=seq
                )

        for internal_sample_id, sample_update in samples_to_update.items():
            sapi.update_sample()

        return external_sample_id_to_internal_id

    def parse_sequencing_type(self, sample_id: str, types: List[str]):
        # filter false-y values
        types = list(set(t for t in types if t))
        if len(types) <= 0:
            if self.default_sequencing_type is None or self.default_sequencing_type.lower() == 'none':
                raise ValueError(
                    f"Couldn't detect sequence type for sample {sample_id}, and "
                    "no default was available."
                )
            return self.default_sequencing_type
        if len(types) > 1:
            raise ValueError(
                f'Multiple library types for same sample {sample_id}, '
                f'maybe there are multiples types of sequencing in the same '
                f'manifest? If so, please raise an issue with mfranklin to '
                f'change the groupby to include {Columns.LIBRARY_STRATEGY}.'
            )

        type_ = types[0].lower()
        if type_ == 'wgs':
            return 'wgs'
        if type_ in ('single-cell', 'ss'):
            return 'single-cell'

        raise ValueError(f'Unrecognised sequencing type {type_}')

    def parse_reads(
        self, reads: List[str]
    ) -> Tuple[Union[List[List[Dict]], List[Dict]], str]:
        """
        Returns a CWL file object
        """
        if all(r.lower() in FASTQ_EXTENSIONS for r in reads):
            structured_fastqs = self.parse_fastqs_structure(reads)
            files = []
            for fastq_group in structured_fastqs:
                files.append([self.create_file_object(f) for f in fastq_group])

            return files, 'fastq'

        elif all(r.lower() in CRAM_EXTENSIONS for r in reads):
            sec_format = ['.crai', '^.crai']
            files = []
            for r in reads:
                secondaries = self.create_secondary_file_objects_by_potential_pattern(
                    r, sec_format
                )
                files.append(self.create_file_object(r, secondary_files=secondaries))

            return files, 'cram'

        elif all(r.lower() in BAM_EXTENSIONS for r in reads):
            sec_format = ['.bai', '^.bai']
            files = []
            for r in reads:
                secondaries = self.create_secondary_file_objects_by_potential_pattern(
                    r, sec_format
                )
                files.append(self.create_file_object(r, secondary_files=secondaries))

            return files, 'bam'

        extensions = set(os.path.splitext(r)[-1] for r in reads)
        joined_reads = ''.join(f'\n\t{i}: {r}' for i, r in enumerate(reads))
        raise ValueError(
            f'Mixed, or unrecognised extensions ({", ".join(extensions)}) for reads: {joined_reads}'
        )

    def parse_fastqs_structure(self, fastqs): List[List[Dict]]:
        # find last instance of R\d, and then group by prefix on that
        sorted_fastqs = sorted(fastqs)
        r_matches = {r: rmatch.search(r) for r in sorted_fastqs}
        no_r_match = [r for r, matched in r_matches.items() if matched is None]
        if no_r_match:
            no_r_match_str = ', '.join(no_r_match)
            raise ValueError(
                f"Couldn't detect the format of FASTQs (expected match for regex '{rmatch.pattern}'): {no_r_match_str}"
            )

        values = []
        for _, grouped in groupby(sorted_fastqs, lambda r: r[: r_matches[r].start()]):
            values.append(sorted(grouped))

        return values

    def create_file_object(
        self,
        filename,
        secondary_files: List[Dict[str, any]] = None,
    ):
        checksum = None
        md5_filename = filename + '.md5'
        if self.file_exists(md5_filename):
            checksum = f'md5:{self.file_contents(md5_filename)}'

        d = {
            "location": self.file_path(filename),
            "basename": os.path.basename(filename),
            "class": "File",
            "checksum": checksum,
            "size": self.file_size(filename),
        }

        if secondary_files:
            d['secondaryFiles'] = secondary_files

        return d

    def create_secondary_file_objects_by_potential_pattern(
        self, filename, potential_secondary_patterns: List[str]
    ):
        secondaries = []
        for sec in potential_secondary_patterns:
            sec_file = apply_secondary_file_format_to_filename(filename, sec)
            if self.file_exists(sec_file):
                secondaries.append(self.create_file_object(sec_file))

        return secondaries


def apply_secondary_file_format_to_filename(
    filepath: Optional[str], secondary_file: str
):
    """
    You can trust this function to do what you want
    :param filepath: Filename to base
    :param secondary_file: CWL secondary format (Remove 1 extension for each leading ^).
    """
    if not filepath:
        return None

    fixed_sec = secondary_file.lstrip("^")
    leading = len(secondary_file) - len(fixed_sec)
    if leading <= 0:
        return filepath + fixed_sec

    basepath = ""
    filename = filepath
    if "/" in filename:
        idx = len(filepath) - filepath[::-1].index("/")
        basepath = filepath[:idx]
        filename = filepath[idx:]

    split = filename.split(".")

    newfname = filename + fixed_sec
    if len(split) > 1:
        newfname = ".".join(split[: -min(leading, len(split) - 1)]) + fixed_sec
    return basepath + newfname


def run_test():
    fastqs = [
        '20210727_PROJECT1_L002_R2.fastq.gz',
        '20210727_PROJECT1_L002_R1.fastq.gz',
        '20210727_PROJECT1_L001_R2.fastq.gz',
        '20210727_PROJECT1_L001_R1.fastq.gz',
    ]
    parser = VcgsManifestParser(
        'gs://bucket/subdir',
        sample_metadata_project=None,
    )
    print(parser.parse_reads(fastqs))
    

@click
def main(manifest, project, sample_metadata_project, default_sample_type='blood', default_sequence_type='wgs'):


if __name__ == "__main__":
    man_path = 'gs://cpg-seqr-upload-<collaborator>/<subdir>/manifest.txt'
    parser = VcgsManifestParser.from_manifest_path(
        man_path, sample_metadata_project='dev'
    )
