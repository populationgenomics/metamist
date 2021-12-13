# pylint: disable=too-many-instance-attributes,too-many-locals,unused-argument,no-self-use,assignment-from-none
import asyncio
import csv
import logging
import os
import re
import traceback
from abc import abstractmethod
from collections import defaultdict
from itertools import groupby
from typing import (
    List,
    Dict,
    Union,
    Optional,
    Tuple,
    Match,
    Any,
    Sequence,
    TypeVar,
    Iterator,
    Coroutine,
)

from google.api_core.exceptions import Forbidden

from sample_metadata import ApiException
from sample_metadata.model_utils import async_wrap
from sample_metadata.apis import SampleApi, SequenceApi, AnalysisApi
from sample_metadata.models import (
    NewSample,
    NewSequence,
    SequenceType,
    SequenceUpdateModel,
    SampleUpdateModel,
    AnalysisModel,
    SampleType,
    AnalysisType,
    SequenceStatus,
    AnalysisStatus,
)

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

FASTQ_EXTENSIONS = ('.fq', '.fastq', '.fq.gz', '.fastq.gz')
BAM_EXTENSIONS = ('.bam',)
CRAM_EXTENSIONS = ('.cram',)
VCFGZ_EXTENSIONS = ('.vcf.gz',)

rmatch = re.compile(r'[_\.-][Rr]\d')
GroupedRow = Union[List[Dict[str, Any]], Dict[str, Any]]

T = TypeVar('T')


def chunk(iterable: Sequence[T], chunk_size=500) -> Iterator[List[T]]:
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i : i + chunk_size]


class GenericParser:  # pylint: disable=too-many-public-methods
    """Parser for VCGS manifest"""

    def __init__(
        self,
        path_prefix: Optional[str],
        sample_metadata_project: str,
        default_sequence_type='wgs',
        default_sequence_status='uploaded',
        default_sample_type='blood',
        skip_checking_gcs_objects=False,
        verbose=True,
    ):

        self.path_prefix = path_prefix
        self.skip_checking_gcs_objects = skip_checking_gcs_objects
        self.verbose = verbose

        if not sample_metadata_project:
            raise ValueError('sample-metadata project is required')

        self.sample_metadata_project = sample_metadata_project

        self.default_sequence_type: str = default_sequence_type
        self.default_sequence_status: str = default_sequence_status
        self.default_sample_type: str = default_sample_type

        # gs specific
        self.default_bucket = None

        self.client = None
        self.bucket_clients: Dict[str, Any] = {}

        if path_prefix and path_prefix.startswith('gs://'):
            # pylint: disable=import-outside-toplevel
            from google.cloud import storage

            self.client = storage.Client()
            path_components = path_prefix[5:].split('/')
            self.default_bucket = path_components[0]
            self.path_prefix = '/'.join(path_components[1:])

    def get_bucket(self, bucket_name=None):
        """Get cached bucket client from optional bucket name"""
        if bucket_name is None:
            bucket_name = self.default_bucket
        if bucket_name not in self.bucket_clients:
            self.bucket_clients[bucket_name] = self.client.get_bucket(bucket_name)

        return self.bucket_clients[bucket_name]

    def file_path(self, filename: str) -> str:
        """
        Get complete filepath of filename:
        - Includes gs://{bucket} if relevant
        - Includes path_prefix decided early on
        """
        if filename.startswith('gs://'):
            return filename

        if self.client and not filename.startswith('/'):
            return os.path.join(
                'gs://',
                self.default_bucket or '',
                self.path_prefix or '',
                filename or '',
            )

        return os.path.join(self.path_prefix or '', filename)

    @async_wrap
    def get_blob(self, filename):
        """Convenience function for getting blob from fully qualified GCS path"""
        if not filename.startswith('gs://'):
            raise ValueError('No blob available')

        bucket_name, *components = filename[5:].split('/')
        bucket = self.get_bucket(bucket_name)
        path = '/'.join(components)

        # the next few lines are equiv to `bucket.get_blob(path)`
        # but without requiring storage.objects.get permission
        blobs = list(self.client.list_blobs(bucket, prefix=path))
        # first where r.name == path (or None)
        return next((r for r in blobs if r.name == path), None)

    def list_directory(self, directory_name) -> List[str]:
        """List directory"""
        path = self.file_path(directory_name)
        if path.startswith('gs://'):
            bucket_name, *components = directory_name[5:].split('/')
            assert self.client
            blobs = self.client.list_blobs(bucket_name, prefix='/'.join(components))
            return [f'gs://{bucket_name}/{blob.name}' for blob in blobs]

        return [os.path.join(path, f) for f in os.listdir(path)]

    async def file_contents(self, filename) -> Optional[str]:
        """Get contents of file (decoded as utf8)"""
        path = self.file_path(filename)
        if path.startswith('gs://'):
            blob = await self.get_blob(path)
            try:
                retval = blob.download_as_string()
                if isinstance(retval, bytes):
                    retval = retval.decode()
                return retval
            except Forbidden:
                logger.warning(f"FORBIDDEN: Can't download {filename}")
                return None

        with open(filename, encoding='utf-8') as f:
            return f.read()

    async def file_exists(self, filename: str) -> bool:
        """Determines whether a file exists"""
        path = self.file_path(filename)

        if path.startswith('gs://'):
            blob = await self.get_blob(filename)
            return blob is not None

        return os.path.exists(path)

    async def file_size(self, filename):
        """Get size of file in bytes"""
        path = self.file_path(filename)
        if path.startswith('gs://'):
            blob = await self.get_blob(filename)
            return blob.size

        return os.path.getsize(path)

    @abstractmethod
    def get_sample_id(self, row: Dict[str, Any]) -> str:
        """Get external sample ID from row"""

    @abstractmethod
    async def get_sample_meta(self, sample_id: str, row: GroupedRow) -> Dict[str, Any]:
        """Get sample-metadata from row"""

    @abstractmethod
    async def get_sequence_meta(
        self, sample_id: str, row: GroupedRow
    ) -> Dict[str, Any]:
        """Get sequence-metadata from row"""

    async def get_analyses(
        self, sample_id: str, row: GroupedRow, cpg_id: Optional[str]
    ) -> List[AnalysisModel]:
        """
        Get analysis objects from row. Optionally, a CPG ID can be passed for
        to help finding files for previously added samples that were already renamed.
        """
        return []

    async def get_qc_meta(
        self, sample_id: str, row: GroupedRow
    ) -> Optional[Dict[str, Any]]:
        """Get qc-meta from row, creates a Analysis object of type QC"""
        return None

    def get_sample_type(
        self, sample_id: str, row: GroupedRow
    ) -> Union[str, SampleType]:
        """Get sample type from row"""
        return self.default_sample_type

    def get_sequence_type(
        self, sample_id: str, row: GroupedRow
    ) -> Union[str, SequenceType]:
        """Get sequence type from row"""
        return self.default_sequence_type

    def get_sequence_status(
        self, sample_id: str, row: GroupedRow
    ) -> Union[str, SequenceStatus]:
        """Get sequence status from row"""
        return self.default_sequence_status

    async def process_group(
        self,
        rows: GroupedRow,
        external_sample_id: str,
        cpg_sample_id: str,
        sequence_id: str,
    ):

        if isinstance(rows, list) and len(rows) == 1:
            rows = rows[0]

        # now we have sample / sequencing meta across 4 different rows, so collapse them
        promises = [
            self.get_sequence_meta(external_sample_id, rows),
            self.get_sample_meta(external_sample_id, rows),
            self.get_qc_meta(external_sample_id, rows),
            self.get_analyses(external_sample_id, rows, cpg_id=cpg_sample_id),
        ]
        (
            collapsed_sequencing_meta,
            collapsed_sample_meta,
            collapsed_qc,
            collapsed_analyses,
        ) = await asyncio.gather(*promises)
        # return collapsed_sequencing_meta, collapsed_sample_meta, collapsed_qc, collapsed_analyses

        (
            sample_to_add,
            sample_to_update,
            sequence_to_add,
            sequence_to_update,
            analysis_to_add,
        ) = (None, None, None, None, [])

        sample_type = self.get_sample_type(external_sample_id, rows)
        sequence_status = self.get_sequence_status(external_sample_id, rows)

        if cpg_sample_id:
            # it already exists
            sample_to_update = SampleUpdateModel(
                meta=collapsed_sample_meta,
            )
        else:
            sample_to_add = NewSample(
                external_id=external_sample_id,
                type=SampleType(sample_type),
                meta=collapsed_sample_meta,
            )

        # should we add or update sequencing
        if collapsed_sequencing_meta:
            if sequence_id:
                # update, we have a cpg sample ID AND a sequencing ID
                sequence_to_update = (
                    sequence_id,
                    SequenceUpdateModel(
                        meta=collapsed_sequencing_meta,
                        status=SequenceStatus(sequence_status),
                    ),
                )
            else:
                # the internal sample ID gets mapped back later
                sequence_to_add = NewSequence(
                    sample_id='<None>',  # keep the type initialisation happy
                    meta=collapsed_sequencing_meta,
                    type=SequenceType('wgs'),
                    status=SequenceStatus(sequence_status),
                )

        if collapsed_analyses:
            analysis_to_add.extend(collapsed_analyses)

        if collapsed_qc:
            analysis_to_add.append(
                AnalysisModel(
                    sample_ids=['<none>'],
                    type=AnalysisType('qc'),
                    status=AnalysisStatus('completed'),
                    meta=collapsed_qc,
                )
            )

        return (
            external_sample_id,
            sample_to_add,
            sample_to_update,
            sequence_to_add,
            sequence_to_update,
            analysis_to_add,
        )

    async def parse_manifest(  # pylint: disable=too-many-branches
        self, file_pointer, delimiter=',', confirm=False, dry_run=False
    ) -> Union[Dict[str, str], Tuple[List, Dict, Dict, Dict, Dict]]:
        """
        Parse manifest from iterable (file pointer / String.IO)

        Returns a dict mapping external sample ID to CPG sample ID
        """
        # a sample has many rows
        sample_map = defaultdict(list)

        reader = csv.DictReader(file_pointer, delimiter=delimiter)
        for row in reader:
            sample_id = self.get_sample_id(row)
            sample_map[sample_id].append(row)

        if len(sample_map) == 0:
            raise ValueError(f'The manifest file contains no records')

        # now we can start adding!!
        sapi = SampleApi()
        seqapi = SequenceApi()
        analysisapi = AnalysisApi()

        # determine if any samples exist
        existing_external_id_to_cpgid = await sapi.get_sample_id_map_by_external_async(
            self.sample_metadata_project, list(sample_map.keys()), allow_missing=True
        )
        existing_cpgid_to_seq_id = {}

        if len(existing_external_id_to_cpgid) > 0:
            existing_cpgid_to_seq_id = (
                await seqapi.get_sequence_ids_from_sample_ids_async(
                    request_body=list(existing_external_id_to_cpgid.values()),
                )
            )

        samples_to_add: List[NewSample] = []
        # by external_sample_id
        sequences_to_add: Dict[str, NewSequence] = {}
        analyses_to_add: Dict[str, List[AnalysisModel]] = defaultdict(list)

        samples_to_update: Dict[str, SampleUpdateModel] = {}
        sequences_to_update: Dict[int, SequenceUpdateModel] = {}

        # we'll batch process the samples as not to open too many threads

        for ex_sample_ids in chunk(list(sample_map.keys())):

            current_batch_promises = []

            for external_sample_id in ex_sample_ids:
                if self.verbose:
                    logger.info(f'Preparing {external_sample_id}')

                rows: Union[Dict[str, str], List[Dict[str, str]]] = sample_map[
                    external_sample_id
                ]
                cpg_sample_id = existing_external_id_to_cpgid.get(external_sample_id)
                promise = self.process_group(
                    rows=rows,
                    external_sample_id=external_sample_id,
                    cpg_sample_id=cpg_sample_id,
                    sequence_id=existing_cpgid_to_seq_id.get(cpg_sample_id),
                )
                current_batch_promises.append(promise)

            resolved_promises = await asyncio.gather(*current_batch_promises)
            for (
                external_sample_id,
                sample_to_add,
                sample_to_update,
                sequence_to_add,
                sequence_to_update,
                analysis_to_add,
            ) in resolved_promises:
                cpg_sample_id = existing_external_id_to_cpgid.get(external_sample_id)
                if sample_to_add:
                    samples_to_add.append(sample_to_add)
                if sample_to_update:
                    samples_to_update[cpg_sample_id].append(sample_to_update)
                if sequence_to_add:
                    sequences_to_add[external_sample_id] = sequence_to_add
                if sequence_to_update:
                    seq_id, update_model = sequence_to_update
                    sequence_to_update[seq_id] = update_model

                if analysis_to_add:
                    analyses_to_add[external_sample_id] = analysis_to_add

        message = f"""\
Processing samples: {', '.join(sample_map.keys())}

Adding {len(samples_to_add)} samples
Adding {len(sequences_to_add)} sequences
Adding {len(analyses_to_add)} analysis results

Updating {len(samples_to_update)} sample
Updating {len(sequences_to_update)} sequences"""

        if dry_run:
            logger.info('Dry run, so returning without inserting / updating metadata')
            return (
                samples_to_add,
                sequences_to_add,
                samples_to_update,
                sequences_to_update,
                analyses_to_add,
            )

        if confirm:
            resp = str(input(message + '\n\nConfirm (y): '))
            if resp.lower() != 'y':
                raise SystemExit()
        else:
            logger.info(message)

        logger.info(f'Adding {len(samples_to_add)} samples to SM-DB database')
        added_ext_sample_to_internal_id = {}

        for chunked_samples in chunk(samples_to_add):
            ordered_external_sample_ids = [s.external_id for s in chunked_samples]
            sample_id_promises = []
            for new_sample in chunked_samples:
                sample_id_promises.append(
                    sapi.create_new_sample_async(
                        project=self.sample_metadata_project, new_sample=new_sample
                    )
                )

            for external_id, sample_id in zip(
                ordered_external_sample_ids, await asyncio.gather(*sample_id_promises)
            ):
                added_ext_sample_to_internal_id[external_id] = sample_id
                existing_external_id_to_cpgid[external_id] = sample_id

        logger.info(f'Adding {len(sequences_to_add)} sequence entries')
        for chunked_sequences in chunk(list(sequences_to_add.items())):
            promises = []
            for sample_id, seq in chunked_sequences:
                seq.sample_id = existing_external_id_to_cpgid[sample_id]
                promises.append(seqapi.create_new_sequence_async(new_sequence=seq))
            # wait for them to finish
            await asyncio.gather(*promises)

        logger.info(f'Adding analysis entries for {len(analyses_to_add)} samples')
        unwrapped_analysis_to_add = [
            (sample_id, a)
            for (sample_id, analyses) in analyses_to_add.items()
            for a in analyses
        ]
        for chunked_analysis in chunk(unwrapped_analysis_to_add):
            promises = []
            for sample_id, analysis in chunked_analysis:
                analysis.sample_ids = [existing_external_id_to_cpgid[sample_id]]
                promises.append(
                    analysisapi.create_new_analysis_async(
                        project=self.sample_metadata_project, analysis_model=analysis
                    )
                )
            await asyncio.gather(*promises)

        logger.info(f'Updating {len(samples_to_update)} samples')
        for chunked_samples in chunk(list(samples_to_update.items())):
            promises = []
            for internal_sample_id, sample_update in chunked_samples:
                promises.append(
                    sapi.update_sample_async(
                        id_=internal_sample_id,
                        sample_update_model=sample_update,
                    )
                )
            await asyncio.gather(*promises)

        logger.info(f'Updating {len(sequences_to_update)} sequences')
        for chunked_sequences in chunk(list(enumerate(sequences_to_update.items()))):
            promises = []
            for i, (seq_id, seq_update) in chunked_sequences:
                promises.append(
                    seqapi.update_sequence_async(
                        sequence_id=seq_id,
                        sequence_update_model=seq_update,
                    )
                )
            await asyncio.gather(*promises)

        return added_ext_sample_to_internal_id

    async def parse_file(
        self, reads: List[str]
    ) -> Tuple[Union[List[List[Dict]], List[Dict]], str]:
        """
        Returns a tuple of:
        1. single / list-of CWL file object(s), based on the extensions of the reads
        2. parsed type (fastq, cram, bam)
        """

        if all(any(r.lower().endswith(ext) for ext in FASTQ_EXTENSIONS) for r in reads):
            structured_fastqs = self.parse_fastqs_structure(reads)
            fastq_files: List[List[Dict]] = []
            for fastq_group in structured_fastqs:
                fastq_files.append(
                    asyncio.gather(self.create_file_object(f) for f in fastq_group)
                )

            return list(await asyncio.gather(*fastq_files)), 'fastq'

        files: List[Coroutine[Dict]] = []

        if all(any(r.lower().endswith(ext) for ext in CRAM_EXTENSIONS) for r in reads):
            sec_format = ['.crai', '^.crai']
            for r in reads:
                secondaries = await self.create_secondary_file_objects_by_potential_pattern(
                    r, sec_format
                )
                files.append(self.create_file_object(r, secondary_files=secondaries))

            return await asyncio.gather(*files), 'cram'

        if all(any(r.lower().endswith(ext) for ext in BAM_EXTENSIONS) for r in reads):
            sec_format = ['.bai', '^.bai']
            for r in reads:
                secondaries = await self.create_secondary_file_objects_by_potential_pattern(
                    r, sec_format
                )
                files.append(self.create_file_object(r, secondary_files=secondaries))

            return await asyncio.gather(*files), 'bam'

        if all(any(r.lower().endswith(ext) for ext in VCFGZ_EXTENSIONS) for r in reads):
            sec_format = ['.tbi']
            for r in reads:
                secondaries = await self.create_secondary_file_objects_by_potential_pattern(
                    r, sec_format
                )
                files.append(self.create_file_object(r, secondary_files=secondaries))

            return await asyncio.gather(*files), 'gvcf'

        extensions = set(os.path.splitext(r)[-1] for r in reads)
        joined_reads = ''.join(f'\n\t{i}: {r}' for i, r in enumerate(reads))
        raise ValueError(
            f'Mixed, or unrecognised extensions ({", ".join(extensions)}) for reads: {joined_reads}'
        )

    @staticmethod
    def parse_fastqs_structure(fastqs) -> List[List[str]]:
        """
        Takes a list of fastqs, and a set of nested lists of each R1 + R2 read.

        >>> GenericParser.parse_fastqs_structure(['/seqr_transfers/Z01_1234_HNXXXXX_TCATCCTT-AGCGAGCT_L004_R1.fastq.gz', '/seqr_transfers/Z01_1234_HNXXXXX_TCATCCTT-AGCGAGCT_L004_R2.fastq.gz'])
        [['/seqr_transfers/Z01_1234_HNXXXXX_TCATCCTT-AGCGAGCT_L004_R1.fastq.gz', '/seqr_transfers/Z01_1234_HNXXXXX_TCATCCTT-AGCGAGCT_L004_R2.fastq.gz']]

        >>> GenericParser.parse_fastqs_structure(['20210727_PROJECT1_L002_R2.fastq.gz', '20210727_PROJECT1_L002_R1.fastq.gz', '20210727_PROJECT1_L001_R2.fastq.gz', '20210727_PROJECT1_L001_R1.fastq.gz'])
        [['20210727_PROJECT1_L001_R1.fastq.gz', '20210727_PROJECT1_L001_R2.fastq.gz'], ['20210727_PROJECT1_L002_R1.fastq.gz', '20210727_PROJECT1_L002_R2.fastq.gz']]

        >>> GenericParser.parse_fastqs_structure(['/directory1/Z01_1234_HNXXXXX_TCATCCTT-AGCGAGCT_L001_R1.fastq.gz', '/directory2/Z01_1234_HNXXXXX_TCATCCTT-AGCGAGCT_L001_R2.fastq.gz'])
        [['/directory1/Z01_1234_HNXXXXX_TCATCCTT-AGCGAGCT_L001_R1.fastq.gz', '/directory2/Z01_1234_HNXXXXX_TCATCCTT-AGCGAGCT_L001_R2.fastq.gz']]

        """
        # find last instance of R\d, and then group by prefix on that
        sorted_fastqs = sorted(fastqs)

        r_matches: Dict[str, Tuple[str, Optional[Match[str]]]] = {
            r: (os.path.basename(r), rmatch.search(os.path.basename(r)))
            for r in sorted_fastqs
        }
        no_r_match = [r for r, (_, matched) in r_matches.items() if matched is None]
        if no_r_match:
            no_r_match_str = ', '.join(no_r_match)
            raise ValueError(
                f"Couldn't detect the format of FASTQs (expected match for regex '{rmatch.pattern}'): {no_r_match_str}"
            )

        values = []
        for _, grouped in groupby(
            sorted_fastqs, lambda r: r_matches[r][0][: r_matches[r][1].start()]  # type: ignore
        ):
            values.append(sorted(grouped))

        return sorted(values, key=lambda el: el[0])

    async def create_file_object(
        self,
        filename: str,
        secondary_files: List[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Takes filename, returns formed CWL dictionary"""
        checksum = None
        file_size = None

        if not self.skip_checking_gcs_objects:
            md5_filename = self.file_path(filename + '.md5')
            if await self.file_exists(md5_filename):
                contents = await self.file_contents(md5_filename)
                if contents:
                    checksum = f'md5:{contents.strip()}'

            file_size = await self.file_size(filename)

        d = {
            'location': self.file_path(filename),
            'basename': os.path.basename(filename),
            'class': 'File',
            'checksum': checksum,
            'size': file_size,
        }

        if secondary_files:
            d['secondaryFiles'] = secondary_files

        return d

    async def create_secondary_file_objects_by_potential_pattern(
        self, filename, potential_secondary_patterns: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Take a base filename and potential secondary patterns:
        - Try each secondary pattern, see if it works
        - If it works, create a CWL file object
        - return a list of those secondary file objects that exist
        """
        secondaries = []
        for sec in potential_secondary_patterns:
            sec_file = _apply_secondary_file_format_to_filename(filename, sec)
            if self.skip_checking_gcs_objects or await self.file_exists(sec_file):
                secondaries.append(self.create_file_object(sec_file))

        return await asyncio.gather(*secondaries)

    @staticmethod
    def guess_delimiter_from_filename(filename: str):
        """
        Guess delimiter from filename
        """
        extension_to_delimiter = {'.csv': ',', '.tsv': '\t'}
        relevant_delimiter = next(
            (
                delimiter
                for ext, delimiter in extension_to_delimiter.items()
                if filename.endswith(ext)
            ),
            None,
        )
        if relevant_delimiter:
            return relevant_delimiter

        raise ValueError(f'Unrecognised extension on file: {filename}')


def _apply_secondary_file_format_to_filename(
    filepath: Optional[str], secondary_file: str
):
    """
    You can trust this function to do what you want
    :param filepath: Filename to base
    :param secondary_file: CWL secondary format (Remove 1 extension for each leading ^).
    """
    if not filepath:
        return None

    fixed_sec = secondary_file.lstrip('^')
    leading = len(secondary_file) - len(fixed_sec)
    if leading <= 0:
        return filepath + fixed_sec

    basepath = ''
    filename = filepath
    if '/' in filename:
        idx = len(filepath) - filepath[::-1].index('/')
        basepath = filepath[:idx]
        filename = filepath[idx:]

    split = filename.split('.')

    newfname = filename + fixed_sec
    if len(split) > 1:
        newfname = '.'.join(split[: -min(leading, len(split) - 1)]) + fixed_sec
    return basepath + newfname
