# pylint: disable=too-many-lines,too-many-instance-attributes,too-many-locals,unused-argument,no-self-use,assignment-from-none,invalid-name,ungrouped-imports
import sys
import asyncio
import csv
import logging
import os
import re
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
from functools import wraps

from google.api_core.exceptions import Forbidden
from google.cloud import storage
from sample_metadata.model.participant_upsert_body import ParticipantUpsertBody

from sample_metadata.model_utils import async_wrap
from sample_metadata.apis import SampleApi, SequenceApi, AnalysisApi, ParticipantApi
from sample_metadata.models import (
    ParticipantUpsert,
    SequenceType,
    AnalysisModel,
    SampleType,
    AnalysisType,
    SequenceStatus,
    AnalysisStatus,
    SampleBatchUpsert,
    SampleBatchUpsertBody,
    SequenceUpsert,
)


# https://mypy.readthedocs.io/en/stable/runtime_troubles.html#using-new-additions-to-the-typing-module
if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

FASTQ_EXTENSIONS = ('.fq', '.fastq', '.fq.gz', '.fastq.gz')
BAM_EXTENSIONS = ('.bam',)
CRAM_EXTENSIONS = ('.cram',)
GVCF_EXTENSIONS = ('.g.vcf.gz',)
VCF_EXTENSIONS = ('.vcf', '.vcf.gz')

ALL_EXTENSIONS = (
    FASTQ_EXTENSIONS
    + CRAM_EXTENSIONS
    + BAM_EXTENSIONS
    + GVCF_EXTENSIONS
    + VCF_EXTENSIONS
)

rmatch = re.compile(r'[_\.-][Rr]\d')
SingleRow = Dict[str, Any]
GroupedRow = List[SingleRow]

T = TypeVar('T')

SUPPORTED_FILE_TYPE = Literal['reads', 'variants']
SUPPORTED_READ_TYPES = Literal['fastq', 'bam', 'cram']
SUPPORTED_VARIANT_TYPES = Literal['gvcf', 'vcf']


class ParticipantMetaGroup:
    """Class for holding participant metadata grouped by id"""

    def __init__(
        self,
        participant_id: int,
        rows: GroupedRow,
        meta: Dict[str, Any],
    ):
        self.id = participant_id
        self.rows = rows
        self.meta = meta


class SampleMetaGroup:
    """Class for holding sample metadata grouped by id"""

    def __init__(
        self,
        sample_id: str,
        rows: GroupedRow,
        meta: Dict[str, Any] = None,
    ):
        self.id = sample_id
        self.rows = rows
        self.meta = meta


class SequenceMetaGroup:
    """Class for holding sequence metadata grouped by type"""

    def __init__(
        self,
        rows: GroupedRow,
        sequence_type: SequenceType,
        meta: Optional[Dict[str, Any]] = None,
    ):
        self.rows = rows
        self.sequence_type = sequence_type
        self.meta = meta


def chunk(iterable: Sequence[T], chunk_size=500) -> Iterator[Sequence[T]]:
    """
    Chunk a sequence by yielding lists of `chunk_size`
    """
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i : i + chunk_size]


def run_as_sync(f):
    """
    Run an async function, synchronously.
    Useful for @click functions that must be async, eg:

    @click.command()
    @click.option(...)
    @run_as_sync
    async def my_async_function(**kwargs):
        return await awaitable_function(**kwargs)
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(f(*args, **kwargs))

    return wrapper


class GenericParser:  # pylint: disable=too-many-public-methods
    """Parser for VCGS manifest"""

    def __init__(
        self,
        path_prefix: Optional[str],
        sample_metadata_project: str,
        default_sequence_type='genome',
        default_sequence_status='uploaded',
        default_sample_type='blood',
        default_analysis_type='qc',
        default_analysis_status='completed',
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
        self.default_analysis_type: str = default_analysis_type
        self.default_analysis_status: str = default_analysis_status

        # gs specific
        self.default_bucket = None

        self._client = None
        self.bucket_clients: Dict[str, Any] = {}

        self.papi = ParticipantApi()
        self.seqapi = SequenceApi()

    @property
    def client(self):
        """Get GCP storage client"""
        if not self._client:
            self._client = storage.Client()
        return self._client

    def get_bucket(self, bucket_name):
        """Get cached bucket client from optional bucket name"""
        assert bucket_name
        if bucket_name not in self.bucket_clients:
            self.bucket_clients[bucket_name] = self.client.get_bucket(bucket_name)

        return self.bucket_clients[bucket_name]

    def file_path(self, filename: str) -> str:
        """
        Get complete filepath of filename:
        - Includes gs://{bucket} if relevant
        - Includes path_prefix decided early on
        """
        if filename.startswith('gs://') or filename.startswith('/'):
            return filename

        if not self.path_prefix:
            raise FileNotFoundError(
                f"Can't form full path to '{filename}' as no path_prefix was defined"
            )

        if self.client and not filename.startswith('/'):
            assert self.default_bucket
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
            blobs = self.client.list_blobs(
                bucket_name, prefix='/'.join(components), delimiter='/'
            )
            return [f'gs://{bucket_name}/{blob.name}' for blob in blobs]

        return [os.path.join(path, f) for f in os.listdir(path)]

    async def file_contents(self, filename) -> Optional[str]:
        """Get contents of file (decoded as utf8)"""
        path = self.file_path(filename)
        if path.startswith('gs://'):
            blob = await self.get_blob(path)
            try:
                retval = blob.download_as_bytes()
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
    def get_sample_id(self, row: SingleRow) -> Optional[str]:
        """Get external sample ID from row"""

    # @abstractmethod
    async def get_cpg_sample_id(self, row: SingleRow) -> Optional[str]:
        """Get internal cpg sample ID from row"""

    @abstractmethod
    def get_participant_id(self, row: SingleRow) -> Optional[str]:
        """Get external participant ID from row"""

    def get_reported_sex(self, row: GroupedRow) -> Optional[int]:
        """Get reported sex from grouped row"""
        return None

    def get_reported_gender(self, row: GroupedRow) -> Optional[str]:
        """Get reported gender from grouped row"""
        return None

    def get_karyotype(self, row: GroupedRow) -> Optional[str]:
        """Get karyotype from grouped row"""
        return None

    # @abstractmethod
    def has_participants(self, file_pointer, delimiter: str) -> bool:
        """Returns True if the file has a Participants column"""

    # @abstractmethod
    async def get_grouped_sample_meta(self, rows: GroupedRow) -> List[SampleMetaGroup]:
        """Return list of grouped by sample metadata from the rows"""

    @abstractmethod
    async def get_sample_meta(self, sample_group: SampleMetaGroup) -> SampleMetaGroup:
        """Get sample-metadata from row"""

    # @abstractmethod
    async def get_grouped_sequence_meta(
        self, sample_id: str, rows: GroupedRow
    ) -> List[SequenceMetaGroup]:
        """Return list of grouped by type sequence metadata from the rows"""

    @abstractmethod
    async def get_sequence_meta(
        self, seq_group: SequenceMetaGroup
    ) -> SequenceMetaGroup:
        """Get sequence-metadata from row then set it in the SequenceMetaGroup"""
        return SequenceMetaGroup(
            rows=[], sequence_type=self.default_sequence_type, meta={}
        )

    # @abstractmethod
    async def get_participant_meta(
        self, participant_id: int, rows: GroupedRow
    ) -> ParticipantMetaGroup:
        """Get participant-metadata from rows then set it in the ParticipantMetaGroup"""
        return ParticipantMetaGroup(participant_id=participant_id, rows=rows, meta={})

    # @abstractmethod
    async def get_analyses(
        self, sample_id: str, row: SingleRow, cpg_id: Optional[str]
    ) -> List[AnalysisModel]:
        """
        Get analysis objects from row. Optionally, a CPG ID can be passed for
        to help finding files for previously added samples that were already renamed.
        """
        return []

    @abstractmethod
    async def get_qc_meta(self, sample_id: str, row: SingleRow) -> Optional[SingleRow]:
        """Get qc-meta from row, creates a Analysis object of type QC"""

    # @abstractmethod
    def get_sample_type(self, row: GroupedRow) -> SampleType:
        """Get sample type from row"""
        return SampleType(self.default_sample_type)

    # @abstractmethod
    def get_sequence_types(self, row: GroupedRow) -> List[SequenceType]:
        """Get sequence types from row"""
        return List(SequenceType(self.default_sequence_type))

    # @abstractmethod
    def get_sequence_type(self, row: SingleRow) -> SequenceType:
        """Get sequence types from row"""
        return SequenceType(self.default_sequence_type)

    # @abstractmethod
    def get_sequence_status(self, row: GroupedRow) -> SequenceStatus:
        """Get sequence status from row"""
        return SequenceStatus(self.default_sequence_status)

    # @abstractmethod
    def get_analysis_type(self, sample_id: str, row: GroupedRow) -> AnalysisType:
        """Get analysis type from row"""
        return AnalysisType(self.default_analysis_type)

    # @abstractmethod
    def get_analysis_status(self, sample_id: str, row: GroupedRow) -> AnalysisStatus:
        """Get analysis status from row"""
        return AnalysisStatus(self.default_analysis_status)

    async def process_sample_group(
        self,
        rows: GroupedRow,
        external_sample_id: str,
        cpg_sample_id: Optional[str],
    ):
        """
        ASYNC function that (maps) transforms one GroupedRow, and returns a Tuple of:
            (
                sample_to_upsert,
                sequences_to_upsert,
                analyses_to_add,
            )

        Then the calling function does the (reduce).
        """

        # Get all the sequence ids for this sample
        sequence_ids = {}
        if cpg_sample_id is not None:
            sequence_ids = await self.seqapi.get_all_sequences_for_sample_id_async(
                sample_id=cpg_sample_id
            )

        # now we have sample / sequencing meta across 4 different rows, so collapse them
        (
            collapsed_sequencing_meta,
            collapsed_sample_meta,
            collapsed_qc,
            collapsed_analyses,
        ) = await asyncio.gather(
            self.get_grouped_sequence_meta(external_sample_id, rows),
            self.get_sample_meta(SampleMetaGroup(sample_id=cpg_sample_id, rows=rows)),
            self.get_qc_meta(external_sample_id, rows[0]),
            self.get_analyses(external_sample_id, rows[0], cpg_id=cpg_sample_id),
        )

        sample_to_upsert = None
        sequences_to_upsert = []
        analyses_to_add = []

        # should we add or update sequencing
        if collapsed_sequencing_meta:
            for seq in collapsed_sequencing_meta:
                sequence_id = sequence_ids.get(str(seq.sequence_type), None)
                if isinstance(sequence_id, list):
                    if len(sequence_id) > 1:
                        raise ValueError(f'Unhandled case with more than one sequence ID for the type {seq.sequence_type}')
                    sequence_id = sequence_id[0]
                args = {
                    'id': sequence_id,
                    'meta': seq.meta,
                    'type': seq.sequence_type,
                    'status': self.get_sequence_status(seq.rows),
                }
                if not args['id']:
                    del args['id']

                sequences_to_upsert.append(SequenceUpsert(**args))

        # Should we add or update sample
        args = {
            'id': cpg_sample_id,
            'meta': collapsed_sample_meta.meta,
            'external_id': external_sample_id,
            'type': self.get_sample_type(rows),
            'sequences': sequences_to_upsert,
        }
        if not cpg_sample_id:
            del args['id']

        sample_to_upsert = SampleBatchUpsert(**args)

        if collapsed_analyses:
            analyses_to_add.extend(collapsed_analyses)

        if collapsed_qc:
            analyses_to_add.append(
                AnalysisModel(
                    sample_ids=['<none>'],
                    type=self.get_analysis_type(external_sample_id, rows),
                    status=self.get_analysis_status(external_sample_id, rows),
                    meta=collapsed_qc,
                )
            )

        return (
            sample_to_upsert,
            sequences_to_upsert,
            analyses_to_add,
        )

    async def process_participant_group(
        self, participant_name: str, sample_map: Dict[str, Any]
    ):
        """
        ASYNC function that (maps) transforms one GroupedRow, and returns a Tuple of:
            (
                participants_to_upsert,
                samples_to_upsert,
                sequences_to_upsert,
                analyses_to_add,
            )

        Then the calling function does the (reduce).
        """

        all_rows = [r for row in sample_map.values() for r in row]

        # Get external sid to cpg map
        existing_external_id_to_cpgid = (
            await SampleApi().get_sample_id_map_by_external_async(
                self.sample_metadata_project,
                list(sample_map.keys()),
                allow_missing=True,
            )
        )

        # Get all the samples and sequences to upsert first
        samples_to_upsert = []
        sequences_to_upsert = []
        analyses_to_add = []

        for sample_id, rows in sample_map.items():
            cpg_id = existing_external_id_to_cpgid.get(sample_id, None)
            sample, seqs, analyses = await self.process_sample_group(
                rows, sample_id, cpg_id
            )
            samples_to_upsert.append(sample)
            sequences_to_upsert.extend(seqs)
            analyses_to_add.extend(analyses)

        # Construct participant to upsert
        existing_participant_ids = self.papi.get_participant_id_map_by_external_ids(
            self.sample_metadata_project, [participant_name], allow_missing=True
        )

        internal_id = existing_participant_ids.get(participant_name, None)

        # pull relevant participant fields
        reported_sex = self.get_reported_sex(all_rows)
        reported_gender = self.get_reported_gender(all_rows)
        karyotype = self.get_karyotype(all_rows)

        # now we have sample / sequencing meta across 4 different rows, so collapse them
        collapsed_participant_meta = await self.get_participant_meta(
            internal_id, all_rows
        )

        args = {
            'id': internal_id,  # noqa: E501
            'external_id': participant_name,
            # 'reported_sex': None,
            # 'reported_gender': None,
            # 'karyotype': None,
            'meta': collapsed_participant_meta.meta,
            'samples': samples_to_upsert,
        }

        if reported_sex:
            args['reported_sex'] = reported_sex
        if reported_gender:
            args['reported_gender'] = reported_gender
        if karyotype:
            args['karyotype'] = karyotype

        if not internal_id:
            del args['id']

        participant_to_upsert = ParticipantUpsert(**args)

        return (
            participant_to_upsert,
            samples_to_upsert,
            sequences_to_upsert,
            analyses_to_add,
        )

    async def file_pointer_to_sample_map(
        self,
        file_pointer,
        delimiter: str,
    ) -> Dict[str, List]:
        """
        Parse manifest file into a list of dicts, indexed by sample name.
        Override this method if you can't use the default implementation that simply
        calls csv.DictReader.
        """
        sample_map = defaultdict(list)
        reader = csv.DictReader(file_pointer, delimiter=delimiter)
        for row in reader:
            sid = self.get_sample_id(row)
            sample_map[sid].append(row)
        return sample_map

    async def file_pointer_to_participant_map(
        self,
        file_pointer,
        delimiter: str,
    ) -> Dict[Any, Dict[Any, List[Any]]]:
        """
        Parse manifest file into a list of dicts, indexed by participant id.
        Override this method if you can't use the default implementation that simply
        calls csv.DictReader.
        """

        participant_map: Dict[Any, Dict[Any, List[Any]]] = defaultdict(
            lambda: defaultdict(list)
        )
        reader = csv.DictReader(file_pointer, delimiter=delimiter)
        for row in reader:
            pid = self.get_participant_id(row)

            if not pid:
                raise ValueError(f'Participant not found in row: {row}')

            sid = self.get_sample_id(row)
            participant_map[pid][sid].append(row)

        return participant_map

    async def validate_rows(self, sample_map: Dict[str, Union[Dict, List[Dict]]]):
        """
        Validate sample rows:
        - throw an exception if an error occurs
        - log a warning for all other issues
        """
        return

    async def parse_manifest(  # pylint: disable=too-many-branches
        self, file_pointer, delimiter=',', confirm=False, dry_run=False
    ) -> Dict[str, Dict]:
        """
        Parse manifest from iterable (file pointer / String.IO)

        Returns a dict mapping external sample ID to CPG sample ID
        """
        sample_map: Dict[str, Any] = {}
        participant_map: Dict[str, Any] = {}
        if self.has_participants(file_pointer, delimiter):
            participant_map = await self.file_pointer_to_participant_map(
                file_pointer, delimiter
            )
        else:
            sample_map = await self.file_pointer_to_sample_map(file_pointer, delimiter)

        if len(sample_map) == 0 and len(participant_map) == 0:
            raise ValueError(
                f'{self.sample_metadata_project}: The manifest file contains no records'
            )

        if len(sample_map) != 0:
            return await self.parse_manifest_by_samples(
                sample_map, confirm=confirm, dry_run=dry_run
            )

        return await self.parse_manifest_by_participants(
            participant_map, confirm=confirm, dry_run=dry_run
        )

    def upsert_summary(
        self,
        existing_summary: Dict[str, Dict[str, List[Any]]],
        participants_to_upsert: List[ParticipantUpsert],
        samples_to_upsert: List[SampleBatchUpsert],
        sequences_to_upsert: List[SequenceUpsert],
        analyses_to_add: Dict[str, List[AnalysisModel]],
    ) -> Dict[str, Dict[str, List[Any]]]:
        """Given lists of values to upsert return grouped summary of updates and inserts"""

        def upsert_type(item: Dict[str, Any]) -> str:
            return 'update' if hasattr(item, 'id') else 'insert'

        # Set initial dictionary value
        summary: Dict[str, Dict[str, List[Any]]] = (
            existing_summary.copy()
            if existing_summary
            else defaultdict(lambda: defaultdict(list))
        )

        for participant in participants_to_upsert:
            summary['participants'][upsert_type(participant)].append(participant)

        for sample in samples_to_upsert:
            summary['samples'][upsert_type(sample)].append(sample)

        for sequence in sequences_to_upsert:
            summary['sequences'][upsert_type(sequence)].append(sequence)

        for sid, analyses in analyses_to_add.items():
            summary['analyses'][sid].extend(analyses)

        return summary

    async def add_analyses(self, analyses_to_add, external_to_internal_id_map):
        """Given an analyses dictionary add analyses"""
        proj = self.sample_metadata_project
        analysisapi = AnalysisApi()

        logger.info(
            f'{proj}: Adding analysis entries for {len(analyses_to_add)} samples'
        )
        unwrapped_analysis_to_add = [
            (sample_id, a)
            for (sample_id, analyses) in analyses_to_add.items()
            for a in analyses
        ]

        results = []
        for chunked_analysis in chunk(unwrapped_analysis_to_add):
            promises = []
            for external_id, analysis in chunked_analysis:
                analysis.sample_ids = [external_to_internal_id_map[external_id]]
                promises.append(
                    analysisapi.create_new_analysis_async(
                        project=proj, analysis_model=analysis
                    )
                )
            results.append(await asyncio.gather(*promises))

        return results

    async def parse_manifest_by_participants(
        self,
        participant_map: Dict[str, Any],
        confirm: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Parses a manifest of data that is keyed on participant id and sample id"""

        proj = self.sample_metadata_project

        # all dicts indexed by external_sample_id
        summary: Dict[str, Dict[str, List[Any]]] = None
        all_participants: List[ParticipantUpsert] = []
        analyses_to_add: Dict[str, List[AnalysisModel]] = defaultdict(list)

        # we'll batch process the samples as not to open too many threads
        for external_pids in chunk(list(participant_map.keys())):

            current_batch_promises = {}
            if self.verbose:
                logger.info(f'{proj}:Preparing {", ".join(external_pids)}')

            for external_pid in external_pids:
                sample_map = participant_map[external_pid]
                promise = self.process_participant_group(external_pid, sample_map)
                current_batch_promises[external_pid] = promise

            processed_ex_pids = list(current_batch_promises.keys())
            batch_promises = list(current_batch_promises.values())
            resolved_promises = await asyncio.gather(*batch_promises)

            for external_pid, resolved_promise in zip(
                processed_ex_pids, resolved_promises
            ):
                (
                    participant_to_upsert,
                    samples_to_upsert,
                    sequences_to_upsert,
                    analysis_to_add,
                ) = resolved_promise

                if analysis_to_add:
                    analyses_to_add[external_pid] = analysis_to_add

                if participant_to_upsert:
                    all_participants.append(participant_to_upsert)

                # Get summary information
                summary = self.upsert_summary(
                    summary,
                    [participant_to_upsert],
                    samples_to_upsert,
                    sequences_to_upsert,
                    {external_pid: analysis_to_add},
                )

        message = f"""\
            {proj}: Processing participants: {', '.join(participant_map.keys())}

            Adding {len(summary['participants']['insert'])} participants
            Adding {len(summary['samples']['insert'])} samples
            Adding {len(summary['sequences']['insert'])} sequences
            Adding {len(summary['analyses']['insert'])} analysis

            Updating {len(summary['participants']['update'])} participants
            Updating {len(summary['samples']['update'])} samples
            Updating {len(summary['sequences']['update'])} sequences
        """

        if dry_run:
            logger.info('Dry run, so returning without inserting / updating metadata')
            return summary

        if confirm:
            resp = str(input(message + '\n\nConfirm (y): '))
            if resp.lower() != 'y':
                raise SystemExit()
        else:
            logger.info(message)

        # Batch update
        result = self.papi.batch_upsert_participants(
            proj, ParticipantUpsertBody(participants=all_participants)
        )

        return result

    async def parse_manifest_by_samples(
        self, sample_map: Dict[str, Any], confirm: bool = False, dry_run: bool = False
    ) -> Dict[str, Any]:
        """Parses a manifest of data that is keyed on sample id"""
        proj = self.sample_metadata_project

        # now we can start adding!!
        sapi = SampleApi()

        # Map external sids into cpg ids
        existing_external_id_to_cpgid = await sapi.get_sample_id_map_by_external_async(
            proj, list(sample_map.keys()), allow_missing=True
        )

        # all dicts indexed by external_sample_id
        summary: Dict[str, Dict[str, List[Any]]] = None
        all_samples: List[SampleBatchUpsert] = []
        analyses_to_add: Dict[str, List[AnalysisModel]] = defaultdict(list)

        # we'll batch process the samples as not to open too many threads
        for external_sids in chunk(list(sample_map.keys())):

            current_batch_promises = {}
            if self.verbose:
                logger.info(f'{proj}:Preparing {", ".join(external_sids)}')

            for external_sid in external_sids:
                rows: GroupedRow = sample_map[external_sid]
                cpg_id = existing_external_id_to_cpgid.get(external_sid, None)
                promise = self.process_sample_group(rows, external_sid, cpg_id)
                current_batch_promises[external_sid] = promise

            processed_ex_sids = list(current_batch_promises.keys())
            batch_promises = list(current_batch_promises.values())
            resolved_promises = await asyncio.gather(*batch_promises)

            for external_sid, resolved_promise in zip(
                processed_ex_sids, resolved_promises
            ):
                (
                    sample_to_upsert,
                    sequences_to_upsert,
                    analysis_to_add,
                ) = resolved_promise

                # Extract Upsert items and add to an array
                if analysis_to_add:
                    analyses_to_add[external_sid] = analysis_to_add

                if sample_to_upsert:
                    all_samples.append(sample_to_upsert)

                # Get summary information
                summary = self.upsert_summary(
                    summary,
                    [],
                    [sample_to_upsert],
                    sequences_to_upsert,
                    {external_sid: analysis_to_add},
                )

        message = f"""\
            {proj}: Processing samples: {', '.join(sample_map.keys())}

            Adding {len(summary['samples']['insert'])} samples
            Adding {len(summary['sequences']['insert'])} sequences
            Adding {len(summary['analyses']['insert'])} analyses

            Updating {len(summary['samples']['update'])} samples
            Updating {len(summary['sequences']['update'])} sequences
        """

        if dry_run:
            logger.info('Dry run, so returning without inserting / updating metadata')
            return summary

        if confirm:
            resp = str(input(message + '\n\nConfirm (y): '))
            if resp.lower() != 'y':
                raise SystemExit()
        else:
            logger.info(message)

        # Batch update
        result = sapi.batch_upsert_samples(
            proj, SampleBatchUpsertBody(samples=all_samples)
        )

        # Add analyses
        # Map external sids into cpg ids
        existing_external_id_to_cpgid = await sapi.get_sample_id_map_by_external_async(
            proj, list(sample_map.keys()), allow_missing=True
        )
        _ = await self.add_analyses(analyses_to_add, existing_external_id_to_cpgid)

        return result

    async def parse_files(
        self, sample_id: str, reads: List[str]
    ) -> Dict[SUPPORTED_FILE_TYPE, Dict[str, List]]:
        """
        Returns a tuple of:
        1. single / list-of CWL file object(s), based on the extensions of the reads
        2. parsed type (fastq, cram, bam)
        """

        file_by_type: Dict[SUPPORTED_FILE_TYPE, Dict[str, List]] = defaultdict(
            lambda: defaultdict(list)
        )

        fastqs = [
            r for r in reads if any(r.lower().endswith(ext) for ext in FASTQ_EXTENSIONS)
        ]
        if fastqs:
            structured_fastqs = self.parse_fastqs_structure(fastqs)
            fastq_files: List[Sequence[Union[Coroutine, BaseException]]] = []  # type: ignore
            for fastq_group in structured_fastqs:
                create_file_futures: List[Coroutine] = [
                    self.create_file_object(f) for f in fastq_group
                ]
                fastq_files.append(asyncio.gather(*create_file_futures))  # type: ignore

            grouped_fastqs = list(await asyncio.gather(*fastq_files))  # type: ignore
            file_by_type['reads']['fastq'].extend(grouped_fastqs)

        crams = [
            r for r in reads if any(r.lower().endswith(ext) for ext in CRAM_EXTENSIONS)
        ]
        file_promises: List[Coroutine]

        if crams:
            file_promises = []
            sec_format = ['.crai', '^.crai']
            for r in crams:
                secondaries = (
                    await self.create_secondary_file_objects_by_potential_pattern(
                        r, sec_format
                    )
                )
                file_promises.append(
                    self.create_file_object(r, secondary_files=secondaries)
                )
            file_by_type['reads']['cram'] = await asyncio.gather(*file_promises)  # type: ignore

        bams = [
            r for r in reads if any(r.lower().endswith(ext) for ext in BAM_EXTENSIONS)
        ]
        if bams:
            file_promises = []
            sec_format = ['.bai', '^.bai']
            for r in bams:
                secondaries = (
                    await self.create_secondary_file_objects_by_potential_pattern(
                        r, sec_format
                    )
                )
                file_promises.append(
                    self.create_file_object(r, secondary_files=secondaries)
                )

            file_by_type['reads']['bam'] = await asyncio.gather(*file_promises)  # type: ignore

        gvcfs = [
            r for r in reads if any(r.lower().endswith(ext) for ext in GVCF_EXTENSIONS)
        ]
        vcfs = [
            r
            for r in reads
            if any(r.lower().endswith(ext) for ext in VCF_EXTENSIONS) and r not in gvcfs
        ]

        if gvcfs:
            file_promises = []
            sec_format = ['.tbi']
            for r in gvcfs:
                secondaries = (
                    await self.create_secondary_file_objects_by_potential_pattern(
                        r, sec_format
                    )
                )
                file_promises.append(
                    self.create_file_object(r, secondary_files=secondaries)
                )

            file_by_type['variants']['gvcf'] = await asyncio.gather(*file_promises)  # type: ignore

        if vcfs:
            file_promises = []
            for r in vcfs:
                file_promises.append(self.create_file_object(r))

            file_by_type['variants']['vcf'] = await asyncio.gather(*file_promises)  # type: ignore

        unhandled_files = [
            r
            for r in reads
            if not any(r.lower().endswith(ext) for ext in ALL_EXTENSIONS)
        ]
        if unhandled_files:
            joined_reads = ''.join(
                f'\n\t{i}: {r}' for i, r in enumerate(unhandled_files)
            )
            logger.warning(
                f'There were files with extensions that were skipped ({sample_id}): {joined_reads}'
            )

        return file_by_type

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
        secondary_files: List[SingleRow] = None,
    ) -> SingleRow:
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
    ) -> List[SingleRow]:
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
