# pylint: disable=too-many-lines,too-many-instance-attributes,too-many-locals,unused-argument,assignment-from-none,invalid-name,ungrouped-imports
import json
import sys
import asyncio
import csv
import logging
import os
import re
from abc import abstractmethod
from collections import defaultdict
from io import StringIO
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
    Set,
    Iterable,
)
from functools import wraps

from cloudpathlib import AnyPath

from api.utils import group_by
from sample_metadata.model.sequence_group_upsert import SequenceGroupUpsert

from sample_metadata.parser.cloudhelper import CloudHelper

from sample_metadata.model.participant_upsert_body import ParticipantUpsertBody

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
    SequenceTechnology,
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

# construct rmatch string to capture all fastq patterns
rmatch_str = (
    r'[_\.-][Rr]?[12]('
    + '|'.join(s.replace('.', '\\.') for s in FASTQ_EXTENSIONS)
    + ')$'
)
rmatch = re.compile(rmatch_str)
SingleRow = Dict[str, Any]
GroupedRow = List[SingleRow]

T = TypeVar('T')

SUPPORTED_FILE_TYPE = Literal['reads', 'variants']
SUPPORTED_READ_TYPES = Literal['fastq', 'bam', 'cram']
SUPPORTED_VARIANT_TYPES = Literal['gvcf', 'vcf']


class CustomDictReader(csv.DictReader):
    """csv.DictReader that strips whitespace off headers"""

    def __init__(
        self,
        *args,
        key_map=None,
        required_keys: Iterable[str] = None,
        ignore_extra_keys=False,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.key_map = key_map
        self.ignore_extra_keys = ignore_extra_keys
        self.required_keys = set(required_keys) if required_keys else None
        self._custom_cached_fieldnames = None

    @property
    def fieldnames(self):
        if not self._custom_cached_fieldnames:
            fs = super().fieldnames
            self._custom_cached_fieldnames = list(map(self.process_fieldname, fs))

            if self.required_keys:
                missing_keys = self.required_keys - set(self._custom_cached_fieldnames)
                if missing_keys:
                    raise ValueError('Missing keys: ' + ','.join(missing_keys))
        return self._custom_cached_fieldnames

    def process_fieldname(self, fieldname: str) -> str:
        """
        Process the fieldname
        (default: strip leading / trailing whitespace)
        """
        if self.key_map:
            for k, values in self.key_map.items():
                if fieldname.lower() in values:
                    return k

            if not self.ignore_extra_keys:
                raise ValueError(
                    f'Key "{fieldname}" not found in provided key map: {", ".join(self.key_map.keys())}'
                )

        return fieldname.strip()


class ParsedParticipant:
    """Class for holding participant metadata grouped by id"""

    def __init__(
        self,
        rows: GroupedRow,
        internal_pid: int | None,
        external_pid: str,
        meta: Dict[str, Any],
        reported_sex,
        reported_gender,
        karyotype,
    ):
        self.rows = rows

        self.internal_pid = internal_pid
        self.external_pid = external_pid

        self.reported_sex = reported_sex
        self.reported_gender = reported_gender
        self.karyotype = karyotype
        self.meta = meta

        self.samples: list[ParsedSample] = []

    def to_sm(self) -> ParticipantUpsert:
        return ParticipantUpsert(samples=[s.to_sm() for s in self.samples])


class ParsedSample:
    """Class for holding sample metadata grouped by id"""

    def __init__(
        self,
        participant: ParsedParticipant | None,
        rows: GroupedRow,
        internal_sid: str | None,
        external_sid: str,
        sample_type: str | SampleType,
        meta: Dict[str, Any] = None,
    ):
        self.participant = participant
        self.rows = rows

        self.internal_sid = internal_sid
        self.external_sid = external_sid
        self.sample_type = sample_type
        self.meta = meta

        self.sequence_groups: list[ParsedSequenceGroup] = []

    def to_sm(self) -> SampleBatchUpsert:
        return SampleBatchUpsert(
            id=self.internal_sid,
            external_id=self.external_sid,
            type=SampleType(self.sample_type),
            meta=self.meta,
            # participant_id=self.par,
            active=True,
            sequence_groups=[sg.to_sm() for sg in self.sequence_groups],
        )


class ParsedSequenceGroup:
    """Class for holding sequence metadata grouped by type"""

    def __init__(
        self,
        sample: ParsedSample,
        rows: GroupedRow,
        internal_seqgroup_id: int | None,
        external_seqgroup_id: str | None,
        sequence_type: SequenceType,
        sequence_technology: SequenceTechnology,
        sequence_platform: str | None,
        meta: dict[str, Any] | None,
    ):
        self.sample = sample
        self.rows = rows

        self.internal_seqgroup_id = internal_seqgroup_id
        self.external_seqgroup_id = external_seqgroup_id
        self.sequence_type = sequence_type
        self.sequence_technology = sequence_technology
        self.sequence_platform = sequence_platform
        self.meta = meta

        self.sequences: list[ParsedSequence] = []
        self.analyses: list[ParsedAnalysis] = []

    def to_sm(self) -> SequenceGroupUpsert:
        return SequenceGroupUpsert(
            type=SequenceType(self.sequence_type),
            technology=SequenceTechnology(self.sequence_technology),
            platform=self.sequence_platform,
            meta=self.meta,
            sequences=[sq.to_sm() for sq in self.sequences],
            id=self.internal_seqgroup_id,
        )


class ParsedSequence:
    def __init__(
        self,
        group: ParsedSequenceGroup,
        rows: GroupedRow,
        internal_seq_id: int | None,
        external_seq_ids: dict[str, str],
        sequence_type: SequenceType,
        sequence_technology: SequenceTechnology,
        sequence_platform: str | None,
        meta: dict[str, Any] | None,
    ):
        self.sequence_group = group
        self.rows = rows

        self.internal_seq_id = internal_seq_id
        self.external_seq_ids = external_seq_ids
        self.sequence_type = sequence_type
        self.sequence_technology = sequence_technology
        self.sequence_platform = sequence_platform
        self.meta = meta

    def to_sm(self) -> SequenceUpsert:
        return SequenceUpsert(
            id=self.internal_seq_id,
            external_ids=self.external_seq_ids,
            # sample_id=self.s,
            status=SequenceStatus(self.sequence_status),
            technology=SequenceTechnology(self.sequence_technology),
            meta=self.meta,
            type=SequenceType(self.sequence_type),
        )


class ParsedAnalysis:
    def __init__(
        self,
        sequence_group: ParsedSequenceGroup,
        rows: GroupedRow,
        status: AnalysisStatus,
        type_: AnalysisType,
        meta: dict,
        output: str | None,
    ):
        self.sequence_group = sequence_group
        self.rows = rows
        self.status = status
        self.type_ = type_
        self.meta = meta
        self.output = output


def chunk(iterable: Iterable[T], chunk_size=50) -> Iterator[List[T]]:
    """
    Chunk a sequence by yielding lists of `chunk_size`
    """
    chnk: List[T] = []
    for element in iterable:
        chnk.append(element)
        if len(chnk) >= chunk_size:
            yield chnk
            chnk = []

    if chnk:
        yield chnk


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


class GenericParser(
    CloudHelper
):  # pylint: disable=too-many-public-methods,too-many-arguments
    """Parser for VCGS manifest"""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        path_prefix: Optional[str],
        search_paths: list[str],
        project: str,
        default_sequence_type='genome',
        default_sequence_technology='short-read',
        default_sequence_status='uploaded',
        default_sequence_platform: str | None = None,
        default_sample_type=None,
        default_analysis_type='qc',
        default_analysis_status='completed',
        skip_checking_gcs_objects=False,
        key_map: Dict[str, str] = None,
        ignore_extra_keys=False,
        required_keys: Set[str] = None,
        verbose=True,
    ):

        self.path_prefix = path_prefix
        self.skip_checking_gcs_objects = skip_checking_gcs_objects
        self.verbose = verbose

        self.key_map = key_map
        self.required_keys = required_keys
        self.ignore_extra_keys = ignore_extra_keys

        if not project:
            raise ValueError('sample-metadata project is required')

        self.project = project

        self.default_sequence_type: str = default_sequence_type
        self.default_sequence_technology: str = default_sequence_technology
        self.default_sequence_platform: str | None = default_sequence_platform
        self.default_sequence_status: str = default_sequence_status
        self.default_sample_type: str = default_sample_type
        self.default_analysis_type: str = default_analysis_type
        self.default_analysis_status: str = default_analysis_status

        # gs specific
        self.default_bucket = None

        self._client = None
        self.bucket_clients: Dict[str, Any] = {}

        self.papi = ParticipantApi()
        self.sapi = SampleApi()
        self.seqapi = SequenceApi()

        super().__init__(search_paths)

    # region generic utils

    def file_path(self, filename: str, raise_exception: bool = True) -> str | None:
        """
        Get complete filepath of filename:
        - Includes gs://{bucket} if relevant
        - Includes path_prefix decided early on
        """
        if filename.startswith('gs://') or filename.startswith('/'):
            return filename

        if not self.path_prefix:
            fn = super().file_path(filename, raise_exception=raise_exception)
            if not fn:
                raise FileNotFoundError(
                    f'Cannot form full path to "{filename}" as '
                    'no path_prefix was defined'
                )
            return fn

        return os.path.join(self.path_prefix or '', filename)

    # endregion

    # region file management

    async def from_manifest_path(
        self,
        manifest: str,
        confirm=False,
        delimiter=None,
        dry_run=False,
    ):
        """Parse manifest from path, and return result of parsing manifest"""
        file = self.file_path(manifest)

        _delimiter = delimiter or self.guess_delimiter_from_filename(file)

        file_contents = await self.file_contents(file)
        return await self.parse_manifest(
            StringIO(file_contents),
            delimiter=_delimiter,
            confirm=confirm,
            dry_run=dry_run,
        )

    async def parse_manifest(  # pylint: disable=too-many-branches
        self, file_pointer, delimiter=',', confirm=False, dry_run=False
    ) -> Any:
        """
        Parse manifest from iterable (file pointer / String.IO)

        Returns a dict mapping external sample ID to CPG sample ID
        """
        rows = await self.file_pointer_to_rows(
            file_pointer=file_pointer, delimiter=delimiter
        )
        await self.validate_rows(rows)

        # one participant with no value
        participants = []
        if self.has_participants(rows):
            # start with participants
            participants = await self.group_participants(rows)
            await self.match_participant_ids(participants)

            samples: list[ParsedSample] = []
            for pchunk in chunk(participants):
                samples_for_chunk = await asyncio.gather(
                    *[self.group_samples(p, p.rows) for p in pchunk]
                )

                for participant, psamples in zip(pchunk, samples_for_chunk):
                    participant.samples = psamples
                    samples.extend(psamples)

        else:
            samples = await self.group_samples(None, rows=rows)

        await self.match_sample_ids(samples)

        sequence_groups: list[ParsedSequenceGroup] = []
        for schunk in chunk(samples):
            seq_groups_for_chunk = await asyncio.gather(
                *map(self.group_sequences, schunk)
            )

            for sample, seqgroups in zip(schunk, seq_groups_for_chunk):
                sample.sequence_groups = seqgroups
                sequence_groups.extend(seqgroups)

        await self.match_sequence_group_ids(sequence_groups)

        sequences: list[ParsedSequence] = []
        for sgchunk in chunk(sequence_groups):
            sequences_for_chunk = await asyncio.gather(
                *map(self.get_sequences_from_group, sgchunk)
            )
            analyses_for_chunk = await asyncio.gather(
                *map(self.get_analyses_from_sequence_group, sgchunk)
            )

            for sequence_group, seqs, analyses in zip(
                sgchunk, sequences_for_chunk, analyses_for_chunk
            ):
                sequence_group.sequences = seqs
                sequences.extend(seqs)
                sequence_group.analyses = analyses

        await self.match_sequence_ids(sequences)

        summary = self.prepare_summary(
            participants, samples, sequence_groups, sequences
        )
        message = self.prepare_message(
            summary, participants, samples, sequence_groups, sequences
        )

        if dry_run:
            logger.info('Dry run, so returning without inserting / updating metadata')
            return summary, (participants if participants else samples)

        if confirm:
            resp = str(input(message + '\n\nConfirm (y): '))
            if resp.lower() != 'y':
                raise SystemExit()
        else:
            logger.info(message)

        if participants:
            result = self.papi.batch_upsert_participants(
                self.project,
                ParticipantUpsertBody(participants=[p.to_sm() for p in participants]),
            )
        else:
            result = await self.sapi.batch_upsert_samples_async(
                self.project,
                SampleBatchUpsertBody(samples=[s.to_sm() for s in samples]),
            )

        print(json.dumps(result, indent=2))

    def _get_dict_reader(self, file_pointer, delimiter: str):
        """
        Return a DictReader from file_pointer
        Override this method if you can't use the default implementation that simply
        calls csv.DictReader
        """
        reader = CustomDictReader(
            file_pointer,
            delimiter=delimiter,
            key_map=self.key_map,
            required_keys=self.required_keys,
            ignore_extra_keys=self.ignore_extra_keys,
        )
        return reader

    async def file_pointer_to_rows(self, file_pointer, delimiter) -> list[SingleRow]:
        reader = self._get_dict_reader(file_pointer, delimiter=delimiter)
        return [r for r in reader]

    def prepare_summary(
        self,
        participants: list[ParsedParticipant],
        samples: list[ParsedSample],
        sequence_groups: list[ParsedSequenceGroup],
        sequences: list[ParsedSequence],
    ):
        participants_to_insert = sum(1 for p in participants if not p.internal_pid)
        samples_to_insert = sum(1 for s in samples if not s.internal_sid)
        sgs_to_insert = sum(1 for sg in sequence_groups if not sg.internal_seqgroup_id)
        sequences_to_insert = sum(1 for sq in sequences if not sq.internal_seq_id)
        analyses_to_insert = sum(len(sg.analyses or []) for sg in sequence_groups)
        summary = {
            'participants': {
                'insert': participants_to_insert,
                'update': len(participants) - participants_to_insert,
            },
            'samples': {
                'insert': samples_to_insert,
                'update': len(samples) - samples_to_insert,
            },
            'sequence_groups': {
                'insert': sgs_to_insert,
                'update': len(sequence_groups) - sgs_to_insert,
            },
            'sequences': {
                'insert': sequences_to_insert,
                'update': len(sequences) - sequences_to_insert,
            },
            'analyses': {'insert': analyses_to_insert},
        }

        return summary

    def prepare_message(
        self,
        summary,
        participants: list[ParsedParticipant],
        samples: list[ParsedSample],
        sequence_groups: list[ParsedSequenceGroup],
        sequences: list[ParsedSequence],
    ):
        if participants:
            external_participant_ids = ', '.join(
                set(p.external_pid for p in participants)
            )
            header = f'Processing participants: {external_participant_ids}'
        else:
            external_sample_ids = ', '.join(set(s.external_sid for s in samples))
            header = f'Processing samples: {external_sample_ids}'

        sequences_count: dict[str, int] = defaultdict(int)
        sequence_group_counts: dict[str, int] = defaultdict(int)
        for s in sequences:
            sequences_count[str(s.sequence_type)] += 1
        for sg in sequence_groups:
            sequence_group_counts[str(sg.sequence_type)] += 1

        str_seq_count = ', '.join(f'{k}={v}' for k, v in sequences_count.items())
        str_seqg_count = ', '.join(f'{k}={v}' for k, v in sequence_group_counts.items())

        message = f"""\
                {self.project}: {header}

                Sequence types: {str_seq_count}
                Sequence group types: {str_seqg_count}

                Adding {summary['participants']['insert']} participants
                Adding {summary['samples']['insert']} samples
                Adding {summary['sequence_groups']['insert']} sequence groups
                Adding {summary['sequences']['insert']} sequences
                Adding {summary['analyses']['insert']} analysis

                Updating {summary['participants']['update']} participants
                Updating {summary['samples']['update']} samples
                Updating {summary['sequence_groups']['update']} sequence groups
                Updating {summary['sequences']['update']} sequences
                """
        return message

    # region MATCHING

    async def match_participant_ids(self, participants: list[ParsedParticipant]):
        external_pids = {p.external_pid for p in participants}

        papi = ParticipantApi()
        pid_map = await papi.get_participant_id_map_by_external_ids_async(
            list(external_pids), allow_missing=True
        )

        for participant in participants:
            participant.internal_pid = pid_map.get(participant.external_pid)

    async def match_sample_ids(self, samples: list[ParsedSample]):
        external_sids = {s.external_sid for s in samples}
        sapi = SampleApi()
        sid_map = await sapi.get_sample_id_map_by_external_async(
            list(external_sids), allow_missing=True
        )

        for sample in samples:
            sample.internal_sid = sid_map.get(sample.external_sid)

    async def match_sequence_group_ids(
        self, sequence_groups: list[ParsedSequenceGroup]
    ):
        pass

    async def match_sequence_ids(self, sequences: list[ParsedSequence]):
        pass

    # endregion MATCHING

    async def validate_rows(self, rows: list[SingleRow]):
        """
        Validate sample rows:
        - throw an exception if an error occurs
        - log a warning for all other issues
        """
        if len(rows) == 0:
            raise ValueError('The manifest contains no records')

    # endregion

    @abstractmethod
    def get_sample_id(self, row: SingleRow) -> str:
        """Get external sample ID from row"""

    # @abstractmethod
    def get_sequence_id(self, row: GroupedRow) -> Optional[dict[str, str]]:
        """Get external sequence ID from row"""
        return None

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
    def has_participants(self, rows: list[SingleRow]) -> bool:
        """Returns True if the file has a Participants column"""

    async def group_participants(
        self, rows: list[SingleRow]
    ) -> list[ParsedParticipant]:

        participant_groups: list[ParsedParticipant] = []
        for pid, rows in group_by(rows, self.get_participant_id).items():
            participant_groups.append(
                ParsedParticipant(
                    internal_pid=None,
                    external_pid=pid,
                    rows=rows,
                    meta=await self.get_participant_meta_from_group(rows),
                    reported_sex=self.get_reported_sex(rows),
                    reported_gender=self.get_reported_gender(rows),
                    karyotype=self.get_karyotype(rows),
                )
            )

        return participant_groups

    async def get_participant_meta_from_group(self, rows: GroupedRow) -> dict:
        return {}

    async def group_samples(
        self, participant: ParsedParticipant | None, rows
    ) -> list[ParsedSample]:
        samples = []
        for sid, sample_rows in group_by(rows, self.get_sample_id).items():
            samples.append(
                ParsedSample(
                    rows=sample_rows,
                    participant=participant,
                    internal_sid=None,
                    external_sid=sid,
                    sample_type=self.get_sample_type(sample_rows),
                    meta=await self.get_sample_meta_from_group(sample_rows),
                )
            )

        return samples

    async def get_sample_meta_from_group(self, rows: GroupedRow) -> dict:
        return {}

    def get_sequence_group_key(self, row):
        if seq_group_id := self.get_sequence_group_id(row):
            return seq_group_id

        return (
            str(self.get_sequence_type(row)),
            str(self.get_sequence_technology(row)),
            str(self.get_sequence_platform(row)),
        )

    async def group_sequences(self, sample: ParsedSample) -> list[ParsedSequenceGroup]:

        sequence_groups = []
        for seq_rows in group_by(sample.rows, self.get_sequence_group_key).values():
            seq_type = self.get_sequence_type(seq_rows[0])
            seq_tech = self.get_sequence_technology(seq_rows[0])
            seq_platform = self.get_sequence_platform(seq_rows[0])

            seq_group = ParsedSequenceGroup(
                internal_seqgroup_id=None,
                external_seqgroup_id=self.get_sequence_group_id(seq_rows[0]),
                sequence_type=seq_type,
                sequence_technology=seq_tech,
                sequence_platform=seq_platform,
                meta={},
                sample=sample,
                rows=seq_rows,
            )

            seq_group.meta = await self.get_sequence_group_meta(seq_group)
            sequence_groups.append(seq_group)

        return sequence_groups

    async def get_analyses_from_sequence_group(
        self, sequence_group: ParsedSequenceGroup
    ) -> list[ParsedAnalysis]:
        return []

    async def get_sequence_group_meta(
        self, sequence_group: ParsedSequenceGroup
    ) -> dict:
        return {}

    @abstractmethod
    async def get_sequences_from_group(
        self, sequence_group: ParsedSequenceGroup
    ) -> list[ParsedSequence]:
        pass

    def get_sample_type(self, row: GroupedRow) -> SampleType:
        """Get sample type from row"""
        return SampleType(self.default_sample_type)

    def get_sequence_group_id(self, row: SingleRow) -> str | None:
        return None

    def get_sequence_type(self, row: SingleRow) -> SequenceType:
        """Get sequence types from row"""
        return SequenceType(self.default_sequence_type)

    def get_sequence_technology(self, row: SingleRow) -> SequenceTechnology | str:
        return self.default_sequence_technology

    def get_sequence_platform(self, row: SingleRow) -> str | None:
        return None

    def get_sequence_status(self, row: GroupedRow) -> SequenceStatus:
        """Get sequence status from row"""
        return SequenceStatus(self.default_sequence_status)

    def get_analysis_type(self, sample_id: str, row: GroupedRow) -> AnalysisType:
        """Get analysis type from row"""
        return AnalysisType(self.default_analysis_type)

    def get_analysis_status(self, sample_id: str, row: GroupedRow) -> AnalysisStatus:
        """Get analysis status from row"""
        return AnalysisStatus(self.default_analysis_status)

    @staticmethod
    def get_existing_external_sequence_ids(
        participant_map: Dict[str, Dict[Any, List[Any]]]
    ):
        """Pulls external sequence IDs from participant map"""
        external_sequence_ids: list[str] = []
        for participant in participant_map:
            for sample in participant_map[participant]:
                for sequence in participant_map[participant][sample]:
                    external_sequence_ids.append((sequence.get('Sequence ID')))

        return external_sequence_ids

    @staticmethod
    def get_existing_sequences(
        sequences: list[dict[str, Any]], external_sequence_ids: list[str]
    ):
        """Accounts for external_sequence_ids when determining which sequences
        need to be updated vs inserted"""

        existing_sequences: list[dict[str, Any]] = []
        for seq in sequences:
            if not seq['external_ids'].values():
                # No existing sequence ID, we can assume that replacement should happen
                # Note: This means that you can't have a mix of sequences with and without
                # external sequence IDs in one dataset.
                existing_sequences.append(seq)

            else:
                for ext_id in seq['external_ids'].values():
                    # If the external ID is already there, we want to upsert.
                    if ext_id in external_sequence_ids:
                        existing_sequences.append(seq)

        return existing_sequences


    async def add_analyses(self, analyses_to_add, external_to_internal_id_map):
        """Given an analyses dictionary add analyses"""
        proj = self.project
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

    async def parse_files(
        self, sample_id: str, reads: List[str], checksums: List[str] = None
    ) -> Dict[SUPPORTED_FILE_TYPE, Dict[str, List]]:
        """
        Returns a tuple of:
        1. single / list-of CWL file object(s), based on the extensions of the reads
        2. parsed type (fastq, cram, bam)
        """

        if not isinstance(reads, list):
            reads = [reads]

        if not checksums:
            checksums = [None] * len(reads)

        if len(checksums) != len(reads):
            raise ValueError(
                'Expected length of reads to match length of provided checksums'
            )

        read_to_checksum = dict(zip(reads, checksums))

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
                    self.create_file_object(f, checksum=read_to_checksum.get(f))
                    for f in fastq_group
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

        >>> GenericParser.parse_fastqs_structure(['Sample_1_L01_1.fastq.gz', 'Sample_1_L01_2.fastq.gz', 'Sample_1_L02_R1.fastq.gz', 'Sample_1_L02_R2.fastq.gz'])
        [['Sample_1_L01_1.fastq.gz', 'Sample_1_L01_2.fastq.gz'], ['Sample_1_L02_R1.fastq.gz', 'Sample_1_L02_R2.fastq.gz']]

        >>> GenericParser.parse_fastqs_structure(['File_1.fastq', 'File_2.fastq'])
        [['File_1.fastq', 'File_2.fastq']]

        >>> GenericParser.parse_fastqs_structure(['File_1.fq', 'File_2.fq'])
        [['File_1.fq', 'File_2.fq']]

        >>> GenericParser.parse_fastqs_structure(['File_1.fq.gz', 'File_2.fq.gz'])
        [['File_1.fq.gz', 'File_2.fq.gz']]
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
        for _, grouped in group_by(
            sorted_fastqs, lambda r: r_matches[r][0][: r_matches[r][1].start()]  # type: ignore
        ).items():
            values.append(sorted(grouped))
        invalid_fastq_groups = [grp for grp in values if len(grp) != 2]
        if invalid_fastq_groups:
            raise ValueError(f'Invalid fastq group {invalid_fastq_groups}')

        return sorted(values, key=lambda el: el[0])

    async def create_file_object(
        self,
        filename: str,
        secondary_files: List[SingleRow] = None,
        checksum: Optional[str] = None,
    ) -> SingleRow:
        """Takes filename, returns formed CWL dictionary"""
        _checksum = checksum
        file_size = None

        if not self.skip_checking_gcs_objects:
            if not _checksum:
                md5_filename = self.file_path(filename + '.md5')
                if await self.file_exists(md5_filename):
                    contents = await self.file_contents(md5_filename)
                    if contents:
                        _checksum = f'md5:{contents.strip()}'

            file_size = await self.file_size(filename)

        d = {
            'location': self.file_path(filename),
            'basename': os.path.basename(filename),
            'class': 'File',
            'checksum': _checksum,
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

        # pylint: disable=no-member
        with AnyPath(filename).open('r') as f:
            first_line = f.readline()
            delimiter = csv.Sniffer().sniff(first_line).delimiter
            if delimiter:
                logger.info(
                    f'Guessing delimiter based on first line, got "{delimiter}"'
                )
                return delimiter

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
