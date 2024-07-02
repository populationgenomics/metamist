# pylint: disable=too-many-lines,too-many-instance-attributes,too-many-locals,unused-argument,assignment-from-none,invalid-name,ungrouped-imports
import asyncio
import csv
import dataclasses
import json
import logging
import os
import re
import sys
from abc import abstractmethod
from collections import defaultdict
from functools import wraps
from io import StringIO
from typing import (
    Any,
    Coroutine,
    Dict,
    Hashable,
    Iterable,
    Iterator,
    List,
    Match,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from cloudpathlib import AnyPath
from tabulate import tabulate

from metamist.apis import AnalysisApi, AssayApi, ParticipantApi, SampleApi
from metamist.graphql import gql, query_async
from metamist.models import (
    Analysis,
    AnalysisStatus,
    AssayUpsert,
    ParticipantUpsert,
    SampleUpsert,
    SequencingGroupUpsert,
)
from metamist.parser.cloudhelper import CloudHelper, group_by

# https://mypy.readthedocs.io/en/stable/runtime_troubles.html#using-new-additions-to-the-typing-module
if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

PRIMARY_EXTERNAL_ORG = ''

FASTQ_EXTENSIONS = ('.fq.gz', '.fastq.gz', '.fq', '.fastq')
BAM_EXTENSIONS = ('.bam',)
CRAM_EXTENSIONS = ('.cram',)
GVCF_EXTENSIONS = ('.g.vcf.gz',)
VCF_EXTENSIONS = ('.vcf', '.vcf.gz')
READS_EXTENSIONS = FASTQ_EXTENSIONS + CRAM_EXTENSIONS + BAM_EXTENSIONS

ALL_EXTENSIONS = (
    FASTQ_EXTENSIONS
    + CRAM_EXTENSIONS
    + BAM_EXTENSIONS
    + GVCF_EXTENSIONS
    + VCF_EXTENSIONS
)
RNA_SEQ_TYPES = ['polyarna', 'totalrna', 'singlecellrna']

# construct rmatch string to capture all fastq patterns
rmatch_str = (
    r'(?:[<>]|\/|_|\.|-|[0-9]|[a-z]|[A-Z])+'
    + r'(?=[_|-]([12]|R[12])?(_[0-9]*?)?('
    + '|'.join(s.replace('.', '\\.') for s in FASTQ_EXTENSIONS)
    + '$))'
)
rmatch = re.compile(rmatch_str)
SingleRow = Dict[str, Any]
GroupedRow = List[SingleRow]

T = TypeVar('T')

SUPPORTED_FILE_TYPE = Literal['reads', 'variants']
SUPPORTED_READ_TYPES = Literal['fastq', 'bam', 'cram']
SUPPORTED_VARIANT_TYPES = Literal['gvcf', 'vcf']

QUERY_MATCH_PARTICIPANTS = gql(
    """
query GetParticipantEidMapQuery($project: String!) {
  project(name: $project) {
    participants {
      externalId
      id
    }
  }
}"""
)

QUERY_MATCH_SAMPLES = gql(
    """
query GetSampleEidMapQuery($project: String!) {
  project(name: $project) {
    samples {
      externalId
      id
    }
  }
}
        """
)
QUERY_MATCH_SEQUENCING_GROUPS = gql(
    """
query MyQuery($project:String!) {
  project(name: $project) {
    sequencingGroups {
      id
      assays {
        id
      }
    }
  }
}
"""
)
QUERY_MATCH_ASSAYS = gql(
    """
query GetSampleEidMapQuery($project: String!) {
  project(name: $project) {
    samples {
      assays {
        id
        externalIds
        meta
      }
    }
  }
}
"""
)


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
                    f'Key {fieldname!r} not found in provided key map: {", ".join(self.key_map.keys())}'
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
        """Convert to SM upsert model"""
        samples = [s.to_sm() for s in self.samples]
        return ParticipantUpsert(
            id=self.internal_pid,
            external_ids={PRIMARY_EXTERNAL_ORG: self.external_pid},
            reported_sex=self.reported_sex,
            reported_gender=self.reported_gender,
            karyotype=self.karyotype,
            meta=self.meta,
            samples=samples,
        )


class ParsedSample:
    """Class for holding sample metadata grouped by id"""

    def __init__(
        self,
        participant: ParsedParticipant | None,
        rows: GroupedRow,
        internal_sid: str | None,
        external_sid: str,
        sample_type: str,
        meta: Dict[str, Any] | None = None,
    ):
        self.participant = participant
        self.rows = rows

        self.internal_sid = internal_sid
        self.external_sid = external_sid
        self.sample_type = sample_type
        self.meta = meta

        self.samples: list[ParsedSample] = []
        self.sequencing_groups: list[ParsedSequencingGroup] = []

    def to_sm(self) -> SampleUpsert:
        """Convert to SM upsert model"""
        return SampleUpsert(
            id=self.internal_sid,
            external_ids={PRIMARY_EXTERNAL_ORG: self.external_sid},
            type=self.sample_type,
            meta=self.meta,
            active=True,
            sequencing_groups=[sg.to_sm() for sg in self.sequencing_groups],
            nested_samples=[s.to_sm() for s in (self.samples or [])],
        )

    @staticmethod
    def get_all_samples_from(samples: list['ParsedSample']) -> list['ParsedSample']:
        """Get all samples (including nested) from this list of samples"""
        all_samples = []
        for sample in samples:
            all_samples.append(sample)
            all_samples.extend(sample.get_all_samples_from(sample.samples))
        return all_samples

    def all_nested_samples(self) -> list['ParsedSample']:
        """Get nested samples from self"""
        if not self.samples:
            return []

        nested_samples = []
        for sample in self.samples:
            nested_samples.append(sample)
            nested_samples.extend(sample.all_nested_samples())

        return nested_samples


class ParsedSequencingGroup:
    """Class for holding sequencing group metadata"""

    def __init__(
        self,
        sample: ParsedSample,
        rows: GroupedRow,
        internal_seqgroup_id: int | None,
        external_seqgroup_id: str | None,
        sequencing_type: str,
        sequencing_technology: str,
        sequencing_platform: str | None,
        meta: dict[str, Any] | None,
    ):
        self.sample = sample
        self.rows = rows

        self.internal_seqgroup_id = internal_seqgroup_id
        self.external_seqgroup_id = external_seqgroup_id
        self.sequencing_type = sequencing_type
        self.sequencing_technology = sequencing_technology
        self.sequencing_platform = sequencing_platform
        self.meta = meta

        self.assays: list[ParsedAssay] = []
        self.analyses: list[ParsedAnalysis] = []

    def to_sm(self) -> SequencingGroupUpsert:
        """Convert to SM upsert model"""
        return SequencingGroupUpsert(
            type=self.sequencing_type,
            technology=self.sequencing_technology,
            platform=self.sequencing_platform,
            meta=self.meta,
            assays=[a.to_sm() for a in self.assays or []],
            id=self.internal_seqgroup_id,
        )


class ParsedAssay:
    """Parsed assay object, internal to parsers"""

    def __init__(
        self,
        group: ParsedSequencingGroup,
        rows: GroupedRow,
        internal_assay_id: int | None,
        external_assay_ids: dict[str, str],
        assay_type: str | None,
        meta: dict[str, Any] | None,
    ):
        self.sequencing_group = group
        self.rows = rows

        self.internal_id = internal_assay_id
        self.external_ids = external_assay_ids
        self.assay_type = assay_type
        self.meta: dict[str, Any] = meta or {}

    def to_sm(self) -> AssayUpsert:
        """Convert to SM upsert model"""
        return AssayUpsert(
            id=self.internal_id,
            type=self.assay_type,
            external_ids=self.external_ids,
            # sample_id=self.s,
            meta=self.meta,
        )


class ParsedAnalysis:
    """Parsed Analysis, ready to create an entry in SM"""

    def __init__(
        self,
        sequencing_group: ParsedSequencingGroup,
        rows: GroupedRow,
        status: AnalysisStatus,
        type_: str,
        meta: dict,
        output: str | None,
    ):
        self.sequencing_group = sequencing_group
        self.rows = rows
        self.status = status
        self.type = type_
        self.meta = meta
        self.output = output

    def to_sm(self):
        """To SM model"""
        if not self.sequencing_group.internal_seqgroup_id:
            raise ValueError('Sequencing group ID must be filled in by now')
        return Analysis(
            status=AnalysisStatus(self.status),
            type=str(self.type),
            meta=self.meta,
            output=self.output,
            sequencing_group_ids=[self.sequencing_group.internal_seqgroup_id],
        )


class DefaultSequencing:
    """Groups default sequencing information"""

    def __init__(
        self,
        seq_type: str = 'genome',  # seq_type because `type` is a built-in
        technology: str = 'short-read',
        platform: str = 'illumina',
        facility: str = None,
        library: str = None,
    ):
        self.seq_type = seq_type
        self.technology = technology
        self.platform = platform
        self.facility = facility
        self.library = library


def chunk(iterable: Iterable[T], chunk_size=50) -> Iterator[List[T]]:
    """
    Chunk an iterable by yielding lists of `chunk_size`
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
    """Parser for ingesting rows of metadata"""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        path_prefix: Optional[str],
        search_paths: list[str],
        project: str,
        default_sample_type: str = None,
        default_sequencing: DefaultSequencing = DefaultSequencing(),
        default_read_end_type: str = None,
        default_read_length: str | int = None,
        default_analysis_type: str = None,
        default_analysis_status: str = 'completed',
        key_map: Dict[str, str] = None,
        required_keys: Set[str] = None,
        ignore_extra_keys=False,
        skip_checking_gcs_objects=False,
        verbose=True,
    ):
        self.path_prefix = path_prefix
        self.skip_checking_gcs_objects = skip_checking_gcs_objects
        self.verbose = verbose

        self.key_map = key_map
        self.required_keys = required_keys
        self.ignore_extra_keys = ignore_extra_keys

        if not project:
            raise ValueError('A metamist project is required')

        self.project = project

        self.default_sequencing = default_sequencing
        self.default_read_end_type: Optional[str] = default_read_end_type
        self.default_read_length: Optional[str] = default_read_length
        self.default_sample_type: Optional[str] = default_sample_type
        self.default_analysis_type: Optional[str] = default_analysis_type
        self.default_analysis_status: Optional[str] = default_analysis_status

        # gs specific
        self.default_bucket = None

        self._client = None
        self.bucket_clients: Dict[str, Any] = {}

        self.papi = ParticipantApi()
        self.sapi = SampleApi()
        self.asapi = AssayApi()

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
                    f'Cannot form full path to {filename!r} as '
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
        Returns a summary of the parsed records.
        """
        rows = await self.file_pointer_to_rows(
            file_pointer=file_pointer, delimiter=delimiter
        )
        return await self.from_json(rows, confirm, dry_run)

    async def from_json(self, rows, confirm=False, dry_run=False):
        """
        Asynchronously parse rows of data, adding chunks of participants, samples, sequencing groups, assays, and analyses.

        Groups rows of participants by their IDs. For each participant, group samples by their IDs.
        If no participants are present, groups samples by their IDs.
        For each sample, gets its sequencing groups by their keys. For each sequencing group, groups assays and analyses.
        """
        if not isinstance(rows, list):
            rows = [rows]

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

        sequencing_groups: list[ParsedSequencingGroup] = []
        for schunk in chunk(samples):
            seq_groups_for_chunk = await asyncio.gather(
                *map(self.get_sample_sequencing_groups, schunk)
            )

            for sample, seqgroups in zip(schunk, seq_groups_for_chunk):
                sample.sequencing_groups = seqgroups
                sequencing_groups.extend(seqgroups)

        assays: list[ParsedAssay] = []
        for sgchunk in chunk(sequencing_groups):
            assays_for_chunk = await asyncio.gather(
                *map(self.get_assays_from_group, sgchunk)
            )
            analyses_for_chunk = await asyncio.gather(
                *map(self.get_analyses_from_sequencing_group, sgchunk)
            )

            for sequencing_group, chunked_assays, analyses in zip(
                sgchunk, assays_for_chunk, analyses_for_chunk
            ):
                if not chunked_assays:
                    # mark for removal
                    sequencing_group.assays = None
                    continue
                sequencing_group.assays = chunked_assays
                assays.extend(chunked_assays)
                sequencing_group.analyses = analyses

        # remove sequencing groups with no assays
        sequencing_groups = [sg for sg in sequencing_groups if sg.assays]
        for sample in ParsedSample.get_all_samples_from(samples):
            sample.sequencing_groups = [
                sg for sg in sample.sequencing_groups if sg.assays
            ]

        # match assay ids after sequencing groups
        await self.match_assay_ids(assays)
        # match sequencing group ids after assays
        await self.match_sequencing_group_ids(sequencing_groups)

        summary = self.prepare_summary(participants, samples, sequencing_groups, assays)
        message = self.prepare_message(
            summary, participants, samples, sequencing_groups, assays
        )

        if dry_run:
            logger.info('Dry run, so returning without inserting / updating metadata')
            self.prepare_detail(samples)
            return summary, (participants if participants else samples)

        if confirm:
            resp = str(input(message + '\n\nConfirm (y): '))
            if resp.lower() != 'y':
                raise SystemExit()
        else:
            logger.info(message)

        if participants:
            result = await self.papi.upsert_participants_async(
                self.project,
                [p.to_sm() for p in participants],
            )
        else:
            result = await self.sapi.upsert_samples_async(
                self.project,
                [s.to_sm() for s in samples],
            )

        if self.verbose:
            logger.info(json.dumps(result, indent=2))
        else:
            self.prepare_detail(samples)

        return result

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
        """Given some file pointer + a delimiter, get a list of rows (dictionary)"""
        reader = self._get_dict_reader(file_pointer, delimiter=delimiter)
        return list(reader)

    @dataclasses.dataclass
    class ParserSummary:
        """Summary of what will be inserted / updated"""

        @dataclasses.dataclass
        class ParserElementSummary:
            """Container for an element summary"""

            insert: int
            update: int = 0

        participants: ParserElementSummary
        samples: ParserElementSummary
        sequencing_groups: ParserElementSummary
        assays: ParserElementSummary
        analyses: ParserElementSummary

    def prepare_summary(
        self,
        participants: list[ParsedParticipant],
        samples: list[ParsedSample],
        sequencing_groups: list[ParsedSequencingGroup],
        assays: list[ParsedAssay],
    ) -> 'ParserSummary':
        """
        From the parsed objects, prepare a summary of what will be inserted / updated
        """
        participants_to_insert = sum(1 for p in participants if not p.internal_pid)
        all_samples = ParsedSample.get_all_samples_from(samples)
        samples_to_insert = sum(1 for s in all_samples if not s.internal_sid)
        sgs_to_insert = sum(
            1 for sg in sequencing_groups if not sg.internal_seqgroup_id
        )
        assays_to_insert = sum(1 for sq in assays if not sq.internal_id)
        analyses_to_insert = sum(len(sg.analyses or []) for sg in sequencing_groups)
        summary = GenericParser.ParserSummary(
            participants=GenericParser.ParserSummary.ParserElementSummary(
                insert=participants_to_insert,
                update=len(participants) - participants_to_insert,
            ),
            samples=GenericParser.ParserSummary.ParserElementSummary(
                insert=samples_to_insert,
                update=len(all_samples) - samples_to_insert,
            ),
            sequencing_groups=GenericParser.ParserSummary.ParserElementSummary(
                insert=sgs_to_insert,
                update=len(sequencing_groups) - sgs_to_insert,
            ),
            assays=GenericParser.ParserSummary.ParserElementSummary(
                insert=assays_to_insert,
                update=len(assays) - assays_to_insert,
            ),
            analyses=GenericParser.ParserSummary.ParserElementSummary(
                insert=analyses_to_insert
            ),
        )

        return summary

    def prepare_detail(self, samples: list[ParsedSample]):
        """Uses tabulate to print a detailed summary of the samples being inserted / updated"""
        sample_participants = {}
        all_samples = ParsedSample.get_all_samples_from(samples)
        for sample in all_samples:
            sample_participants[sample.external_sid] = (
                sample.participant.external_pid if sample.participant else None
            )
        sample_sequencing_groups = {
            sample.external_sid: sample.sequencing_groups for sample in all_samples
        }

        details = []
        for sample, participant in sample_participants.items():
            for sg in sample_sequencing_groups[sample]:
                sg_details = {
                    'Participant': participant if participant else '',
                    'Sample': sample,
                    'Sequencing Type': sg.sequencing_type,
                    'Assays': sum(1 for a in sg.assays if not a.internal_id),
                }
                details.append(sg_details)

        headers = ['Participant', 'Sample', 'Sequencing Type', 'Assays']
        table = list(list(detail.values()) for detail in details)

        print(tabulate(table, headers=headers, tablefmt='grid'))

    def prepare_message(
        self,
        summary,
        participants: list[ParsedParticipant],
        samples: list[ParsedSample],
        sequencing_groups: list[ParsedSequencingGroup],
        assays: list[ParsedAssay],
    ) -> str:
        """From summary, prepare a string to log to the console"""
        if participants:
            external_participant_ids = ', '.join(
                set(p.external_pid for p in participants)
            )
            header = f'Processing participants: {external_participant_ids}'
        else:
            external_sample_ids = ', '.join(
                set(s.external_sid for s in ParsedSample.get_all_samples_from(samples))
            )
            header = f'Processing samples: {external_sample_ids}'

        assays_count: dict[str, int] = defaultdict(int)
        assays_types_count: dict[str, int] = defaultdict(int)
        sequencing_group_counts: dict[str, int] = defaultdict(int)
        for a in assays:
            assays_count[str(a.meta.get('sequencing_type'))] += 1
            assays_types_count[str(a.assay_type)] += 1
        for sg in sequencing_groups:
            sequencing_group_counts[str(sg.sequencing_type)] += 1

        str_assay_count = ', '.join(f'{k}={v}' for k, v in assays_count.items())
        str_assay_types_count = ', '.join(
            f'{k}={v}' for k, v in assays_types_count.items()
        )
        str_seqg_count = ', '.join(
            f'{k}={v}' for k, v in sequencing_group_counts.items()
        )

        message = f"""\


                {self.project}: {header}

                Assays count: {str_assay_count}
                Assays types count: {str_assay_types_count}
                Sequencing group count: {str_seqg_count}

                Adding {summary.participants.insert} participants
                Adding {summary.samples.insert} samples
                Adding {summary.sequencing_groups.insert} sequencing groups
                Adding {summary.assays.insert} assays
                Adding {summary.analyses.insert} analyses

                Updating {summary.participants.update} participants
                Updating {summary.samples.update} samples
                Updating {summary.sequencing_groups.update} sequencing groups
                Updating {summary.assays.update} assays
                """
        return message

    # region MATCHING

    async def match_participant_ids(self, participants: list[ParsedParticipant]):
        """
        Determine if a participant is NEW or UPDATE, and match the ID if so.
        Participants only match on external_id
        """

        values = await query_async(
            QUERY_MATCH_PARTICIPANTS, variables={'project': self.project}
        )
        pid_map = {p['externalId']: p['id'] for p in values['project']['participants']}

        for participant in participants:
            participant.internal_pid = pid_map.get(participant.external_pid)

    async def match_sample_ids(self, samples: list[ParsedSample]):
        """
        Determine if a sample is NEW or UPDATE, and match the ID if so.
        Only matches based on the external ID
        """

        values = await query_async(
            QUERY_MATCH_SAMPLES, variables={'project': self.project}
        )
        sid_map = {p['externalId']: p['id'] for p in values['project']['samples']}

        for sample in ParsedSample.get_all_samples_from(samples):
            sample.internal_sid = sid_map.get(sample.external_sid)

    async def match_sequencing_group_ids(
        self, sequencing_groups: list[ParsedSequencingGroup]
    ):
        """
        Determine if sequencing groups are NEW, or UPDATE, and match the ID if so.
        **sequencing_groups MUST have assays already attached.**

        This one is a little more tricky, because we won't have a direct mapping.
        We're only allowed to bind the ID if:
         - All the assays already exist
         - The group members match
        Otherwise we should leave the ID blank (forcing a create).
        """

        if not all(sg.assays for sg in sequencing_groups):
            raise ValueError('sequencing_groups must have assays attached')

        values = await query_async(
            QUERY_MATCH_SEQUENCING_GROUPS, variables={'project': self.project}
        )
        sg_map = {
            tuple(sorted(a['id'] for a in sg['assays'])): sg['id']
            for sg in values['project']['sequencingGroups']
        }
        for sg in sequencing_groups:
            sg_ids = [a.internal_id for a in sg.assays]
            # don't match if any of the assays are missing
            if any(asid is None for asid in sg_ids):
                continue
            # matches only if they all exist!
            sorted_sg_ids = tuple(sorted(sg_ids))
            sg.internal_seqgroup_id = sg_map.get(sorted_sg_ids)

    async def match_assay_ids(self, assays: list[ParsedAssay]):
        """
        Determine if assays are NEW, or UPDATE, and match the ID if so.
        This works based on the filenames of the reads.
        """

        values = await query_async(
            QUERY_MATCH_ASSAYS, variables={'project': self.project}
        )

        assay_eid_map = {
            external_id: assay['id']
            for sample in values['project']['samples']
            for assay in sample['assays']
            for external_id in assay['externalIds'].values()
        }

        # map filenames of reads to assay IDs as that's the most likely way we'll map
        def reads_to_key(reads):
            if isinstance(reads, list):
                return tuple(sorted(map(reads_to_key, reads)))
            if isinstance(reads, dict):
                return reads['location']
            if isinstance(reads, str):
                raise TypeError(f'Unformmatted reads (expected file object): {reads}')
            raise ValueError(f'Unknown type {reads}')

        filename_meta_map = {
            reads_to_key(assay['meta']['reads']): assay['id']
            for sample in values['project']['samples']
            for assay in sample['assays']
            if assay['meta'].get('reads')
        }

        def _map_assay(assay: ParsedAssay):
            # put it in a function so we can return early

            # external IDs match
            for exid in (assay.external_ids or {}).values():
                if exid in assay_eid_map:
                    return assay_eid_map.get(exid)

            # reads match
            if assay.meta.get('reads'):
                key = reads_to_key(assay.meta['reads'])
                if key in filename_meta_map:
                    return filename_meta_map.get(key)

            return None

        for assay in assays:
            assay.internal_id = _map_assay(assay)

        return assays

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
    def get_assay_id(self, row: GroupedRow) -> Optional[dict[str, str]]:
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
        """
        From a set of rows, group (by calling self.get_participant_meta)
        and parse participant other participant values.
        """

        participant_groups: list[ParsedParticipant] = []
        pgroups = group_by(rows, self.get_participant_id)
        for pid, prows in pgroups.items():
            participant_groups.append(
                ParsedParticipant(
                    internal_pid=None,
                    external_pid=pid,
                    rows=prows,
                    meta=await self.get_participant_meta_from_group(prows),
                    reported_sex=self.get_reported_sex(prows),
                    reported_gender=self.get_reported_gender(prows),
                    karyotype=self.get_karyotype(prows),
                )
            )

        return participant_groups

    async def get_participant_meta_from_group(self, rows: GroupedRow) -> dict:
        """From a list of rows, get any relevant participant meta"""
        return {}

    async def group_samples(
        self, participant: ParsedParticipant | None, rows: GroupedRow
    ) -> list[ParsedSample]:
        """
        From a set of rows, group (by calling self.get_sample_id)
        and parse samples and their values.
        """
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
        """From a list of rows, get any relevant sample meta"""
        return {}

    def get_sequencing_group_key(self, row: SingleRow) -> Hashable:
        """
        Get a key to group sequencing group rows by.
        """
        if seq_group_id := self.get_sequencing_group_id(row):
            return seq_group_id

        keys = [
            ('sequencing_type', str(self.get_sequencing_type(row))),
            ('sequencing_technology', str(self.get_sequencing_technology(row))),
            ('sequencing_platform', str(self.get_sequencing_platform(row))),
        ]

        invalid_keys = [k for k, v in keys if v is None]
        if invalid_keys:
            raise ValueError(f'Invalid sequencing group key: {invalid_keys}')

        return tuple(v for _, v in keys)

    async def get_sample_sequencing_groups(
        self, sample: ParsedSample
    ) -> list[ParsedSequencingGroup]:
        """
        From a set of samples, group (by calling self.get_sequencing_group_key)
        and parse sequencing groups and their values.
        """
        sequencing_groups = []
        for seq_rows in group_by(sample.rows, self.get_sequencing_group_key).values():
            seq_type = self.get_sequencing_type(seq_rows[0])
            seq_tech = self.get_sequencing_technology(seq_rows[0])
            seq_platform = self.get_sequencing_platform(seq_rows[0])

            seq_group = ParsedSequencingGroup(
                internal_seqgroup_id=None,
                external_seqgroup_id=self.get_sequencing_group_id(seq_rows[0]),
                sequencing_type=seq_type,
                sequencing_technology=seq_tech,
                sequencing_platform=seq_platform,
                meta={},
                sample=sample,
                rows=seq_rows,
            )

            seq_group.meta = await self.get_sequencing_group_meta(seq_group)
            sequencing_groups.append(seq_group)

        return sequencing_groups

    async def get_sequencing_group_meta(
        self, sequencing_group: ParsedSequencingGroup
    ) -> dict:
        """
        From a list of rows, get any relevant sequencing group meta
        """
        return {}

    async def get_analyses_from_sequencing_group(
        self, sequencing_group: ParsedSequencingGroup
    ) -> list[ParsedAnalysis]:
        """
        An override that allows a subclass to return a list of analyses
        for a sequencing group.
        """
        return []

    @abstractmethod
    async def get_assays_from_group(
        self, sequencing_group: ParsedSequencingGroup
    ) -> list[ParsedAssay]:
        """
        From a sequencing_group (list of rows with some common seq fields),
        return list[ParsedAssay] (does not have to equal number of rows).
        """

    def get_sequencing_group_meta_from_assays(self, assays: list[ParsedAssay]) -> dict:
        """
        From a list of assays, get any relevant sequencing group meta
        """
        meta = {}
        for assay in assays:
            if assay.meta.get('sequencing_type') == 'exome':
                keys = ('sequencing_facility', 'sequencing_library')
            elif assay.meta.get('sequencing_type') in RNA_SEQ_TYPES:
                keys = (
                    'sequencing_facility',
                    'sequencing_library',
                    'read_end_type',
                    'read_length',
                )
            elif assay.meta.get('sequencing_technology') == 'long-read':
                # lift all assay meta into the sequencing group meta for long-read
                # except for assay reads, and keys that are already top-level sequencing group fields
                keys_to_avoid = (
                    'reads',
                    'reads_type',
                    'sequencing_type',
                    'sequencing_technology',
                    'sequencing_platform',
                )
                keys = [k for k in assay.meta.keys() if k not in keys_to_avoid]
            else:
                continue
            for key in keys:
                if assay.meta.get(key) is not None:
                    meta[key] = assay.meta[key]
        return meta

    def get_sample_type(self, row: GroupedRow) -> str:
        """Get sample type from row"""
        return self.default_sample_type

    def get_sequencing_group_id(self, row: SingleRow) -> str | None:
        """
        External sequencing_group identifier. Odds are you don't want this.
            Unless you have a "library ID" or something similar"

        There are few cases where the collaborator actually generates a sequencing
        group identifier (more of a CPG concept), but you can probably proxy it.
        """
        return None

    def get_sequencing_type(self, row: SingleRow) -> str:
        """Get sequence types from row"""
        return self.default_sequencing.seq_type

    def get_sequencing_technology(self, row: SingleRow) -> str:
        """Get sequencing technology from row"""
        return self.default_sequencing.technology

    def get_sequencing_platform(self, row: SingleRow) -> str | None:
        """Get sequencing platform from row"""
        return self.default_sequencing.platform

    def get_sequencing_facility(self, row: SingleRow) -> str | None:
        """Get sequencing facility from row"""
        return self.default_sequencing.facility

    def get_sequencing_library(self, row: SingleRow) -> str | None:
        """Get library type from row"""
        return self.default_sequencing.library

    def get_read_end_type(self, row: SingleRow) -> str | None:
        """Get read end type from row"""
        return self.default_read_end_type

    def get_read_length(self, row: SingleRow) -> str | None:
        """Get read length from row"""
        return self.default_read_length

    def get_analysis_type(self, sample_id: str, row: GroupedRow) -> str:
        """Get analysis type from row"""
        return str(self.default_analysis_type)

    def get_analysis_status(self, sample_id: str, row: GroupedRow) -> AnalysisStatus:
        """Get analysis status from row"""
        return AnalysisStatus(self.default_analysis_status)

    def get_existing_external_sequence_ids(
        self, participant_map: Dict[str, Dict[Any, List[Any]]]
    ):
        """Pulls external sequence IDs from participant map"""
        external_sequence_ids: list[str] = []
        for participant in participant_map:
            for sample in participant_map[participant]:
                for sequence in participant_map[participant][sample]:
                    external_sequence_ids.append((sequence.get('Sequence ID')))

        return external_sequence_ids

    @staticmethod
    def get_existing_assays(
        assays: list[dict[str, Any]], external_sequence_ids: list[str]
    ):
        """Accounts for external_sequence_ids when determining which assays
        need to be updated vs inserted"""

        existing_assays: list[dict[str, Any]] = []
        for seq in assays:
            if not seq['external_ids'].values():
                # No existing sequence ID, we can assume that replacement should happen
                # Note: This means that you can't have a mix of assays with and without
                # external sequence IDs in one dataset.
                existing_assays.append(seq)

            else:
                for ext_id in seq['external_ids'].values():
                    # If the external ID is already there, we want to upsert.
                    if ext_id in external_sequence_ids:
                        existing_assays.append(seq)

        return existing_assays

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
                # TODO: resolve this external_to_internal_id_map
                # this one is going to be slightly harder :
                analysis.sequencing_group_ids = [
                    external_to_internal_id_map[external_id]
                ]
                promises.append(
                    analysisapi.create_analysis_async(
                        project=proj, analysis_model=analysis
                    )
                )
            results.append(await asyncio.gather(*promises))

        return results

    async def parse_files(
        self, sample_id: str, reads: list[str] | str, checksums: List[str] = None
    ) -> Dict[SUPPORTED_FILE_TYPE, Dict[str, List]]:
        """
        Returns a tuple of:
        1. single / list-of CWL file object(s), based on the extensions of the reads
        2. parsed type (fastq, cram, bam)
        """
        _reads: list[str]
        if not isinstance(reads, list):
            _reads = [reads]
        else:
            _reads = reads

        if not checksums:
            checksums = [None] * len(_reads)

        if len(checksums) != len(_reads):
            raise ValueError(
                'Expected length of reads to match length of provided checksums'
            )

        read_to_checksum: dict[str, str | None] = dict(zip(_reads, checksums))

        file_by_type: Dict[SUPPORTED_FILE_TYPE, Dict[str, List]] = defaultdict(
            lambda: defaultdict(list)
        )

        fastqs = [
            r
            for r in _reads
            if any(r.lower().endswith(ext) for ext in FASTQ_EXTENSIONS)
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
            r for r in _reads if any(r.lower().endswith(ext) for ext in CRAM_EXTENSIONS)
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
            r for r in _reads if any(r.lower().endswith(ext) for ext in BAM_EXTENSIONS)
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
            r for r in _reads if any(r.lower().endswith(ext) for ext in GVCF_EXTENSIONS)
        ]
        vcfs = [
            r
            for r in _reads
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
            for r in _reads
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

        >>> GenericParser.parse_fastqs_structure(['21R2112345-20210326-A00123_S70_L001_R1_001.fastq.gz', '21R2112345-20210326-A00123_S70_L001_R2_001.fastq.gz'])
        [['21R2112345-20210326-A00123_S70_L001_R1_001.fastq.gz', '21R2112345-20210326-A00123_S70_L001_R2_001.fastq.gz']]

        >>> GenericParser.parse_fastqs_structure(['ACG0xx_2_1.fastq.gz', 'ACG0xx_2_2.fastq.gz', 'ACG0xx_3_1.fastq.gz', 'ACG0xx_3_2.fastq.gz'])
        [['ACG0xx_2_1.fastq.gz', 'ACG0xx_2_2.fastq.gz'], ['ACG0xx_3_1.fastq.gz', 'ACG0xx_3_2.fastq.gz']]

        >>> GenericParser.parse_fastqs_structure(['21R2112345-20210326-A00123_S70_L001_R1_001.fastq.gz', '21R2112345-20210326-A00123_S70_L001_R2_001.fastq.gz', '21R2112345-20210326-A00123_S70_L001_R1_002.fastq.gz', '21R2112345-20210326-A00123_S70_L001_R2_002.fastq.gz'])
        [['21R2112345-20210326-A00123_S70_L001_R1_001.fastq.gz', '21R2112345-20210326-A00123_S70_L001_R2_001.fastq.gz'], ['21R2112345-20210326-A00123_S70_L001_R1_002.fastq.gz', '21R2112345-20210326-A00123_S70_L001_R2_002.fastq.gz']]

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
                f"Couldn't detect the format of FASTQs (expected match for regex {rmatch.pattern!r}): {no_r_match_str!r}"
            )

        # Create a dict with filenames as keys and prefixes and suffixes as values
        fastq_groups = defaultdict(list)
        for full_filename, (basename, matched) in r_matches.items():
            # use only file path basename to define prefix first
            pre_r_basename = basename[: matched.end()]
            bits_to_group_on = [pre_r_basename]
            groups = matched.groups()
            # group fasts based on the regex groups 1 / 2
            for i in (1, 2):
                # index 1: optional _001 group. index 2: file extension
                bits_to_group_on.append(groups[i])

            fastq_groups[tuple(bits_to_group_on)].append(full_filename)

        invalid_fastq_groups = [grp for grp in fastq_groups.values() if len(grp) != 2]
        if invalid_fastq_groups:
            # TODO: implement handling for single-ended reads
            raise ValueError(f'Invalid fastq group {invalid_fastq_groups}')

        sorted_groups = sorted(
            (sorted(fastqgroup) for fastqgroup in fastq_groups.values()),
            key=lambda el: os.path.basename(el[0]),
        )

        return sorted_groups

    async def create_file_object(
        self,
        filename: str,
        secondary_files: List[SingleRow] = None,
        checksum: Optional[str] = None,
    ) -> SingleRow:
        """Takes filename, returns formed CWL dictionary"""
        _checksum = checksum
        file_size = None
        datetime_added = None

        if not self.skip_checking_gcs_objects:
            if not _checksum:
                md5_filename = self.file_path(filename + '.md5')
                if await self.file_exists(md5_filename):
                    contents = await self.file_contents(md5_filename)
                    if contents:
                        _checksum = f'md5:{contents.strip()}'

            file_size, datetime_added = await asyncio.gather(
                self.file_size(filename), self.datetime_added(filename)
            )

        d = {
            'location': self.file_path(filename),
            'basename': os.path.basename(filename),
            'class': 'File',
            'checksum': _checksum,
            'size': file_size,
            'datetime_added': datetime_added.isoformat() if datetime_added else None,
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
                    f'Guessing delimiter based on first line, got {delimiter!r}'
                )
                return delimiter

        raise ValueError(f'Unrecognised extension on file: {filename}')


def group_fastqs_by_common_filename_components(d: dict) -> dict:
    """Groups key-value pairs by common prefix components and suffix components
    Where the key is the fastq filename and the value is a tuple of the filename split at R1/R2/1/2

    Returns a dict with grouping term constructed from extracted prefix+suffix identifiers as key
    and a list of filenames matching the group as value.
    Sample input: {
        'S70_L001_R1_001.fastq.gz': ('S70_L001_', 'R1_001.fastq.gz'),
        'S70_L001_R2_001.fastq.gz': ('S70_L001_', 'R2_001.fastq.gz'),
        'S70_L001_R1.fastq.gz': ('S70_L001_', 'R1.fastq.gz'),
        'S70_L001_R2.fastq.gz': ('S70_L001_', 'R1.fastq.gz')
        }
    Sample output: {
        'S70_L001_001.fastq.gz': ['S70_L001_R1_001.fastq.gz', 'S70_L001_R2_001.fastq.gz'],
        'S70_L001_fastq.gz': ['S70_L001_R1.fastq.gz', 'S70_L001_R2.fastq.gz']
        }
    """

    def key_selector(kv: tuple[str, str]) -> tuple[str, str]:
        _, fastq_file_components = kv
        fastq_suffix = fastq_file_components[1]
        try:
            common_component = fastq_suffix.split('_')[
                1
            ]  # get the component of the file suffix after the first underscore if there
        except IndexError:
            common_component = fastq_suffix.split('.', 1)[
                1
            ]  # or get the file extension if there is no trailing underscore after R1/R2

        return fastq_suffix, common_component

    grouping = group_by(d.items(), key_selector)

    fastq_groups = defaultdict(list)
    for suffixes, matches in grouping.items():
        for match in matches:
            common_components = os.path.basename(match[1][0]) + suffixes[1]
            fastq_groups[common_components].append(match[0])

    return fastq_groups


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
