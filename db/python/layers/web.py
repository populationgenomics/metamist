# pylint: disable=too-many-locals, too-many-instance-attributes
import json
import asyncio
import dataclasses
from datetime import date
from collections import defaultdict
from typing import Dict, List, Optional, Set

from enum import Enum
from pydantic import BaseModel

from api.utils import group_by

from db.python.connect import DbBase
from db.python.layers.base import BaseLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.seqr import SeqrLayer
from db.python.tables.analysis import AnalysisTable
from db.python.tables.project import ProjectPermissionsTable
from db.python.tables.sequence import SampleSequencingTable
from models.base import SMBase

from models.enums import SampleType, SequenceType, SequenceStatus, SequenceTechnology


class MetaSearchEntityPrefix(Enum):
    """Links prefixes to tables"""

    PARTICIPANT = 'p'
    SAMPLE = 's'
    SEQUENCE = 'sq'
    FAMILY = 'f'


class SearchItem(SMBase):
    """Summary Grid Filter Model"""

    model_type: MetaSearchEntityPrefix
    query: str
    field: str
    is_meta: bool


class NestedSequence(BaseModel):
    """Sequence model"""

    id: int
    type: SequenceType
    status: SequenceStatus
    technology: SequenceTechnology
    meta: Dict


class NestedSample(BaseModel):
    """Sample with nested sequences"""

    id: str
    external_id: str
    type: SampleType
    meta: Dict
    sequences: List[NestedSequence]
    created_date: Optional[str]


class NestedFamily(BaseModel):
    """Simplified family model"""

    id: int
    external_id: str


class NestedParticipant(BaseModel):
    """Participant with nested family and sampels"""

    id: Optional[int]
    external_id: Optional[str]
    meta: Optional[Dict]
    families: List[NestedFamily]
    samples: List[NestedSample]
    reported_sex: Optional[int]
    reported_gender: Optional[str]
    karyotype: Optional[str]


@dataclasses.dataclass
class WebProject:
    """Return class for Project, minimal fields"""

    id: int
    name: str
    dataset: str
    meta: dict


@dataclasses.dataclass
class ProjectSummary:
    """Return class for the project summary endpoint"""

    project: WebProject

    # stats
    total_samples: int
    total_samples_in_query: int
    total_participants: int
    total_sequences: int
    cram_seqr_stats: dict[str, dict[str, str]]
    batch_sequence_stats: dict[str, dict[str, str]]

    # grid
    participants: List[NestedParticipant]
    participant_keys: list[tuple[str, str]]
    sample_keys: list[tuple[str, str]]
    sequence_keys: list[tuple[str, str]]

    # seqr
    seqr_links: dict[str, str]
    seqr_sync_types: list[SequenceType]


class WebLayer(BaseLayer):
    """Web layer"""

    async def get_project_summary(
        self,
        grid_filter: list[SearchItem],
        token: Optional[int],
        limit: int = 20,
    ) -> ProjectSummary:
        """
        Get a summary of a project, allowing some "after" token,
        and limit to the number of results.
        """
        webdb = WebDb(self.connection)
        return await webdb.get_project_summary(
            grid_filter=grid_filter, token=token, limit=limit
        )


class WebDb(DbBase):
    """Db layer for web related routes,"""

    def _project_summary_sample_query(self, grid_filter: List[SearchItem]):
        """
        Get query for getting list of samples
        """
        wheres = ['s.project = :project']
        values = {'project': self.project}
        where_str = ''
        for query in grid_filter:
            value = query.query
            field = query.field
            # prefix = MetaSearchEntityPrefix(query.model_type).name
            prefix = query.model_type.value
            key = (
                f'{query.model_type}_{field}_{value}'.replace('-', '_')
                .replace('.', '_')
                .replace(':', '_')
                .replace(' ', '_')
            )
            if not query.is_meta:
                q = f'{prefix}.{field} LIKE :{key}'
            else:
                q = f'JSON_VALUE({prefix}.meta, "$.{field}") LIKE :{key}'
            wheres.append(q)
            values[key] = self.escape_like_term(value) + '%'
        if wheres:
            where_str = 'WHERE ' + ' AND '.join(wheres)

        # Skip 'limit' and 'after' SQL commands so we can get all samples that match the query to determine
        # the total count, then take the selection of samples for the current page.
        # This is more efficient than doing 2 queries separately
        sample_query = f"""
        SELECT s.id, s.external_id, s.type, s.meta, s.participant_id
        FROM sample s
        LEFT JOIN sample_sequencing sq ON s.id = sq.sample_id
        LEFT JOIN participant p ON p.id = s.participant_id
        LEFT JOIN family_participant fp on s.participant_id = fp.participant_id
        LEFT JOIN family f ON f.id = fp.family_id
        {where_str}
        GROUP BY id
        ORDER BY id
        """
        return sample_query, values

    @staticmethod
    def _project_summary_process_sequence_rows_by_sample_id(
        sequence_rows,
    ) -> Dict[int, List[NestedSequence]]:
        """
        Get sequences for samples for project summary
        """

        seq_id_to_sample_id_map = {seq['id']: seq['sample_id'] for seq in sequence_rows}
        seq_models = [
            NestedSequence(
                id=seq['id'],
                status=SequenceStatus(seq['status']),
                type=SequenceType(seq['type']),
                meta=json.loads(seq['meta']),
                technology=SequenceTechnology(seq['technology'])
                if seq['technology']
                else None,
            )
            for seq in sequence_rows
        ]
        seq_models_by_sample_id = group_by(
            seq_models, lambda s: seq_id_to_sample_id_map[s.id]
        )

        return seq_models_by_sample_id

    @staticmethod
    def _project_summary_process_sample_rows(
        sample_rows, seq_models_by_sample_id, sample_id_start_times: Dict[int, date]
    ) -> List[NestedSample]:
        """
        Process the returned sample rows into nested samples + sequences
        """

        smodels = [
            NestedSample(
                id=s['id'],
                external_id=s['external_id'],
                type=s['type'],
                meta=json.loads(s['meta']) or {},
                created_date=str(sample_id_start_times.get(s['id'], '')),
                sequences=seq_models_by_sample_id.get(s['id'], []) or [],
            )
            for s in sample_rows
        ]
        return smodels

    async def get_total_number_of_samples(self):
        """Get total number of active samples within a project"""
        _query = 'SELECT COUNT(*) FROM sample WHERE project = :project AND active'
        return await self.connection.fetch_val(_query, {'project': self.project})

    async def get_total_number_of_participants(self):
        """Get total number of participants within a project"""
        _query = 'SELECT COUNT(*) FROM participant WHERE project = :project'
        return await self.connection.fetch_val(_query, {'project': self.project})

    async def get_total_number_of_sequences(self):
        """Get total number of sequences within a project"""
        _query = 'SELECT COUNT(*) FROM sample_sequencing sq INNER JOIN sample s ON s.id = sq.sample_id WHERE s.project = :project'
        return await self.connection.fetch_val(_query, {'project': self.project})

    @staticmethod
    def _project_summary_process_family_rows_by_pid(
        family_rows,
    ) -> Dict[int, List[NestedFamily]]:
        """
        Process the family rows into NestedFamily objects
        """
        pid_to_fids = defaultdict(list)
        for frow in family_rows:
            pid_to_fids[frow['participant_id']].append(frow['family_id'])

        res_families = {}
        for f in family_rows:
            if f['family_id'] in family_rows:
                continue
            res_families[f['family_id']] = NestedFamily(
                id=f['family_id'], external_id=f['external_family_id']
            )
        pid_to_families = {
            pid: [res_families[fid] for fid in fids]
            for pid, fids in pid_to_fids.items()
        }
        return pid_to_families

    def get_seqr_links_from_project(self, project: WebProject) -> dict[str, str]:
        """
        From project.meta, select our project guids and form seqr links
        """
        if not project.meta.get('is_seqr', False):
            return {}

        seqr_links = {}
        for seqtype in SequenceType:
            key = f'seqr-project-{seqtype.value}'
            if guid := project.meta.get(key):
                seqr_links[seqtype.value] = SeqrLayer.get_seqr_link_from_guid(guid)

        return seqr_links

    async def get_project_summary(
        self,
        grid_filter: List[SearchItem],
        limit: int,
        token: Optional[int] = 0,
    ) -> ProjectSummary:
        """
        Get project summary

        :param token: for PAGING
        :param limit: Number of SAMPLEs to return, not including nested sequences
        """
        # do initial query to get sample info
        sampl = SampleLayer(self._connection)
        sample_query, values = self._project_summary_sample_query(grid_filter)
        ptable = ProjectPermissionsTable(self.connection)
        project_db = await ptable.get_project_by_id(self.project)
        project = WebProject(
            id=project_db.id,
            name=project_db.name,
            meta=project_db.meta,
            dataset=project_db.dataset,
        )
        seqr_links = self.get_seqr_links_from_project(project)

        # This retrieves all samples that match the current query
        # This is not currently a problem as no projects are even close to 10000 rows
        # So won't be causing any memory/resource issues
        # Could be optimised in future by limiting to 10k if necessary
        sample_rows_all = list(await self.connection.fetch_all(sample_query, values))
        total_samples_in_query = len(sample_rows_all)
        sample_rows = sample_rows_all[token : token + limit]

        if len(sample_rows) == 0:
            return ProjectSummary(
                project=project,
                participants=[],
                participant_keys=[],
                sample_keys=[],
                sequence_keys=[],
                # stats
                total_samples=0,
                total_samples_in_query=0,
                total_participants=0,
                total_sequences=0,
                batch_sequence_stats={},
                cram_seqr_stats={},
                seqr_links=seqr_links,
                seqr_sync_types=[],
            )

        pids = list(set(s['participant_id'] for s in sample_rows))
        sids = list(s['id'] for s in sample_rows)

        # sequences

        seq_query = 'SELECT id, sample_id, meta, type, status, technology FROM sample_sequencing WHERE sample_id IN :sids'
        sequence_promise = self.connection.fetch_all(seq_query, {'sids': sids})

        # participant
        p_query = 'SELECT id, external_id, meta, reported_sex, reported_gender, karyotype FROM participant WHERE id in :pids'
        participant_promise = self.connection.fetch_all(p_query, {'pids': pids})

        # family
        f_query = """
SELECT f.id as family_id, f.external_id as external_family_id, fp.participant_id
FROM family_participant fp
INNER JOIN family f ON f.id = fp.family_id
WHERE fp.participant_id in :pids
        """
        family_promise = self.connection.fetch_all(f_query, {'pids': pids})

        atable = AnalysisTable(self._connection)
        seqtable = SampleSequencingTable(self._connection)

        [
            sequence_rows,
            participant_rows,
            family_rows,
            sample_id_start_times,
            total_samples,
            total_participants,
            total_sequences,
            cram_number_by_seq_type,
            seq_number_by_seq_type,
            seq_number_by_seq_type_and_batch,
            seqr_stats_by_seq_type,
            seqr_sync_types,
        ] = await asyncio.gather(
            sequence_promise,
            participant_promise,
            family_promise,
            sampl.get_samples_create_date(sids),
            self.get_total_number_of_samples(),
            self.get_total_number_of_participants(),
            self.get_total_number_of_sequences(),
            atable.get_number_of_crams_by_sequence_type(project=self.project),
            seqtable.get_sequence_type_numbers_for_project(project=self.project),
            seqtable.get_sequence_type_numbers_by_batch_for_project(
                project=self.project
            ),
            atable.get_seqr_stats_by_sequence_type(project=self.project),
            SeqrLayer(self._connection).get_synchronisable_types(self.project),
        )

        # post-processing
        seq_models_by_sample_id = (
            self._project_summary_process_sequence_rows_by_sample_id(sequence_rows)
        )
        smodels = self._project_summary_process_sample_rows(
            sample_rows, seq_models_by_sample_id, sample_id_start_times
        )
        # the pydantic model is casting to the id to a str, as that makes sense on the front end
        # but cast back here to do the lookup
        sid_to_pid = {s['id']: s['participant_id'] for s in sample_rows}
        smodels_by_pid = group_by(smodels, lambda s: sid_to_pid[int(s.id)])

        pid_to_families = self._project_summary_process_family_rows_by_pid(family_rows)
        participant_map = {p['id']: p for p in participant_rows}

        # we need to specifically handle the empty participant case,
        # we'll accomplish this using an hash set

        pid_seen = set()
        pmodels = []

        for s, srow in zip(smodels, sample_rows):
            pid = srow['participant_id']
            if pid is None:
                pmodels.append(
                    NestedParticipant(
                        id=None,
                        external_id=None,
                        meta=None,
                        families=[],
                        samples=[s],
                        reported_sex=None,
                        reported_gender=None,
                        karyotype=None,
                    )
                )
            elif pid not in pid_seen:
                pid_seen.add(pid)
                p = participant_map[pid]
                pmodels.append(
                    NestedParticipant(
                        id=p['id'],
                        external_id=p['external_id'],
                        meta=json.loads(p['meta']),
                        families=pid_to_families.get(p['id'], []),
                        samples=list(smodels_by_pid.get(p['id'])),
                        reported_sex=p['reported_sex'],
                        reported_gender=p['reported_gender'],
                        karyotype=p['karyotype'],
                    )
                )

        ignore_participant_keys: Set[str] = set()
        ignore_sample_meta_keys = {'reads', 'vcfs', 'gvcf'}
        ignore_sequence_meta_keys = {'reads', 'vcfs', 'gvcf'}

        participant_meta_keys = set(
            pk
            for p in pmodels
            if p and p.meta
            for pk in p.meta.keys()
            if pk not in ignore_participant_keys
        )
        sample_meta_keys = set(
            sk
            for p in pmodels
            for s in p.samples
            for sk in s.meta.keys()
            if (sk not in ignore_sample_meta_keys)
        )
        sequence_meta_keys = set(
            sk
            for p in pmodels
            for s in p.samples
            for seq in s.sequences
            for sk in seq.meta
            if (sk not in ignore_sequence_meta_keys)
        )

        has_reported_sex = any(p.reported_sex for p in pmodels)
        has_reported_gender = any(p.reported_gender for p in pmodels)
        has_karyotype = any(p.karyotype for p in pmodels)

        participant_keys = [('external_id', 'Participant ID')]

        if has_reported_sex:
            participant_keys.append(('reported_sex', 'Reported sex'))
        if has_reported_gender:
            participant_keys.append(('reported_gender', 'Reported gender'))
        if has_karyotype:
            participant_keys.append(('karyotype', 'Karyotype'))

        participant_keys.extend(('meta.' + k, k) for k in participant_meta_keys)
        sample_keys: list[tuple[str, str]] = [
            ('id', 'Sample ID'),
            ('external_id', 'External Sample ID'),
            ('created_date', 'Created date'),
        ] + [('meta.' + k, k) for k in sample_meta_keys]
        sequence_keys = [('type', 'type'), ('technology', 'technology')] + sorted(
            [('meta.' + k, k) for k in sequence_meta_keys]
        )

        seen_seq_types = set(cram_number_by_seq_type.keys()).union(
            set(seq_number_by_seq_type.keys())
        )
        seen_batches = set(seq_number_by_seq_type_and_batch.keys())

        sequence_stats: Dict[str, Dict[str, str]] = {}
        cram_seqr_stats = {}

        for seq in seen_seq_types:
            cram_seqr_stats[seq] = {
                'Sequences': str(seq_number_by_seq_type.get(seq, 0)),
                'Crams': str(cram_number_by_seq_type.get(seq, 0)),
                'Seqr': str(seqr_stats_by_seq_type.get(seq, 0)),
            }

        for batch in seen_batches:
            sequence_stats[batch] = {
                seq: seq_number_by_seq_type_and_batch[batch].get(seq, 0)
                for seq in seen_seq_types
            }

        return ProjectSummary(
            project=project,
            participants=pmodels,
            participant_keys=participant_keys,
            sample_keys=sample_keys,
            sequence_keys=sequence_keys,
            total_samples=total_samples,
            total_samples_in_query=total_samples_in_query,
            total_participants=total_participants,
            total_sequences=total_sequences,
            batch_sequence_stats=sequence_stats,
            cram_seqr_stats=cram_seqr_stats,
            seqr_links=seqr_links,
            seqr_sync_types=seqr_sync_types,
        )
