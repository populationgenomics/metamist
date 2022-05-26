# pylint: disable=too-many-locals
import asyncio
import dataclasses
import json
from collections import defaultdict
from itertools import groupby
from typing import Dict, List, Optional, Set

from pydantic import BaseModel

from db.python.connect import DbBase
from db.python.layers.base import BaseLayer
from models.enums import SampleType, SequenceType, SequenceStatus


class NestedSequence(BaseModel):
    """Sequence model"""

    id: int
    type: SequenceType
    status: SequenceStatus
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
class ProjectSummary:
    """Return class for the project summary endpoint"""

    total_samples: int
    participants: List[NestedParticipant]
    participant_keys: List[str]
    sample_keys: List[str]
    sequence_keys: List[str]


class WebLayer(BaseLayer):
    """Web layer"""

    async def get_project_summary(
        self, token: Optional[str], limit: int = 50
    ) -> ProjectSummary:
        """
        Get a summary of a project, allowing some "after" token,
        and limit to the number of results.
        """
        webdb = WebDb(self.connection)
        return await webdb.get_project_summary(token=token, limit=limit)


class WebDb(DbBase):
    """Db layer for web related routes,"""

    def _project_summary_sample_query(self, after, limit):
        """
        Get query for getting list of samples
        """
        wheres = ['project = :project']
        values = {'limit': limit, 'project': self.project}
        if after:
            values['after'] = after
            wheres.append('id > :after')

        where_str = ''
        if wheres:
            where_str = 'WHERE ' + ' AND '.join(wheres)
        sample_query = f'SELECT id, external_id, type, meta, participant_id FROM sample {where_str} ORDER BY id LIMIT :limit'

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
            )
            for seq in sequence_rows
        ]
        seq_models_by_sample_id = {
            k: list(v)
            for k, v in (
                groupby(seq_models, key=lambda s: seq_id_to_sample_id_map[s.id])
            )
        }

        return seq_models_by_sample_id

    @staticmethod
    def _project_summary_process_sample_rows(
        sample_rows, seq_models_by_sample_id, sample_id_start_times: Dict[int, str]
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
                created_date=sample_id_start_times.get(s['id'], ''),
                sequences=seq_models_by_sample_id.get(s['id'], []) or [],
            )
            for s in sample_rows
        ]
        return smodels

    async def get_total_number_of_samples(self):
        """Get total number of active samples within a project"""
        _query = 'SELECT COUNT(*) FROM sample WHERE project = :project AND active'
        return (await self.connection.fetch_one(_query, {'project': self.project}))[0]

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

    async def _project_summary_get_sample_create_date(self, sample_ids: List[int]):
        """Get a map of {internal_sample_id: date_created} for list of sample_ids"""
        _query = 'SELECT id, min(row_start) FROM sample FOR SYSTEM_TIME ALL WHERE id in :sids GROUP BY id'
        rows = await self.connection.fetch_all(_query, {'sids': sample_ids})
        return {r[0]: str(r[1].date()) for r in rows}

    async def get_project_summary(
        self, token: Optional[str], limit: int
    ) -> ProjectSummary:
        """
        Get project summary

        :param token: for PAGING
        :param limit: Number of SAMPLEs to return, not including nested sequences
        """
        # do initial query to get sample info
        sample_query, values = self._project_summary_sample_query(token, limit)
        sample_rows = list(await self.connection.fetch_all(sample_query, values))

        if len(sample_rows) == 0:
            return ProjectSummary(
                participants=[],
                participant_keys=[],
                sample_keys=[],
                sequence_keys=[],
                total_samples=0,
            )

        pids = list(set(s['participant_id'] for s in sample_rows))
        sids = list(s['id'] for s in sample_rows)

        # sequences

        seq_query = 'SELECT id, sample_id, meta, type, status FROM sample_sequencing WHERE sample_id IN :sids'
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

        [
            sequence_rows,
            participant_rows,
            family_rows,
            sample_id_start_times,
            total_samples,
        ] = await asyncio.gather(
            sequence_promise,
            participant_promise,
            family_promise,
            self._project_summary_get_sample_create_date(sids),
            self.get_total_number_of_samples(),
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
        smodels_by_pid = {
            k: list(v)
            for k, v in (groupby(smodels, key=lambda s: sid_to_pid[int(s.id)]))
        }

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

        participant_keys = ['external_id']

        if has_reported_sex:
            participant_keys.append('reported_sex')
        if has_reported_gender:
            participant_keys.append('reported_gender')
        if has_karyotype:
            participant_keys.append('karyotype')

        participant_keys.extend('meta.' + k for k in participant_meta_keys)
        sample_keys = ['id', 'external_id', 'created_date'] + [
            'meta.' + k for k in sample_meta_keys
        ]
        sequence_keys = ['type'] + ['meta.' + k for k in sequence_meta_keys]

        return ProjectSummary(
            participants=pmodels,
            participant_keys=participant_keys,
            sample_keys=sample_keys,
            sequence_keys=sequence_keys,
            total_samples=total_samples,
        )
