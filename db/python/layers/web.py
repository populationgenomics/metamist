# pylint: disable=too-many-locals, too-many-instance-attributes
import asyncio
import itertools
import json
import re
from collections import defaultdict
from datetime import date

from api.utils import group_by
from db.python.layers.base import BaseLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.seqr import SeqrLayer
from db.python.tables.analysis import AnalysisTable
from db.python.tables.assay import AssayTable
from db.python.tables.base import DbBase
from db.python.tables.project import ProjectPermissionsTable
from db.python.tables.sequencing_group import SequencingGroupTable
from db.python.utils import escape_like_term
from models.models import (
    AssayInternal,
    FamilySimpleInternal,
    NestedParticipantInternal,
    NestedSampleInternal,
    NestedSequencingGroupInternal,
    SearchItem,
    parse_sql_bool,
)
from models.models.web import ProjectSummaryInternal, WebProject


class WebLayer(BaseLayer):
    """Web layer"""

    async def get_project_summary(
        self,
        grid_filter: list[SearchItem],
        token: int = 0,
        limit: int = 20,
    ) -> ProjectSummaryInternal:
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

    def _project_summary_sample_query(self, grid_filter: list[SearchItem]):
        """
        Get query for getting list of samples
        """
        wheres = ['s.project = :project', 's.active']
        values = {'project': self.project}
        where_str = ''
        for query in grid_filter:
            value = query.query
            field = query.field
            prefix = query.model_type.value
            key = (
                f'{query.model_type}_{field}_{value}'.replace('-', '_')
                .replace('.', '_')
                .replace(':', '_')
                .replace(' ', '_')
            )
            if bool(re.search(r'\W', field)) and not query.is_meta:
                # protect against SQL injection attacks
                raise ValueError('Invalid characters in field')
            if not query.is_meta:
                q = f'{prefix}.{field} LIKE :{key}'
            else:
                # double double quote field to allow white space
                q = f'JSON_VALUE({prefix}.meta, "$.""{field}""") LIKE :{key}'  # noqa: B028
            wheres.append(q)
            values[key] = escape_like_term(value) + '%'
        if wheres:
            where_str = 'WHERE ' + ' AND '.join(wheres)

        # Skip 'limit' and 'after' SQL commands so we can get all samples that match
        # the query to determine the total count, then take the selection of samples
        # for the current page. This is more efficient than doing 2 queries separately.
        sample_query = f"""
        SELECT s.id, s.external_id, s.type, s.meta, s.participant_id, s.active
        FROM sample s
        LEFT JOIN assay a ON s.id = a.sample_id
        LEFT JOIN participant p ON p.id = s.participant_id
        LEFT JOIN family_participant fp on s.participant_id = fp.participant_id
        LEFT JOIN family f ON f.id = fp.family_id
        LEFT JOIN sequencing_group sg ON s.id = sg.sample_id
        {where_str}
        GROUP BY id
        ORDER BY id
        """
        return sample_query, values

    @staticmethod
    def _project_summary_process_assay_rows_by_sample_id(
        assay_rows,
    ) -> dict[int, list[AssayInternal]]:
        """
        Get sequences for samples for project summary
        """

        seq_id_to_sample_id_map = {seq['id']: seq['sample_id'] for seq in assay_rows}
        seq_models = [
            AssayInternal(
                id=seq['id'],
                type=seq['type'],
                meta=json.loads(seq['meta']),
                sample_id=seq['sample_id'],
            )
            for seq in assay_rows
        ]
        seq_models_by_sample_id = group_by(
            seq_models, lambda s: seq_id_to_sample_id_map[s.id]
        )

        return seq_models_by_sample_id

    @staticmethod
    def _project_summary_process_sequencing_group_rows_by_sample_id(
        sequencing_group_rows,
        sequencing_eid_rows: list,
        seq_models_by_sample_id: dict[int, list[AssayInternal]],
    ) -> dict[int, list[NestedSequencingGroupInternal]]:
        assay_models_by_id = {
            assay.id: assay
            for assay in itertools.chain(*seq_models_by_sample_id.values())
        }

        sequencing_group_eid_map: dict[int, dict[str, str]] = defaultdict(dict)
        for row in sequencing_eid_rows:
            sgid = row['sequencing_group_id']
            sequencing_group_eid_map[sgid][row['name']] = row['name']

        sg_by_id: dict[int, NestedSequencingGroupInternal] = {}
        sg_id_to_sample_id: dict[int, int] = {}
        for row in sequencing_group_rows:
            sg_id = row['id']
            assay = assay_models_by_id.get(row['assay_id'])
            if sg_id in sg_by_id:
                if assay:
                    sg_by_id[sg_id].assays.append(assay)
                continue
            sg_id_to_sample_id[sg_id] = row['sample_id']
            sg_by_id[sg_id] = NestedSequencingGroupInternal(
                id=sg_id,
                meta=json.loads(row['meta']),
                type=row['type'],
                technology=row['technology'],
                platform=row['platform'],
                assays=[assay] if assay else [],
                external_ids=sequencing_group_eid_map.get(sg_id, {}),
            )

        return group_by(sg_by_id.values(), lambda sg: sg_id_to_sample_id[sg.id])

    @staticmethod
    def _project_summary_process_sample_rows(
        sample_rows,
        assay_models_by_sample_id: dict[int, list[AssayInternal]],
        sg_models_by_sample_id: dict[int, list[NestedSequencingGroupInternal]],
        sample_id_start_times: dict[int, date],
    ) -> list[NestedSampleInternal]:
        """
        Process the returned sample rows into nested samples + sequences
        """
        assays_in_sgs = set(
            assay.id
            for sgs in sg_models_by_sample_id.values()
            for sg in sgs
            for assay in sg.assays
        )
        # filter assays to only those not in sequencing groups
        filtered_assay_models_by_sid = {}
        for sample_id, assays in assay_models_by_sample_id.items():
            filtered_assays = [a for a in assays if a.id not in assays_in_sgs]
            if len(filtered_assays) > 0:
                filtered_assay_models_by_sid[sample_id] = filtered_assays

        smodels = [
            NestedSampleInternal(
                id=s['id'],
                external_id=s['external_id'],
                type=s['type'],
                meta=json.loads(s['meta']) or {},
                created_date=str(sample_id_start_times.get(s['id'], '')),
                sequencing_groups=sg_models_by_sample_id.get(s['id'], []),
                non_sequencing_assays=filtered_assay_models_by_sid.get(s['id'], []),
                active=parse_sql_bool(s['active']),
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

    async def get_total_number_of_sequencing_groups(self):
        """Get total number of sequencing groups within a project"""
        _query = """
        SELECT COUNT(*)
        FROM sequencing_group sg
        INNER JOIN sample s ON s.id = sg.sample_id
        WHERE project = :project AND NOT sg.archived"""
        return await self.connection.fetch_val(_query, {'project': self.project})

    async def get_total_number_of_assays(self):
        """Get total number of sequences within a project"""
        _query = """
        SELECT COUNT(*)
        FROM assay sq
        INNER JOIN sample s ON s.id = sq.sample_id
        WHERE s.project = :project"""
        return await self.connection.fetch_val(_query, {'project': self.project})

    @staticmethod
    def _project_summary_process_family_rows_by_pid(
        family_rows,
    ) -> dict[int, list[FamilySimpleInternal]]:
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
            res_families[f['family_id']] = FamilySimpleInternal(
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
        # TODO: get this from the database
        for seqtype in 'genome', 'exome':
            key = f'seqr-project-{seqtype}'
            if guid := project.meta.get(key):
                seqr_links[seqtype] = SeqrLayer.get_seqr_link_from_guid(guid)

        return seqr_links

    async def get_project_summary(
        self,
        grid_filter: list[SearchItem],
        limit: int,
        token: int = 0,
    ) -> ProjectSummaryInternal:
        """
        Get project summary

        :param token: for PAGING
        :param limit: Number of SAMPLEs to return, not including nested sequences
        """
        # do initial query to get sample info
        sampl = SampleLayer(self._connection)
        sample_query, values = self._project_summary_sample_query(grid_filter)
        ptable = ProjectPermissionsTable(self._connection)
        project_db = await ptable.get_and_check_access_to_project_for_id(
            self.author, self.project, readonly=True
        )
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
            return ProjectSummaryInternal.empty(project)

        pids = list(set(s['participant_id'] for s in sample_rows))
        sids = list(s['id'] for s in sample_rows)

        # assays
        assay_query = """
            SELECT id, sample_id, meta, type
            FROM assay
            WHERE sample_id IN :sids
        """
        assay_promise = self.connection.fetch_all(assay_query, {'sids': sids})

        # sequencing_groups
        sg_query = """
            SELECT
                sg.id, sg.meta, sg.type, sg.sample_id,
                sg.technology, sg.platform, sga.assay_id
            FROM sequencing_group sg
            INNER JOIN sequencing_group_assay sga ON sga.sequencing_group_id = sg.id
            WHERE sg.sample_id IN :sids AND NOT sg.archived
        """
        sequencing_group_promise = self.connection.fetch_all(sg_query, {'sids': sids})

        sg_eid_query = """
            SELECT sgeid.sequencing_group_id, sgeid.name, sgeid.external_id
            FROM sequencing_group_external_id sgeid
            INNER JOIN sequencing_group sg ON sg.id = sgeid.sequencing_group_id
            WHERE sg.sample_id IN :sids AND NOT sg.archived
        """
        sequencing_group_eid_promise = self.connection.fetch_all(
            sg_eid_query, {'sids': sids}
        )

        # participant
        p_query = """
            SELECT id, external_id, meta, reported_sex, reported_gender, karyotype
            FROM participant
            WHERE id in :pids
        """
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
        seqtable = AssayTable(self._connection)
        sgtable = SequencingGroupTable(self._connection)

        [
            assay_rows,
            sequencing_group_rows,
            sequencing_group_eids,
            participant_rows,
            family_rows,
            sample_id_start_times,
            total_samples,
            total_participants,
            total_sequencing_groups,
            total_assays,
            cram_number_by_seq_type,
            seq_number_by_seq_type,
            seq_number_by_seq_type_and_batch,
            seqr_stats_by_seq_type,
            seqr_sync_types,
        ] = await asyncio.gather(
            assay_promise,
            sequencing_group_promise,
            sequencing_group_eid_promise,
            participant_promise,
            family_promise,
            sampl.get_samples_create_date(sids),
            self.get_total_number_of_samples(),
            self.get_total_number_of_participants(),
            self.get_total_number_of_sequencing_groups(),
            self.get_total_number_of_assays(),
            atable.get_number_of_crams_by_sequencing_type(project=self.project),
            sgtable.get_type_numbers_for_project(project=self.project),
            seqtable.get_assay_type_numbers_by_batch_for_project(project=self.project),
            atable.get_seqr_stats_by_sequencing_type(project=self.project),
            SeqrLayer(self._connection).get_synchronisable_types(project_db),
        )

        # post-processing
        assay_models_by_sample_id = (
            self._project_summary_process_assay_rows_by_sample_id(assay_rows)
        )
        seq_group_models_by_sample_id = (
            self._project_summary_process_sequencing_group_rows_by_sample_id(
                sequencing_group_rows=sequencing_group_rows,
                sequencing_eid_rows=sequencing_group_eids,
                seq_models_by_sample_id=assay_models_by_sample_id,
            )
        )
        smodels = self._project_summary_process_sample_rows(
            sample_rows,
            assay_models_by_sample_id=assay_models_by_sample_id,
            sg_models_by_sample_id=seq_group_models_by_sample_id,
            sample_id_start_times=sample_id_start_times,
        )
        # the pydantic model is casting to the id to a str, as that makes sense on
        # the front end but cast back here to do the lookup
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
                    NestedParticipantInternal(
                        id=None,
                        external_id=None,
                        meta=None,
                        families=[],
                        samples=[s],
                        reported_sex=None,
                        reported_gender=None,
                        karyotype=None,
                        # project=self.project,
                    )
                )
            elif pid not in pid_seen:
                pid_seen.add(pid)
                p = participant_map[pid]
                pmodels.append(
                    NestedParticipantInternal(
                        id=p['id'],
                        external_id=p['external_id'],
                        meta=json.loads(p['meta']),
                        families=pid_to_families.get(p['id'], []),
                        samples=list(smodels_by_pid.get(p['id'])),
                        reported_sex=p['reported_sex'],
                        reported_gender=p['reported_gender'],
                        karyotype=p['karyotype'],
                        # project=self.project,
                    )
                )

        ignore_participant_keys: set[str] = set()
        ignore_sample_meta_keys = {'reads', 'vcfs', 'gvcf'}
        ignore_assay_meta_keys = {
            'reads',
            'vcfs',
            'gvcf',
            'sequencing_platform',
            'sequencing_technology',
            'sequencing_type',
        }
        ignore_sg_meta_keys: set[str] = set()

        participant_meta_keys = set(
            pk
            for p in pmodels
            if p and p.meta
            for pk in p.meta.keys()
            if pk not in ignore_participant_keys
        )
        sample_meta_keys = set(
            sk
            for s in smodels
            for sk in s.meta.keys()
            if (sk not in ignore_sample_meta_keys)
        )
        sg_meta_keys = set(
            sk
            for sgs in seq_group_models_by_sample_id.values()
            for sg in sgs
            for sk in sg.meta
            if (sk not in ignore_sg_meta_keys)
        )

        assay_meta_keys = set(
            sk
            for assays in assay_models_by_sample_id.values()
            for assay in assays
            for sk in assay.meta
            if (sk not in ignore_assay_meta_keys)
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

        assay_keys = [('type', 'type')] + sorted(
            [('meta.' + k, k) for k in assay_meta_keys]
        )
        sequencing_group_keys = [
            ('id', 'Sequencing Group ID'),
            ('platform', 'Platform'),
            ('technology', 'Technology'),
            ('type', 'Type'),
        ] + sorted([('meta.' + k, k) for k in sg_meta_keys])

        seen_seq_types = set(cram_number_by_seq_type.keys()).union(
            set(seq_number_by_seq_type.keys())
        )
        seen_batches = set(seq_number_by_seq_type_and_batch.keys())

        sequence_stats: dict[str, dict[str, str]] = {}
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

        return ProjectSummaryInternal(
            project=project,
            participants=pmodels,
            participant_keys=participant_keys,
            sample_keys=sample_keys,
            sequencing_group_keys=sequencing_group_keys,
            assay_keys=assay_keys,
            total_samples=total_samples,
            total_samples_in_query=total_samples_in_query,
            total_participants=total_participants,
            total_assays=total_assays,
            total_sequencing_groups=total_sequencing_groups,
            batch_sequencing_group_stats=sequence_stats,
            cram_seqr_stats=cram_seqr_stats,
            seqr_links=seqr_links,
            seqr_sync_types=seqr_sync_types,
        )
