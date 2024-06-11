# pylint: disable=too-many-locals, too-many-instance-attributes
import asyncio
from collections import defaultdict
from datetime import date

from api.utils import group_by
from db.python.layers.assay import AssayLayer
from db.python.layers.base import BaseLayer
from db.python.layers.family import FamilyLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.seqr import SeqrLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.analysis import AnalysisTable
from db.python.tables.assay import AssayFilter, AssayTable
from db.python.tables.base import DbBase
from db.python.tables.participant import ParticipantFilter
from db.python.tables.project import ProjectPermissionsTable
from db.python.tables.sample import SampleFilter
from db.python.tables.sequencing_group import (
    SequencingGroupFilter,
    SequencingGroupTable,
)
from db.python.utils import GenericFilter
from models.models import (
    AssayInternal,
    FamilySimpleInternal,
    NestedSampleInternal,
    NestedSequencingGroupInternal,
)
from models.models.family import FamilyInternal
from models.models.participant import NestedParticipantInternal, ParticipantInternal
from models.models.sample import SampleInternal
from models.models.sequencing_group import SequencingGroupInternal
from models.models.web import ProjectSummaryInternal, WebProject


class WebLayer(BaseLayer):
    """Web layer"""

    async def get_project_summary(
        self,
    ) -> ProjectSummaryInternal:
        """
        Get a summary of a project, allowing some "after" token,
        and limit to the number of results.
        """
        webdb = WebDb(self.connection)
        return await webdb.get_project_summary()

    async def query_participants(
        self,
        query: ParticipantFilter,
        limit: int | None,
        skip: int | None = None,
    ) -> list[NestedParticipantInternal]:
        """
        Query participants
        """
        webdb = WebDb(self.connection)
        return await webdb.query_participants(query, limit, skip=skip)

    async def count_participants(self, query: ParticipantFilter) -> int:
        """Run query to count participants"""
        webdb = WebDb(self.connection)
        return await webdb.count_participants(query)


class WebDb(DbBase):
    """Db layer for web related routes,"""

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
    ) -> ProjectSummaryInternal:
        """
        Get project summary

        :param token: for PAGING
        :param limit: Number of SAMPLEs to return, not including nested sequences
        """
        if not self.project:
            raise ValueError('Project not provided')

        ptable = ProjectPermissionsTable(self._connection)
        project_db = await ptable.get_and_check_access_to_project_for_id(
            self.author, self.project, readonly=True
        )
        if not project_db:
            raise ValueError(f'Project {self.project} not found')

        project = WebProject(
            id=project_db.id,
            name=project_db.name,
            meta=project_db.meta,
            dataset=project_db.dataset,
        )
        seqr_links = self.get_seqr_links_from_project(project)

        atable = AnalysisTable(self._connection)
        seqtable = AssayTable(self._connection)
        sgtable = SequencingGroupTable(self._connection)

        [
            total_samples,
            total_participants,
            total_sequencing_groups,
            total_assays,
            cram_number_by_seq_type,
            seq_number_by_seq_type,
            assay_batch_stats,
            seqr_stats_by_seq_type,
            seqr_sync_types,
        ] = await asyncio.gather(
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

        seen_seq_types: set[str] = set(cram_number_by_seq_type.keys()).union(
            set(seq_number_by_seq_type.keys())
        )
        seq_number_by_seq_type_and_batch: dict[str, dict[str, str]] = defaultdict(dict)
        for stat in assay_batch_stats:
            # batch, sequencing_type,
            seq_number_by_seq_type_and_batch[stat.batch][stat.sequencing_type] = str(
                len(stat.sequencing_group_ids)
            )

        seen_batches = set(a.batch for a in assay_batch_stats)

        sequence_stats: dict[str, dict[str, str]] = {}
        cram_seqr_stats = {}

        for seq in seen_seq_types:
            cram_seqr_stats[seq] = {
                'Sequences': str(seq_number_by_seq_type.get(seq, 0)),
                'Crams': str(cram_number_by_seq_type.get(seq, 0)),
                'Seqr': str(seqr_stats_by_seq_type.get(seq, 0)),
            }

        for batch in seen_batches:
            batch_display = batch or '<no-batch>'
            sequence_stats[batch_display] = {
                seq: seq_number_by_seq_type_and_batch[batch].get(seq, '0')
                for seq in seen_seq_types
            }

        return ProjectSummaryInternal(
            project=project,
            total_samples=total_samples,
            total_participants=total_participants,
            total_assays=total_assays,
            total_sequencing_groups=total_sequencing_groups,
            batch_sequencing_group_stats=sequence_stats,
            cram_seqr_stats=cram_seqr_stats,
            seqr_links=seqr_links,
            seqr_sync_types=seqr_sync_types,
        )

    async def count_participants(self, query: ParticipantFilter) -> int:
        """
        Count participants
        """
        player = ParticipantLayer(self._connection)
        return await player.query_count(query)

    async def query_participants(
        self,
        query: ParticipantFilter,
        limit: int | None,
        skip: int | None = None,
    ) -> list[NestedParticipantInternal]:
        """Use query to build up nested participants"""
        player = ParticipantLayer(self._connection)
        slayer = SampleLayer(self._connection)
        sglayer = SequencingGroupLayer(self._connection)
        alayer = AssayLayer(self._connection)
        flayer = FamilyLayer(self._connection)

        participants = await player.query(query, limit=limit, skip=skip)
        if not participants:
            return []

        sfilter = SampleFilter(
            participant_id=GenericFilter(in_=[p.id for p in participants]),
        )
        if query.sample:
            sfilter.id = query.sample.id
            sfilter.type = query.sample.type
            sfilter.meta = query.sample.meta
            sfilter.external_id = query.sample.external_id
            # sfilter.active = query.sample.active

        samples = await slayer.query(sfilter)

        sgfilter = SequencingGroupFilter(
            sample_id=GenericFilter(in_=[s.id for s in samples])
        )
        if query.sequencing_group:
            sgfilter.id = query.sequencing_group.id
            sgfilter.type = query.sequencing_group.type
            sgfilter.technology = query.sequencing_group.technology
            sgfilter.platform = query.sequencing_group.platform
            sgfilter.meta = query.sequencing_group.meta

        sequencing_groups = await sglayer.query(sgfilter)

        sg_ids = [sg.id for sg in sequencing_groups if sg.id]
        assayfilter: AssayFilter | None = None
        if query.assay:
            assayfilter = AssayFilter(
                id=query.assay.id,
                type=query.assay.type,
                meta=query.assay.meta,
                external_id=query.assay.external_id,
            )

        samples_created_date = await slayer.get_samples_create_date(
            [s.id for s in samples]
        )

        assays_by_sgids = await alayer.get_assays_for_sequencing_group_ids(
            sg_ids, filter_=assayfilter
        )

        families = await flayer.get_families_by_participants(
            list(set(p.id for p in participants))
        )

        return self.assemble_nested_participants_from(
            participants=participants,
            samples=samples,
            sequencing_groups=sequencing_groups,
            assays_by_sg=assays_by_sgids,
            sample_created_dates=samples_created_date,
            families_by_pid=families,
        )

    @staticmethod
    def assemble_nested_participants_from(
        *,
        participants: list[ParticipantInternal],
        samples: list[SampleInternal],
        sequencing_groups: list[SequencingGroupInternal],
        assays_by_sg: dict[int, list[AssayInternal]],
        sample_created_dates: dict[int, date],
        families_by_pid: dict[int, list[FamilyInternal]],
    ) -> list[NestedParticipantInternal]:
        """
        Assemble nested participants from the various components
        """
        samples_by_participant_id = group_by(samples, lambda s: s.participant_id)
        sequencing_groups_by_sample_id = group_by(
            sequencing_groups, lambda sg: sg.sample_id
        )

        nested_participants = []
        for participant in participants:
            nested_samples = []

            for sample in samples_by_participant_id.get(participant.id, []):
                nested_sgs = []
                for sg in sequencing_groups_by_sample_id.get(sample.id, []):
                    nested_sgs.append(
                        NestedSequencingGroupInternal(
                            id=sg.id,
                            meta=sg.meta,
                            type=sg.type,
                            technology=sg.technology,
                            platform=sg.platform,
                            assays=assays_by_sg.get(sg.id, []),
                        )
                    )

                screate: str | None = None
                if screate_date := sample_created_dates.get(sample.id):
                    screate = screate_date.isoformat()

                nested_samples.append(
                    NestedSampleInternal(
                        id=sample.id,
                        external_id=sample.external_id,
                        type=sample.type,
                        meta=sample.meta,
                        created_date=screate,
                        sequencing_groups=nested_sgs,
                        non_sequencing_assays=[],
                        active=sample.active,
                    )
                )
            families = []
            for family in families_by_pid.get(participant.id, []):
                families.append(
                    FamilySimpleInternal(id=family.id, external_id=family.external_id)
                )
            nested_participant = NestedParticipantInternal(
                id=participant.id,
                external_id=participant.external_id,
                samples=nested_samples,
                meta=participant.meta,
                families=families,
            )

            nested_participants.append(nested_participant)

        return nested_participants
