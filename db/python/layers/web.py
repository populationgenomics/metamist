# pylint: disable=too-many-locals, too-many-instance-attributes
import asyncio
from collections import defaultdict
from datetime import date

from api.utils import group_by
from db.python.filters import GenericFilter
from db.python.filters.sequencing_group import SequencingGroupFilter
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
from db.python.tables.sequencing_group import SequencingGroupTable
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
from models.models.web import (
    ProjectSummaryInternal,
    WebProject,
    ProjectQcWebReportInternal,
)


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

    async def get_project_qc_web_reports(
        self,
        sequencing_types: list[str] | None = None,
        stages: list[str] | None = None,
    ):
        """
        Get web reports for a project, optionally filtered by sequencing type.
        """
        webdb = WebDb(self.connection)
        return await webdb.get_project_qc_web_reports(
            sequencing_types=sequencing_types or [],
            stages=stages or [],
        )

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
        return await self.connection.fetch_val(_query, {'project': self.project_id})

    async def get_total_number_of_participants(self):
        """Get total number of participants within a project"""
        _query = 'SELECT COUNT(*) FROM participant WHERE project = :project'
        return await self.connection.fetch_val(_query, {'project': self.project_id})

    async def get_total_number_of_sequencing_groups(self):
        """Get total number of sequencing groups within a project"""
        _query = """
        SELECT COUNT(*)
        FROM sequencing_group sg
        INNER JOIN sample s ON s.id = sg.sample_id
        WHERE project = :project AND NOT sg.archived"""
        return await self.connection.fetch_val(_query, {'project': self.project_id})

    async def get_total_number_of_assays(self):
        """Get total number of sequences within a project"""
        _query = """
        SELECT COUNT(*)
        FROM assay sq
        INNER JOIN sample s ON s.id = sq.sample_id
        WHERE s.project = :project"""
        return await self.connection.fetch_val(_query, {'project': self.project_id})

    async def get_project_qc_web_reports(
        self, sequencing_types: list[str], stages: list[str]
    ):
        """Get qc web report analyses for a project filtered by sequencing type and stage."""
        _query = """
        SELECT
            a.id,
            a.timestamp_completed,
            of.path as output,
            JSON_VALUE(a.meta, '$.sequencing_type') as sequencing_type,
            JSON_VALUE(a.meta, '$.stage') as stage,
            GROUP_CONCAT(DISTINCT asg.sequencing_group_id) as sequencing_groups
        FROM analysis a
        LEFT JOIN analysis_outputs ao ON ao.analysis_id = a.id
        LEFT JOIN output_file of ON of.id = ao.file_id
        LEFT JOIN analysis_sequencing_group asg ON asg.analysis_id = a.id
        WHERE a.project = :project
        AND a.type = 'qc'
        AND a.status = 'COMPLETED'
        AND JSON_VALUE(a.meta, '$.sequencing_type') IN :sequencing_types
        AND JSON_VALUE(a.meta, '$.stage') in :stages
        GROUP BY a.id, of.path
        """
        web_reports = await self.connection.fetch_all(
            _query,
            {
                'project': self.project_id,
                'sequencing_types': tuple(sequencing_types),
                'stages': tuple(stages),
            },
        )
        return [
            ProjectQcWebReportInternal(
                id=report['id'],
                timestamp_completed=report['timestamp_completed'],
                output=report['output'],
                sequencing_type=report['sequencing_type'],
                stage=report['stage'],
                sequencing_groups=report['sequencing_groups'].split(',')
                if report['sequencing_groups']
                else [],
            )
            for report in web_reports
        ]

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

        project_db = self.project

        if not project_db:
            raise ValueError('Project not provided')

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
            atable.get_number_of_crams_by_sequencing_type(project=self.project_id),
            sgtable.get_type_numbers_for_project(project=self.project_id),
            seqtable.get_assay_type_numbers_by_batch_for_project(
                project=self.project_id
            ),
            atable.get_seqr_stats_by_sequencing_type(project=self.project_id),
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

        sfilter = query.get_sample_filter()
        if not sfilter.participant_id:
            sfilter.participant_id = GenericFilter()
        if sfilter.participant_id.in_:
            # take the intersection of the participants, because we're not showing
            # participants that aren't returned by other criteria
            sfilter.participant_id.in_ = list(
                set(sfilter.participant_id.in_) & {p.id for p in participants}
            )
        else:
            sfilter.participant_id.in_ = [p.id for p in participants]

        samples = await slayer.query(sfilter)

        sgfilter = sfilter.get_sg_filter()
        if not sgfilter.sample:
            sgfilter.sample = SequencingGroupFilter.SequencingGroupSampleFilter()
        if sgfilter.sample:
            if not sgfilter.sample.id:
                sgfilter.sample.id = GenericFilter()
            if sgfilter.sample.id and sgfilter.sample.id.in_:
                sgfilter.sample.id.in_ = list(
                    set(sgfilter.sample.id.in_) & {s.id for s in samples}
                )
            else:
                sgfilter.sample.id.in_ = [s.id for s in samples]

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
                            external_ids=sg.external_ids or {},
                        )
                    )

                screate: str | None = None
                if screate_date := sample_created_dates.get(sample.id):
                    screate = screate_date.isoformat()

                nested_samples.append(
                    NestedSampleInternal(
                        id=sample.id,
                        external_ids=sample.external_ids,
                        type=sample.type,
                        meta=sample.meta,
                        sample_root_id=sample.sample_root_id,
                        sample_parent_id=sample.sample_parent_id,
                        created_date=screate,
                        sequencing_groups=nested_sgs,
                        non_sequencing_assays=[],
                        active=sample.active,
                    )
                )
            families = []
            for family in families_by_pid.get(participant.id, []):
                families.append(
                    FamilySimpleInternal(id=family.id, external_ids=family.external_ids)
                )
            nested_participant = NestedParticipantInternal(
                id=participant.id,
                external_ids=participant.external_ids,
                samples=nested_samples,
                meta=participant.meta,
                families=families,
            )

            nested_participants.append(nested_participant)

        return nested_participants
