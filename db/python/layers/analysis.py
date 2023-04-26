from collections import defaultdict
from datetime import date
from typing import Any

from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.analysis import AnalysisTable
from db.python.tables.project import ProjectId
from db.python.tables.sample import SampleTable
from db.python.utils import get_logger
from models.enums import AnalysisStatus
from models.models.analysis import AnalysisInternal

logger = get_logger()


class AnalysisLayer(BaseLayer):
    """Layer for analysis logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)

        self.sampt = SampleTable(connection)
        self.at = AnalysisTable(connection)

    # GETS

    async def get_analyses_for_samples(
        self,
        sample_ids: list[int],
        analysis_type: str | None,
        status: AnalysisStatus | None,
        check_project_id=True,
    ) -> list[AnalysisInternal]:
        """
        Get a list of all analysis that relevant for samples

        """
        projects, analysis = await self.at.get_analyses_for_samples(
            sample_ids,
            analysis_type=analysis_type,
            status=status,
        )

        if len(analysis) == 0:
            return []

        if check_project_id:
            await self.ptable.check_access_to_project_ids(
                self.author, projects, readonly=True
            )

        return analysis

    async def get_analysis_by_id(self, analysis_id: int, check_project_id=True):
        """Get analysis by ID"""
        project, analysis = await self.at.get_analysis_by_id(analysis_id)
        if check_project_id:
            await self.ptable.check_access_to_project_id(
                self.author, project, readonly=True
            )

        return analysis

    async def get_latest_complete_analysis_for_type(
        self,
        project: ProjectId,
        analysis_type: str,
        meta: dict[str, Any] = None,
    ) -> AnalysisInternal:
        """Get SINGLE latest complete analysis for some analysis type"""
        return await self.at.get_latest_complete_analysis_for_type(
            project=project, analysis_type=analysis_type, meta=meta
        )

    async def get_all_sequencing_group_ids_without_analysis_type(
        self, project: ProjectId, analysis_type: str
    ):
        """
        Find all the sequencing_groups that don't have an "analysis_type"
        """
        return await self.at.get_all_sequencing_group_ids_without_analysis_type(
            analysis_type=analysis_type, project=project
        )

    async def get_incomplete_analyses(
        self, project: ProjectId
    ) -> list[AnalysisInternal]:
        """
        Gets details of analysis with status queued or in-progress
        """
        return await self.at.get_incomplete_analyses(project=project)

    async def get_sample_cram_path_map_for_seqr(
        self,
        project: ProjectId,
        sequence_types: list[str],
        participant_ids: list[int] = None,
    ) -> list[dict[str, Any]]:
        """Get (ext_participant_id, cram_path, internal_id) map"""
        return await self.at.get_sample_cram_path_map_for_seqr(
            project=project,
            sequence_types=sequence_types,
            participant_ids=participant_ids,
        )

    async def query_analysis(
        self,
        sample_ids: list[int] = None,
        sequencing_group_ids: list[int] = None,
        project_ids: list[int] = None,
        analysis_type: str = None,
        status: AnalysisStatus = None,
        meta: dict[str, Any] = None,
        output: str = None,
        active: bool = None,
    ) -> list[AnalysisInternal]:
        """
        :param sample_ids: sample_ids means it contains the analysis contains at least one of the sample_ids in the list
        """
        analyses = await self.at.query_analysis(
            sample_ids=sample_ids,
            sequencing_group_ids=sequencing_group_ids,
            project_ids=project_ids,
            analysis_type=analysis_type,
            status=status,
            meta=meta,
            output=output,
            active=active,
        )
        # print(analyses)
        return analyses

    async def get_sequencing_group_file_sizes(
        self,
        project_ids: list[int] = None,
        start_date: date = None,
        end_date: date = None,
    ) -> list[dict]:
        """
        Get the file sizes from all the given projects group by sample filtered
        on the date range
        """

        # Get samples from pids
        sglayer = SequencingGroupLayer(self.connection)
        sequencing_groups = await sglayer.query(project_ids=project_ids)

        sequencing_group_ids = [sg.id for sg in sequencing_groups]

        # Get sample history
        history = await sglayer.get_sequencing_groups_create_date(sequencing_group_ids)

        def keep_sequencing_group(sid):
            d = history[sid]
            if start_date and d <= start_date:
                return True
            if end_date and d <= end_date:
                return True
            if not start_date and not end_date:
                return True
            return False

        # Get size of analysis crams
        filtered_sequencing_group_ids = list(
            filter(keep_sequencing_group, sequencing_group_ids)
        )
        if not filtered_sequencing_group_ids:
            # if there are no sequencing group IDs, the query analysis treats that
            # as not including a filter (so returns all for the project IDs)
            return []
        crams = await self.at.query_analysis(
            sequencing_group_ids=filtered_sequencing_group_ids,
            analysis_type='cram',
            status=AnalysisStatus.COMPLETED,
        )
        crams_by_project: dict[int, dict[int, list[dict]]] = defaultdict(dict)
        sg_by_id = {s.id: s for s in sequencing_groups}

        # Manual filtering to find the most recent analysis cram of each sequence type
        # for each sample
        affected_analyses = []
        for cram in crams:
            sgids = cram.sequencing_group_ids
            if len(sgids) > 1:
                affected_analyses.append(cram)
                continue

            if size := cram.meta.get('size'):
                sgid = int(sgids[0])
                sg = sg_by_id.get(sgid)
                if not sg:
                    affected_analyses.append(cram)
                    continue

                # Allow for multiple crams per sample in the future
                # even though for now we only support 1
                crams_by_project[sg.project][sgid] = [
                    {
                        'start': history[sgid],
                        'end': None,  # TODO: add functionality for deleted samples
                        'size': size,
                    }
                ]

        formated = [
            {'project': p, 'sequencing_groups': crams_by_project[p]}
            for p in crams_by_project
        ]
        return formated

    # CREATE / UPDATE

    async def create_analysis(
        self,
        analysis: AnalysisInternal,
        author: str = None,
        project: ProjectId = None,
    ) -> int:
        """Create a new analysis"""
        return await self.at.create_analysis(
            analysis_type=analysis.type,
            status=analysis.status,
            sequencing_group_ids=analysis.sequencing_group_ids,
            meta=analysis.meta,
            output=analysis.output,
            active=analysis.active,
            author=author,
            project=project,
        )

    async def add_sequencing_groups_to_analysis(
        self, analysis_id: int, sequencing_group_ids: list[int], check_project_id=True
    ):
        """Add samples to an analysis (through the linked table)"""
        if check_project_id:
            project_ids = await self.at.get_project_ids_for_analysis_ids([analysis_id])
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=False
            )

        return await self.at.add_sequencing_groups_to_analysis(
            analysis_id=analysis_id, sequencing_group_ids=sequencing_group_ids
        )

    async def update_analysis(
        self,
        analysis_id: int,
        status: AnalysisStatus,
        meta: dict[str, Any] = None,
        output: str | None = None,
        author: str | None = None,
        check_project_id=True,
    ):
        """
        Update the status of an analysis, set timestamp_completed if relevant
        """
        if check_project_id:
            project_ids = await self.at.get_project_ids_for_analysis_ids([analysis_id])
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=False
            )

        await self.at.update_analysis(
            analysis_id=analysis_id,
            status=status,
            meta=meta,
            output=output,
            author=author,
        )

    async def get_analysis_runner_log(
        self,
        project_ids: list[int] = None,
        author: str = None,
        output_dir: str = None,
    ) -> list[AnalysisInternal]:
        """
        Get log for the analysis-runner, useful for checking this history of analysis
        """
        return await self.at.get_analysis_runner_log(
            project_ids, author=author, output_dir=output_dir
        )
