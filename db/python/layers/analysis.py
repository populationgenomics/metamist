from collections import defaultdict
from datetime import date, datetime
from typing import Any

from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.analysis import AnalysisFilter, AnalysisTable
from db.python.tables.project import ProjectId
from db.python.tables.sample import SampleTable
from db.python.tables.sequencing_group import SequencingGroupFilter
from db.python.utils import GenericFilter, get_logger
from models.enums import AnalysisStatus
from models.models import (
    AnalysisInternal,
    ProportionalDateModel,
    ProportionalDateProjectModel,
    ProportionalDateTemporalMethod,
)

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
        sequencing_types: list[str],
        participant_ids: list[int] = None,
    ) -> list[dict[str, Any]]:
        """Get (ext_participant_id, cram_path, internal_id) map"""
        return await self.at.get_sample_cram_path_map_for_seqr(
            project=project,
            sequencing_types=sequencing_types,
            participant_ids=participant_ids,
        )

    async def query(self, filter_: AnalysisFilter, check_project_ids=True):
        """Query analyses"""
        analyses = await self.at.query(filter_)

        if not analyses:
            return []

        if check_project_ids and not filter_.project:
            await self.ptable.check_access_to_project_ids(
                self.author, set(a.project for a in analyses), readonly=True
            )

        return analyses

    async def get_sg_history_for_temporal_method(
        self,
        sequencing_group_ids: list[int],
        temporal_method: ProportionalDateTemporalMethod,
    ) -> dict[int, date]:
        """Get the history of samples for the given temporal method"""
        sglayer = SequencingGroupLayer(self.connection)

        if temporal_method == ProportionalDateTemporalMethod.SAMPLE_CREATE_DATE:
            return await sglayer.get_samples_create_date_from_sgs(sequencing_group_ids)
        if temporal_method == ProportionalDateTemporalMethod.SG_ES_INDEX_DATE:
            return await self.at.get_sg_add_to_project_es_index(
                sg_ids=sequencing_group_ids
            )

        raise NotImplementedError(
            f'Have not implemented {temporal_method.value} temporal method yet'
        )

    @staticmethod
    def _sg_history_keep_sg_group(
        sgid: int, sg_history: dict[int, date], start_date, end_date
    ):
        """Keep the sequencing group for prop map based on the params"""
        d = sg_history.get(sgid)
        if not d:
            return False
        if start_date and d <= start_date:
            return True
        if end_date and d <= end_date:
            return True
        if not start_date and not end_date:
            return True
        return False

    async def get_sequencing_group_file_sizes(
        self,
        temporal_methods: list[ProportionalDateTemporalMethod],
        project_ids: list[int] = None,
        start_date: date = None,
        end_date: date = None,
        sequencing_types: list[str] = None,
    ) -> dict[ProportionalDateTemporalMethod, list[dict]]:
        """
        Get the file sizes from all the given projects group by sample filtered
        on the date range
        """

        if not temporal_methods:
            return {}

        # Get samples from pids
        sglayer = SequencingGroupLayer(self.connection)
        sgfilter = SequencingGroupFilter(
            project=GenericFilter(in_=project_ids),
            type=GenericFilter(in_=sequencing_types) if sequencing_types else None,
        )

        sequencing_groups = await sglayer.query(sgfilter)
        sg_to_project = {sg.id: sg.project for sg in sequencing_groups}

        crams = await self.at.query(
            AnalysisFilter(
                sequencing_group_id=GenericFilter(in_=list(sg_to_project.keys())),
                type=GenericFilter(eq='cram'),
                status=GenericFilter(eq=AnalysisStatus.COMPLETED),
            )
        )

        sg_history_by_method = {}
        # Get size of analysis crams
        for method in temporal_methods:
            sg_history_by_method[method] = await self.get_sg_date_sizes_for_method(
                sg_to_project=sg_to_project,
                method=method,
                start_date=start_date,
                end_date=end_date,
                crams=crams,
            )
        return sg_history_by_method

    async def get_sg_date_sizes_for_method(
        self,
        sg_to_project: dict,
        method: ProportionalDateTemporalMethod,
        start_date: date,
        end_date: date | None,
        crams: list[AnalysisInternal],
    ):
        """
        Take the params, determine the history method to use,
        and return the format:

        {
            {
                'project': p,
                'sequencing_groups': [

                ]
            project_id: {
                sg_id: [{
                    'start': date,
                    'end': date | None
                    'size': size,
                }]
            }
        }
        """
        method_history = await self.get_sg_history_for_temporal_method(
            sequencing_group_ids=list(sg_to_project.keys()), temporal_method=method
        )
        filtered_sequencing_group_ids = {
            sgid
            for sgid in sg_to_project
            if self._sg_history_keep_sg_group(
                sgid=sgid,
                sg_history=method_history,
                start_date=start_date,
                end_date=end_date,
            )
        }
        if not filtered_sequencing_group_ids:
            # if there are no sequencing group IDs, the query analysis treats that
            # as not including a filter (so returns all for the project IDs)
            return []

        crams_by_project: dict[int, dict[int, list[dict]]] = defaultdict(dict)

        # Manual filtering to find the most recent analysis cram of each sequence type
        # for each sample
        affected_analyses = []
        for cram in crams:
            sgids = cram.sequencing_group_ids
            if len(sgids) > 1:
                affected_analyses.append(cram)
                continue

            sgid = int(sgids[0])
            if sgid not in filtered_sequencing_group_ids:
                continue

            # Allow for multiple crams per sample in the future
            # even though for now we only support 1
            if sgid not in method_history:
                # sometimes we might find crams that actually shouldn't
                # be included in the cost yet, we can skip them :)
                continue

            if size := cram.meta.get('size'):
                if project := sg_to_project.get(sgid):
                    crams_by_project[project][sgid] = [
                        {
                            'start': method_history[sgid],
                            'end': None,  # TODO: add functionality for deleted samples
                            'size': size,
                        }
                    ]

        return [
            {'project': p, 'sequencing_groups': crams_by_project[p]}
            for p in crams_by_project
        ]

    async def get_cram_size_proportionate_map(
        self,
        projects: list[ProjectId],
        sequencing_types: list[str] | None,
        temporal_methods: list[ProportionalDateTemporalMethod],
        start_date: date = None,
        end_date: date = None,
    ) -> dict[ProportionalDateTemporalMethod, list[ProportionalDateModel]]:
        """
        This is a bit more complex, but we want to generate a map of cram size by day,
        based on the temporal_method (sample create date, joint call date).
            NB: Can't use the align date because the data is not good enough
        """
        # sanity checks
        if not start_date:
            raise ValueError('start_date must be set')
        if end_date and start_date and end_date < start_date:
            raise ValueError('end_date must be after start_date')

        if start_date < date(2020, 1, 1):
            raise ValueError('start_date must be after 2020-01-01')

        end_date_date = None
        if end_date:
            if isinstance(end_date, date):
                end_date_date = end_date
            elif isinstance(end_date, datetime):
                end_date_date = end_date.date()
            else:
                raise ValueError('end_date must be a date or datetime')

        project_objs = await self.ptable.get_and_check_access_to_projects_for_ids(
            project_ids=projects, user=self.author, readonly=True
        )
        project_name_map = {p.id: p.name for p in project_objs}

        sg_sizes_by_method_by_project = await self.get_sequencing_group_file_sizes(
            project_ids=projects,
            start_date=start_date,
            end_date=end_date,
            sequencing_types=sequencing_types,
            temporal_methods=temporal_methods,
        )

        results: dict[ProportionalDateTemporalMethod, list[ProportionalDateModel]] = {}
        for (
            method,
            sequencing_group_sizes_by_project,
        ) in sg_sizes_by_method_by_project.items():
            results[
                method
            ] = self.get_cram_size_proportionate_map_from_sequencing_group_sizes(
                sequencing_group_sizes_by_project=sequencing_group_sizes_by_project,
                project_name_map=project_name_map,
                start_date=start_date,
                end_date=end_date_date,
            )

        return {k.value: v for k, v in results.items()}

    def get_cram_size_proportionate_map_from_sequencing_group_sizes(
        self,
        sequencing_group_sizes_by_project: list[dict],
        project_name_map: dict[int, str],
        start_date: date,
        end_date: date = None,
    ):
        """
        Turn the sequencing_group_sizes_project into a proportionate map

        We'll do this in three steps:

        1. First assign the cram a {dataset: total_size} map on the relevant day.
            This generates a diff of each dataset by day.

        2. Iterate over the days, and progressively sum up the sizes in the map.

        3. Iterate over the days, and proportion each day by total size in the day.
        """
        by_date_diff: dict[date, dict[str, int]] = defaultdict(lambda: defaultdict(int))

        # 1.
        for obj in sequencing_group_sizes_by_project:
            project_name = project_name_map.get(obj['project'])
            for sg_dates in obj['sequencing_groups'].values():
                sizes_dates: list[tuple[date, int]] = []
                for obj in sg_dates:
                    obj_date = obj['start']
                    size = int(obj['size'])
                    if end_date and start_date > end_date:
                        continue
                    if len(sizes_dates) > 0:
                        # subtract last size to get the difference
                        # if the crams got smaller, this number will be negative
                        size -= sizes_dates[-1][1]

                    adjusted_start_date = obj_date
                    if start_date and obj_date < start_date:
                        adjusted_start_date = start_date
                    by_date_diff[adjusted_start_date][project_name] += size
                    sizes_dates.append((adjusted_start_date, size))

        # 2: progressively sum up the sizes, prepping for step 3

        by_date_totals: list[tuple[date, dict[str, int]]] = []
        sorted_days = list(sorted(by_date_diff.items(), key=lambda el: el[0]))
        for idx, (dt, project_map) in enumerate(sorted_days):
            if idx == 0:
                by_date_totals.append((dt, project_map))
                continue

            new_project_map = {**by_date_totals[idx - 1][1]}

            for pn, cram_size in project_map.items():
                if pn not in new_project_map:
                    new_project_map[pn] = cram_size
                else:
                    new_project_map[pn] += cram_size

            by_date_totals.append((dt, new_project_map))

        # 3: proportion each day
        prop_map: list[ProportionalDateModel] = []
        for dt, project_map in by_date_totals:
            total_size = sum(project_map.values())
            for_date = ProportionalDateModel(date=dt, projects=[])
            for project_name, size in project_map.items():
                for_date.projects.append(
                    ProportionalDateProjectModel(
                        project=project_name,
                        percentage=float(size) / total_size,
                        size=size,
                    )
                )

            prop_map.append(for_date)

        return prop_map

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
