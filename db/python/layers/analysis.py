import datetime
from collections import defaultdict
from typing import Any

from api.utils import group_by
from db.python.connect import Connection
from db.python.filters import GenericFilter
from db.python.layers.base import BaseLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.analysis import AnalysisFilter, AnalysisTable
from db.python.tables.cohort import CohortTable
from db.python.tables.output_file import OutputFileTable
from db.python.tables.sample import SampleTable
from db.python.tables.sequencing_group import SequencingGroupFilter
from db.python.utils import get_logger
from models.enums import AnalysisStatus
from models.models import (
    AnalysisInternal,
    AuditLogInternal,
    ProportionalDateModel,
    ProportionalDateProjectModel,
    ProportionalDateTemporalMethod,
    SequencingGroupInternal,
)
from models.models.output_file import RecursiveDict
from models.models.project import (
    FullWriteAccessRoles,
    ProjectId,
    ReadAccessRoles,
)
from models.models.sequencing_group import SequencingGroupInternalId

ES_ANALYSIS_OBJ_INTRO_DATE = datetime.date(2022, 6, 21)

logger = get_logger()


def check_or_parse_date(
    date_: datetime.date | str | None,
) -> datetime.date | None:
    """Check or parse a date"""
    if not date_:
        return None
    if isinstance(date_, datetime.datetime):
        return date_.date()
    if isinstance(date_, datetime.date):
        return date_
    if isinstance(date_, str):
        return datetime.datetime.strptime(date_, '%Y-%m-%d').date()
    raise ValueError(f'Invalid datetime.date {date_!r}')


class AnalysisLayer(BaseLayer):
    """Layer for analysis logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)

        self.sampt = SampleTable(connection)
        self.at = AnalysisTable(connection)
        self.ct = CohortTable(connection)
        self.oft = OutputFileTable(connection)

    # GETS

    async def get_analysis_by_id(self, analysis_id: int):
        """Get analysis by ID"""
        project, analysis = await self.at.get_analysis_by_id(analysis_id)

        self.connection.check_access_to_projects_for_ids(
            [project], allowed_roles=ReadAccessRoles
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

        return await self.at.query(
            filter_=AnalysisFilter(
                project=GenericFilter(eq=project),
                active=GenericFilter(eq=True),
                status=GenericFilter(
                    in_=[AnalysisStatus.IN_PROGRESS, AnalysisStatus.QUEUED]
                ),
            )
        )

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

    async def query(self, filter_: AnalysisFilter) -> list[AnalysisInternal]:
        """Query analyses"""
        analyses = await self.at.query(filter_)

        if not analyses:
            return []

        self.connection.check_access_to_projects_for_ids(
            set(a.project for a in analyses if a.project is not None),
            allowed_roles=ReadAccessRoles,
        )

        return analyses

    async def get_cram_size_proportionate_map(
        self,
        projects: list[ProjectId],
        sequencing_types: list[str] | None,
        temporal_methods: list[ProportionalDateTemporalMethod],
        start_date: datetime.date = None,
        end_date: datetime.date = None,
    ) -> dict[ProportionalDateTemporalMethod, list[ProportionalDateModel]]:
        """
        This is a bit more complex, but we want to generate a map of cram size by day,
        based on the temporal_method (sample create datetime.date, joint call datetime.date).
            NB: Can't use the align datetime.date because the data is not good enough
        """
        # sanity checks
        if not start_date:
            raise ValueError('start_date must be set')
        start_date = check_or_parse_date(start_date)
        end_date = check_or_parse_date(end_date)

        if end_date and start_date and end_date < start_date:
            raise ValueError(
                f'end_date ({end_date}) must be after start_date ({start_date})'
            )

        if start_date < datetime.date(2020, 1, 1):
            raise ValueError(f'start_date ({start_date}) must be after 2020-01-01')

        project_objs = self.connection.get_and_check_access_to_projects_for_ids(
            project_ids=projects, allowed_roles=ReadAccessRoles
        )
        project_name_map = {p.id: p.name for p in project_objs}

        sglayer = SequencingGroupLayer(self.connection)
        sgfilter = SequencingGroupFilter(
            project=GenericFilter(in_=projects),
            type=(GenericFilter(in_=sequencing_types) if sequencing_types else None),
        )

        sequencing_groups = await sglayer.query(sgfilter)
        sg_by_id = {sg.id: sg for sg in sequencing_groups}
        sg_to_project = {sg.id: sg.project for sg in sequencing_groups}

        cram_list = await self.at.query(
            AnalysisFilter(
                sequencing_group_id=GenericFilter(in_=list(sg_to_project.keys())),
                type=GenericFilter(eq='cram'),
                status=GenericFilter(eq=AnalysisStatus.COMPLETED),
            )
        )

        crams_by_sg = group_by(cram_list, lambda c: c.sequencing_group_ids[0])

        results: dict[ProportionalDateTemporalMethod, list[ProportionalDateModel]] = {}
        for method in temporal_methods:
            if method == ProportionalDateTemporalMethod.SAMPLE_CREATE_DATE:
                results[method] = await self.get_prop_map_for_sample_create_date(
                    sg_by_id=sg_by_id,
                    crams=crams_by_sg,
                    project_name_map=project_name_map,
                    start_date=start_date,
                    end_date=end_date,
                )
            elif method == ProportionalDateTemporalMethod.SG_ES_INDEX_DATE:
                results[method] = await self.get_prop_map_for_es_index_date(
                    sg_by_id=sg_by_id,
                    crams=crams_by_sg,
                    project_name_map=project_name_map,
                    start_date=start_date,
                    end_date=end_date,
                )
            else:
                raise NotImplementedError(
                    f'Have not implemented {method.value} temporal method yet'
                )

        return results

    async def get_prop_map_for_sample_create_date(
        self,
        sg_by_id: dict[SequencingGroupInternalId, SequencingGroupInternal],
        crams: dict[SequencingGroupInternalId, list[AnalysisInternal]],
        project_name_map: dict[int, str],
        start_date: datetime.date | None,
        end_date: datetime.date | None,
    ) -> list[ProportionalDateModel]:
        """
        Turn the sequencing_group_sizes_project into a proportionate map

        We'll do this in three steps:

        1. First, calculate a delta of each project by day, based on the sample create date
            This means we can more easily handle SGs with multiple crams

        2. Iterate over the days, and progressively sum up the sizes in the map.

        3. Iterate over the days, and proportion each day by total size in the day.
        """

        # 1.
        by_project_delta = await self.calculate_delta_of_crams_by_project_for_day(
            sg_by_id=sg_by_id,
            crams=crams,
            project_name_map=project_name_map,
            start_date=start_date,
            end_date=end_date,
        )

        # 2: progressively sum up the sizes, prepping for step 3

        by_date_totals: list[tuple[datetime.date, dict[str, int]]] = []
        sorted_days = list(sorted(by_project_delta.items(), key=lambda el: el[0]))
        for dt, project_map in sorted_days:
            if len(by_date_totals) == 0:
                by_date_totals.append((dt, project_map))
                continue

            new_project_map = {**by_date_totals[-1][1]}

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

    async def get_prop_map_for_es_index_date(
        self,
        sg_by_id: dict[SequencingGroupInternalId, SequencingGroupInternal],
        crams: dict[SequencingGroupInternalId, list[AnalysisInternal]],
        project_name_map: dict[ProjectId, str],
        start_date: datetime.date | None,
        end_date: datetime.date | None,
    ) -> list[ProportionalDateModel]:
        """
        Calculate the prop map for es-indices.
        """
        sizes_by_sg = await self.get_cram_sizes_between_range(
            crams=crams,
            start_date=start_date,
            end_date=end_date,
        )

        sg_to_project = self.map_sg_to_project(sg_by_id)
        sgs_added_by_day = await self.get_sgs_added_by_day_by_es_indices(
            start=start_date,
            end=end_date,
            projects=list(project_name_map.keys()),
        )
        sgs_seen: set[SequencingGroupInternalId] = set()

        ordered_days = sorted(sgs_added_by_day.items(), key=lambda el: el[0])
        prop_map = []
        for day, sgs_for_day in ordered_days:
            by_project = self.calculate_cram_size_by_project(
                sgs_seen, sgs_for_day, sg_to_project, sizes_by_sg, day
            )
            total_size = sum(by_project.values())
            prop_map.append(
                ProportionalDateModel(
                    date=day,
                    projects=[
                        ProportionalDateProjectModel(
                            project=project_name_map[pid],
                            percentage=size / total_size,
                            size=size,
                        )
                        for pid, size in by_project.items()
                    ],
                )
            )

        return prop_map

    def map_sg_to_project(
        self, sg_by_id: dict[SequencingGroupInternalId, SequencingGroupInternal]
    ) -> dict[SequencingGroupInternalId, ProjectId]:
        """Map the sequencing group id to project"""
        return {sg.id: sg.project for sg in sg_by_id.values()}

    def calculate_cram_size_by_project(
        self,
        sgs_seen: set,
        sgs_for_day: set,
        sg_to_project: dict,
        sizes_by_sg: dict,
        day: datetime.date,
    ) -> dict[ProjectId, int]:
        """Calculate the cram size by project for a day"""
        by_project: dict[ProjectId, int] = defaultdict(int)
        sgs_seen |= sgs_for_day
        for sg in sgs_seen:
            if sg not in sg_to_project:
                continue
            if cram_size := self.get_cram_size_for_squencing_group(
                sg, sizes_by_sg, day
            ):
                by_project[sg_to_project[sg]] += cram_size
        return by_project

    def get_cram_size_for_squencing_group(
        self, sg_id: SequencingGroupInternalId, sizes_by_sg: dict, date: datetime.date
    ) -> int | None:
        """Get the cram size for a sequencing group"""
        if sg_id not in sizes_by_sg:
            return None
        sg_sizes = sizes_by_sg[sg_id]
        if len(sg_sizes) == 1:
            return sg_sizes[0][1]
        for dt, size in sg_sizes[::-1]:
            if dt <= date:
                return size
        logger.warning(f'Could not find size for {sg_id} on {date}')
        return None

    async def calculate_delta_of_crams_by_project_for_day(
        self,
        sg_by_id: dict[SequencingGroupInternalId, SequencingGroupInternal],
        crams: dict[SequencingGroupInternalId, list[AnalysisInternal]],
        project_name_map: dict[int, str],
        start_date: datetime.date | None,
        end_date: datetime.date | None,
    ) -> dict[datetime.date, dict[str, int]]:
        """
        Calculate a delta of cram size for each project by day, so you can sum them up
        """
        sglayer = SequencingGroupLayer(self.connection)
        sample_create_dates = await sglayer.get_samples_create_date_from_sgs(
            list(crams.keys())
        )
        by_date_diff: dict[datetime.date, dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )

        for sg_id, analyses in crams.items():
            await self._process_crams_for_sg(
                sg_id,
                sg_by_id[sg_id].project,
                analyses,
                sample_create_dates,
                project_name_map,
                by_date_diff,
                start_date,
                end_date,
            )

        return by_date_diff

    async def _process_crams_for_sg(
        self,
        sg_id: SequencingGroupInternalId,
        project_id: ProjectId,
        analyses: list[AnalysisInternal],
        sample_create_dates: dict[SequencingGroupInternalId, datetime.date],
        project_name_map: dict[int, str],
        by_date_diff: dict[datetime.date, dict[str, int]],
        start_date: datetime.date | None,
        end_date: datetime.date | None,
    ):
        """Process the crams for a sequencing group and add to the by_date_diff"""

        for idx, cram in enumerate(analyses):
            project = project_name_map.get(project_id)
            delta, sg_start_date = self._calculate_delta_and_start_date(
                idx, cram, analyses, sample_create_dates[sg_id]
            )
            if not delta or (end_date and sg_start_date > end_date):
                continue
            clamped_date = max(sg_start_date, start_date)
            by_date_diff[clamped_date][project] += delta

    def _calculate_delta_and_start_date(
        self,
        idx: int,
        cram: AnalysisInternal,
        analyses: list[AnalysisInternal],
        sample_create_date: datetime.date,
    ) -> tuple[int | None, datetime.date]:
        """Calculate the delta and start date for a cram"""
        if idx == 0:
            return cram.meta.get('size') or 0, sample_create_date
        sg_start_date = check_or_parse_date(cram.timestamp_completed)
        new_cram_size = cram.meta.get('size')
        if new_cram_size:
            delta = new_cram_size - analyses[idx - 1].meta.get('size', 0)
            return delta, sg_start_date
        return None, sg_start_date

    async def get_cram_sizes_between_range(
        self,
        crams: dict[SequencingGroupInternalId, list[AnalysisInternal]],
        start_date: datetime.date | None,
        end_date: datetime.date | None,
    ) -> dict[SequencingGroupInternalId, list[tuple[datetime.date, int]]]:
        """
        This method uses the cram start time
        """
        sglayer = SequencingGroupLayer(self.connection)
        sample_create_dates = await sglayer.get_samples_create_date_from_sgs(
            list(crams.keys())
        )
        by_date: dict[SequencingGroupInternalId, list[tuple[datetime.date, int]]] = (
            defaultdict(list)
        )

        for sg_id, analyses in crams.items():
            # For each cram, get the date range and size from each analysis object by
            # sequencing group id
            by_date[sg_id] = self._get_list_of_dates_and_sizes(
                sg_id, analyses, sample_create_dates[sg_id], start_date, end_date
            )

        return by_date

    def _get_list_of_dates_and_sizes(
        self, sg_id, analyses, default_start_date, start_date, end_date
    ):
        """Given a list of analyses, get the list of tuple (date, size)"""
        if len(analyses) == 1:
            # it does resolve the same, but most cases come through here
            return [
                (
                    max(default_start_date, start_date),
                    analyses[0].meta.get('size') or 0,
                )
            ]

        by_date: list[tuple[datetime.date, int]] = []
        for idx, cram in enumerate(
            sorted(analyses, key=lambda a: a.timestamp_completed)
        ):
            # use the default_start_date for the first analysis
            # replace with the current analyses timestamp_completed for the rest
            sg_start_date = (
                default_start_date if idx == 0 else cram.timestamp_completed.date()
            )

            # If the start is after the end, we can skip
            if end_date and sg_start_date > end_date:
                continue

            # Take the latest of the sg_start_date and the start_date
            clamped_date = (
                max(sg_start_date, start_date) if start_date else sg_start_date
            )

            by_date[sg_id].append((clamped_date, cram.meta.get('size', 0)))

        return by_date

    async def get_sgs_added_by_day_by_es_indices(
        self,
        start: datetime.date,
        end: datetime.date,
        projects: list[ProjectId],
    ):
        """
        Fetch the relevant analysis objects + crams from sample-metadata
        to put together the proportionate_map.
        """
        by_day: dict[datetime.date, set[SequencingGroupInternalId]] = defaultdict(set)

        # unfortunately, the way ES-indices are progressive, it's basically impossible
        # for us to know if a sequencing-group was removed. So we assume that no SG
        # was removed. So we'll sum up all SGs up to the start date and then use that
        # as the starting point for the prop map.

        by_day[start] = await self.at.find_sgs_in_joint_call_or_es_index_up_to_date(
            date=start
        )

        if start < ES_ANALYSIS_OBJ_INTRO_DATE:
            # do a special check for joint-calling
            joint_calls = await self.at.query(
                AnalysisFilter(
                    type=GenericFilter(eq='joint-calling'),
                    status=GenericFilter(eq=AnalysisStatus.COMPLETED),
                    project=GenericFilter(in_=projects),
                    timestamp_completed=GenericFilter(
                        # midnight on the day
                        gt=datetime.datetime.combine(start, datetime.time()),
                        lte=datetime.datetime.combine(end, datetime.time()),
                    ),
                )
            )
            for jc in joint_calls:
                by_day[jc.timestamp_completed.date()].update(jc.sequencing_group_ids)

        es_indices = await self.at.query(
            AnalysisFilter(
                type=GenericFilter(eq='es-index'),
                status=GenericFilter(eq=AnalysisStatus.COMPLETED),
                project=GenericFilter(in_=projects),
                timestamp_completed=GenericFilter(
                    # midnight on the day
                    gt=datetime.datetime.combine(start, datetime.time()),
                    lte=datetime.datetime.combine(end, datetime.time()),
                ),
            )
        )
        for es in es_indices:
            by_day[es.timestamp_completed.date()].update(es.sequencing_group_ids)

        return by_day

    async def get_audit_logs_by_analysis_ids(
        self, analysis_ids: list[int]
    ) -> dict[int, list[AuditLogInternal]]:
        """Get audit logs for analysis IDs"""
        return await self.at.get_audit_log_for_analysis_ids(analysis_ids)

    # CREATE / UPDATE

    async def create_analysis(
        self,
        analysis: AnalysisInternal,
        project: ProjectId = None,
    ) -> int:
        """Create a new analysis"""

        # Validate cohort sgs equal sgs
        if analysis.cohort_ids and analysis.sequencing_group_ids:
            all_cohort_sgs: list[int] = []
            for cohort_id in analysis.cohort_ids:
                cohort_sgs = await self.ct.get_cohort_sequencing_group_ids(cohort_id)
                all_cohort_sgs.extend(cohort_sgs)
            if set(all_cohort_sgs) != set(analysis.sequencing_group_ids):
                raise ValueError(
                    'Cohort sequencing groups do not match analysis sequencing groups'
                )

        new_analysis_id = await self.at.create_analysis(
            analysis_type=analysis.type,
            status=analysis.status,
            sequencing_group_ids=analysis.sequencing_group_ids,
            cohort_ids=analysis.cohort_ids,
            meta=analysis.meta,
            active=analysis.active,
            project=project,
        )

        await self.oft.process_output_for_analysis(
            analysis_id=new_analysis_id,
            output=analysis.output,
            outputs=analysis.outputs,
            blobs=None,
        )

        return new_analysis_id

    async def add_sequencing_groups_to_analysis(
        self, analysis_id: int, sequencing_group_ids: list[int]
    ):
        """Add samples to an analysis (through the linked table)"""
        project_ids = await self.at.get_project_ids_for_analysis_ids([analysis_id])
        self.connection.check_access_to_projects_for_ids(
            project_ids, allowed_roles=FullWriteAccessRoles
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
        outputs: RecursiveDict | None = None,
        active: bool | None = None,
    ):
        """
        Update the status of an analysis, set timestamp_completed if relevant
        """
        project_ids = await self.at.get_project_ids_for_analysis_ids([analysis_id])
        self.connection.check_access_to_projects_for_ids(
            project_ids, allowed_roles=FullWriteAccessRoles
        )

        await self.at.update_analysis(
            analysis_id=analysis_id,
            status=status,
            meta=meta,
            active=active,
        )

        await self.oft.process_output_for_analysis(
            analysis_id=analysis_id, output=output, outputs=outputs, blobs=None
        )
