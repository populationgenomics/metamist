import datetime
import warnings
from collections import defaultdict
from typing import Any, Optional, Union

from api.utils import group_by
from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.analysis import AnalysisFilter, AnalysisTable
from db.python.tables.output_file import OutputFileTable
from db.python.tables.sample import SampleTable
from db.python.tables.sequencing_group import SequencingGroupFilter
from db.python.utils import GenericFilter, get_logger
from models.enums import AnalysisStatus
from models.models import (
    AnalysisInternal,
    AuditLogInternal,
    ProportionalDateModel,
    ProportionalDateProjectModel,
    ProportionalDateTemporalMethod,
    SequencingGroupInternal,
)
from models.models.project import ProjectId
from models.models.sequencing_group import SequencingGroupInternalId

ES_ANALYSIS_OBJ_INTRO_DATE = datetime.date(2022, 6, 21)

logger = get_logger()


def check_or_parse_date(date_: datetime.date | str | None) -> datetime.date | None:
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

        self.sample_table = SampleTable(connection)
        self.analysis_table = AnalysisTable(connection)
        self.output_file_table = OutputFileTable(connection)

    # GETS

    async def get_analyses_for_samples(
        self,
        sample_ids: list[int],
        analysis_type: str | None,
        status: AnalysisStatus | None,
        check_project_id=True,
    ) -> list[AnalysisInternal]:
        """
        Retrieves analyses related to given sample IDs based on specified filters and permissions.

        Args:
            sample_ids: List of integer IDs for samples.
            analysis_type: Optional; specific type of analysis to filter by.
            status: Optional; filter analyses by their current status.
            check_project_id: Boolean indicating whether to check access rights to the related project IDs.

        Returns:
            A list of AnalysisInternal objects corresponding to the filtered analyses.
        """
        projects, analyses = await self.analysis_table.get_analyses_for_samples(
            sample_ids, analysis_type=analysis_type, status=status
        )

        if not analyses:  # Early return if no analyses found
            return []

        if check_project_id:  # Check access rights if required
            await self.ptable.check_access_to_project_ids(self.author, projects, readonly=True)

        return analyses

    async def get_analysis_by_id(self, analysis_id: int, check_project_id=True):
        """
        Retrieves a specific analysis by its ID, with an optional check for project access rights.

        Args:
            analysis_id: The integer ID of the analysis to retrieve.
            check_project_id: Boolean indicating whether to check access rights to the analysis's project ID.

        Returns:
            An AnalysisInternal object corresponding to the specified analysis ID, or None if not found.
        """
        project, analysis = await self.analysis_table.get_analysis_by_id(analysis_id)

        if analysis and check_project_id:  # Perform project access check if analysis exists and is required
            await self.ptable.check_access_to_project_id(self.author, project, readonly=True)

        return analysis

    async def get_latest_complete_analysis_for_type(
        self,
        project: ProjectId,
        analysis_type: str,
        meta: dict[str, Any] = None,
    ) -> AnalysisInternal:
        """
        Retrieves the most recent completed analysis of a specified type for a given project.

        Args:
            project: The project identifier for which the analysis is being retrieved.
            analysis_type: A string representing the specific type of analysis to retrieve.
            meta: An optional dictionary containing additional metadata parameters for the query.

        Returns:
            The latest completed analysis of the specified type for the given project.
        """
        meta = meta or {}  # Safeguard against mutable default argument
        return await self.analysis_table.get_latest_complete_analysis_for_type(
            project=project, analysis_type=analysis_type, meta=meta
        )

    async def get_all_sequencing_group_ids_without_analysis_type(
        self, project: ProjectId, analysis_type: str
    ):
        """
        Retrieves all sequencing group IDs associated with a given project that do not have a specified analysis type.

        Args:
            project: The project identifier to filter sequencing groups.
            analysis_type: The type of analysis to exclude in the search.

        Returns:
            A list of sequencing group IDs that do not have the specified analysis type.
        """
        return await self.analysis_table.get_all_sequencing_group_ids_without_analysis_type(
            project=project, analysis_type=analysis_type
        )

    async def get_incomplete_analyses(
        self, project: ProjectId
    ) -> list[AnalysisInternal]:
        """
        Retrieves all analyses for a specified project that are currently not complete,
        specifically those with statuses 'queued' or 'in-progress'.

        Args:
            project: The identifier of the project for which to retrieve incomplete analyses.

        Returns:
            A list of AnalysisInternal instances representing each incomplete analysis for the project.
        """
        return await self.analysis_table.get_incomplete_analyses(project=project)

    async def get_sample_cram_path_map_for_seqr(
        self,
        project: ProjectId,
        sequencing_types: list[str],
        participant_ids: list[int] = None,
    ) -> list[dict[str, Any]]:
        """
        Retrieves a mapping of external participant IDs to CRAM file paths and internal IDs for a given project,
        filtered by sequencing types and optionally by participant IDs.

        This mapping is specifically formatted for use in the seqr project, facilitating data retrieval and organization.

        Args:
            project: The project identifier for which the CRAM path mapping is being retrieved.
            sequencing_types: A list of strings representing the types of sequencing to filter the CRAM paths.
            participant_ids: Optional; a list of integer IDs representing specific participants to include in the mapping.

        Returns:
            A list of dictionaries, each containing 'ext_participant_id', 'cram_path', and 'internal_id' keys, representing each relevant sample.
        """
        return await self.analysis_table.get_sample_cram_path_map_for_seqr(
            project=project,
            sequencing_types=sequencing_types,
            participant_ids=participant_ids,
        )

    async def query(self, filter_: AnalysisFilter, check_project_ids=True):
        """
        Queries and returns analyses based on specified filters. Optionally checks for access rights
        to the projects associated with these analyses if no specific project is defined in the filter.

        Args:
            filter_: An AnalysisFilter object containing filtering criteria for the analyses.
            check_project_ids: A boolean flag indicating whether to check access rights for the projects
                            of the returned analyses if no project is specified in the filter.

        Returns:
            A list of AnalysisInternal objects that match the filter criteria. Returns an empty list if
            no analyses match the criteria.
        """
        analyses = await self.analysis_table.query(filter_)

        if not analyses:
            return []

        if check_project_ids and not filter_.project:
            await self.ptable.check_access_to_project_ids(
                self.author, {a.project for a in analyses}, readonly=True
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

        project_objs = await self.ptable.get_and_check_access_to_projects_for_ids(
            project_ids=projects, user=self.author, readonly=True
        )
        project_name_map = {p.id: p.name for p in project_objs}

        sglayer = SequencingGroupLayer(self.connection)
        sgfilter = SequencingGroupFilter(
            project=GenericFilter(in_=projects),
            type=GenericFilter(in_=sequencing_types) if sequencing_types else None,
        )

        sequencing_groups = await sglayer.query(sgfilter)
        sg_by_id = {sg.id: sg for sg in sequencing_groups}
        sg_to_project = {sg.id: sg.project for sg in sequencing_groups}

        cram_list = await self.analysis_table.query(
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

        This one works a bit different, we start with the es-indices, and progressively
        add samples into this list as we see new samples.

        We'll do this in three steps:

            1. Prepare the crams into a format where it's easier for us to find:
                "What cram size is appropriate for this day"

            2. Get all SGs inside any es-index (* or joint call) before the start date
                (that forms our baseline crams)

            3. Get all es-indices between the start and end date
                We'll do some processing on these analysis objects so we just get the
                SGs that are new on a specific day.

            4. Iterate over the days, and add the most appropriate cram size for each
                SG for that day.
                    * We can't progressively sum, because the cram size might change
                        between days, so get it on each day.
        """
        sizes_by_sg = await self.get_cram_sizes_between_range(
            crams=crams,
            start_date=start_date,
            end_date=end_date,
        )

        def get_cram_size_for(sg_id: SequencingGroupInternalId, date):
            """
            From the list of crams, return the most appropriate cram size for a
            sequencing group on a specific day.
            """
            if sg_id not in sizes_by_sg:
                return None
            sg_sizes = sizes_by_sg[sg_id]
            if len(sg_sizes) == 1:
                # probably shouldn't just return it, but it's the only cram size
                # and for some reason it's in the es-index, so we'll just use it
                return sg_sizes[0][1]
            for dt, size in sg_sizes[::-1]:
                if dt <= date:
                    return size
            logger.warning(f'Could not find size for {sg_id} on {date}')
            return None

        sg_to_project: dict[SequencingGroupInternalId, ProjectId] = {
            sg.id: sg.project for sg in sg_by_id.values()
        }

        sgs_added_by_day = await self.get_sgs_added_by_day_by_es_indices(
            start=start_date, end=end_date, projects=list(project_name_map.keys())
        )
        sgs_seen: set[SequencingGroupInternalId] = set()

        ordered_days = sorted(sgs_added_by_day.items(), key=lambda el: el[0])
        prop_map: list[ProportionalDateModel] = []
        for day, sgs_for_day in ordered_days:
            by_project: dict[ProjectId, int] = defaultdict(int)
            sgs_seen |= sgs_for_day
            for sg in sgs_seen:
                if sg not in sg_to_project:
                    # it's a sg that was in an es-index, but not in the projects
                    # we care about, so happily skip. It's _probably_ quicker to do
                    # it this way, rather than only querying for the SGs we care about
                    continue
                if cram_size := get_cram_size_for(sg, day):
                    by_project[sg_to_project[sg]] += cram_size

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
            for idx, cram in enumerate(analyses):
                project = project_name_map.get(sg_by_id[sg_id].project)
                delta = None
                if idx == 0:
                    # use the sample_create_date for the first analysis
                    sg_start_date = sample_create_dates[sg_id]
                    delta = cram.meta.get('size') or 0
                else:
                    # replace with the current analyses timestamp_completed
                    sg_start_date = check_or_parse_date(cram.timestamp_completed)
                    if new_cram_size := cram.meta.get('size'):
                        delta = new_cram_size - analyses[idx - 1].meta.get('size', 0)
                if not delta:
                    continue
                if end_date and sg_start_date > end_date:
                    continue

                # this will eventually get the "best" cram size correctly by applying
                # deltas for multiple crams before the start datetime.date, so the
                # clamping here is fine.
                clamped_date = max(sg_start_date, start_date)
                by_date_diff[clamped_date][project] += delta

        return by_date_diff

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
            if len(analyses) == 1:
                # it does resolve the same, but most cases come through here
                by_date[sg_id] = [
                    (
                        max(sample_create_dates[sg_id], start_date),
                        analyses[0].meta.get('size') or 0,
                    )
                ]
            else:
                for idx, cram in enumerate(
                    sorted(analyses, key=lambda a: a.timestamp_completed)
                ):
                    if idx == 0:
                        # use the sample_create_date for the first analysis
                        sg_start_date = sample_create_dates[sg_id]
                    else:
                        # replace with the current analyses timestamp_completed
                        sg_start_date = cram.timestamp_completed.date()

                    if end_date and sg_start_date > end_date:
                        continue

                    clamped_date = (
                        max(sg_start_date, start_date) if start_date else sg_start_date
                    )

                    if 'size' not in cram.meta:
                        continue

                    by_date[sg_id].append((clamped_date, cram.meta.get('size') or 0))

        return by_date

    async def get_sgs_added_by_day_by_es_indices(
        self, start: datetime.date, end: datetime.date, projects: list[ProjectId]
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

        by_day[start] = await self.analysis_table.find_sgs_in_joint_call_or_es_index_up_to_date(
            date=start
        )

        if start < ES_ANALYSIS_OBJ_INTRO_DATE:
            # do a special check for joint-calling
            joint_calls = await self.analysis_table.query(
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

        es_indices = await self.analysis_table.query(
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
        return await self.analysis_table.get_audit_log_for_analysis_ids(analysis_ids)

    async def add_sequencing_groups_to_analysis(
        self, analysis_id: int, sequencing_group_ids: list[int], check_project_id=True
    ):
        """Add samples to an analysis (through the linked table)"""
        if check_project_id:
            project_ids = await self.analysis_table.get_project_ids_for_analysis_ids([analysis_id])
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=False
            )

        return await self.analysis_table.add_sequencing_groups_to_analysis(
            analysis_id=analysis_id, sequencing_group_ids=sequencing_group_ids
        )

    # CREATE / UPDATE

    async def _handle_analysis_outputs(
        self,
        analysis_id: int,
        output: Optional[Union[str, dict]],
        outputs: Optional[Union[str, dict]]
    ):
        """
        Handle the deprecation of 'output' and update analysis output files.

        :param analysis_id: ID of the analysis.
        :param output: Deprecated single output field.
        :param outputs: New outputs field.
        """
        if output:
            warnings.warn(
                'Analysis.output will be deprecated, use Analysis.outputs instead',
                PendingDeprecationWarning,
                stacklevel=2
            )
            await self.output_file_table.create_or_update_analysis_output_files_from_json(
                analysis_id=analysis_id,
                json_dict=output
            )
        elif outputs:
            await self.output_file_table.create_or_update_analysis_output_files_from_json(
                analysis_id=analysis_id,
                json_dict=outputs
            )

    async def create_analysis(
        self,
        analysis: AnalysisInternal,
        project: ProjectId = None,
    ) -> int:
        """Create a new analysis"""
        new_analysis_id = await self.analysis_table.create_analysis(
            analysis_type=analysis.type,
            status=analysis.status,
            sequencing_group_ids=analysis.sequencing_group_ids,
            meta=analysis.meta,
            active=analysis.active,
            project=project,
        )

        # Handle analysis outputs
        await self._handle_analysis_outputs(
            analysis_id=new_analysis_id,
            output=analysis.output,
            outputs=analysis.outputs
        )

        return new_analysis_id

    async def update_analysis(
        self,
        analysis_id: int,
        status: AnalysisStatus,
        meta: dict[str, Any] = None,
        output: str | None = None,
        outputs: str | None = None,
        check_project_id=True,
    ):
        """
        Update the status of an analysis, set timestamp_completed if relevant
        """
        if check_project_id:
            project_ids = await self.analysis_table.get_project_ids_for_analysis_ids([analysis_id])
            await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=False
            )

        await self.analysis_table.update_analysis(
            analysis_id=analysis_id,
            status=status,
            meta=meta,
        )

        # Handle analysis outputs
        await self._handle_analysis_outputs(
            analysis_id=analysis_id,
            output=output,
            outputs=outputs
        )

    async def get_analysis_runner_log(
        self,
        project_ids: list[int] = None,
        # author: str = None,
        output_dir: str = None,
        ar_guid: str = None,
    ) -> list[AnalysisInternal]:
        """
        Get log for the analysis-runner, useful for checking this history of analysis
        """
        return await self.analysis_table.get_analysis_runner_log(
            project_ids,
            # author=author,
            output_dir=output_dir,
            ar_guid=ar_guid,
        )
