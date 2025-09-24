# pylint: disable=dangerous-default-value
import csv
import io
from datetime import date
from typing import Annotated, Any

from fastapi import APIRouter
from fastapi.params import Body, Query
from pydantic import BaseModel
from starlette.responses import StreamingResponse

from api.utils.dates import parse_date_only_string
from api.utils.db import (
    Connection,
    get_project_db_connection,
    get_projectless_db_connection,
)
from api.utils.export import ExportType
from db.python.filters import GenericFilter
from db.python.layers.analysis import AnalysisLayer
from db.python.layers.analysis_runner import AnalysisRunnerLayer
from db.python.tables.analysis import AnalysisFilter
from db.python.tables.analysis_runner import AnalysisRunnerFilter
from models.enums import AnalysisStatus
from models.models.analysis import Analysis, ProportionalDateTemporalMethod
from models.models.analysis_runner import AnalysisRunner
from models.models.project import FullWriteAccessRoles, ReadAccessRoles
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_format,
    sequencing_group_id_format_list,
    sequencing_group_id_transform_to_raw_list,
)

router = APIRouter(prefix='/analysis', tags=['analysis'])


class AnalysisModel(BaseModel):
    """
    Model for 'createNewAnalysis'
    """

    sequencing_group_ids: list[str]
    type: str
    status: AnalysisStatus
    meta: dict[str, Any] | None = None
    output: str | None = None
    outputs: dict[str, Any] | None = None
    active: bool = True
    # please don't use this, unless you're the analysis-runner,
    # the usage is tracked ... (Ծ_Ծ)
    author: str | None = None


class AnalysisUpdateModel(BaseModel):
    """Update analysis model"""

    status: AnalysisStatus
    output: str | None = None
    outputs: dict[str, Any] | None = None
    meta: dict[str, Any] | None = None
    active: bool | None = None


class AnalysisQueryModel(BaseModel):
    """Used to query for many analysis"""

    # sequencing_group_ids means it the analysis contains at least one of in the list
    sequencing_group_ids: list[str] | None
    projects: list[str]
    type: str | None = None
    status: AnalysisStatus | None = None
    meta: dict[str, Any] | None = None
    output: str | None = None
    # Need to consider how we might filter outputs that are dicts
    # outputs: str | dict | None = None
    active: bool | None = None

    def to_filter(self, project_id_map: dict[str, int]) -> AnalysisFilter:
        """Convert to internal analysis filter"""
        return AnalysisFilter(
            sequencing_group_id=(
                GenericFilter(
                    in_=sequencing_group_id_transform_to_raw_list(
                        self.sequencing_group_ids
                    )
                )
                if self.sequencing_group_ids
                else None
            ),
            project=(
                GenericFilter(in_=[project_id_map.get(p) for p in self.projects])
                if self.projects
                else None
            ),
            type=GenericFilter(eq=self.type) if self.type else None,
        )


@router.put('/{project}/', operation_id='createAnalysis', response_model=int)
async def create_analysis(
    analysis: Analysis,
    connection: Connection = get_project_db_connection(FullWriteAccessRoles),
) -> int:
    """Create a new analysis"""

    atable = AnalysisLayer(connection)

    if analysis.author:
        # special tracking here, if we can't catch it through the header
        connection.on_behalf_of = analysis.author

    if not analysis.sequencing_group_ids and not analysis.cohort_ids:
        raise ValueError('Must specify "sequencing_group_ids" or "cohort_ids"')

    analysis_id = await atable.create_analysis(
        analysis.to_internal(),
    )

    return analysis_id


@router.patch('/{analysis_id}/', operation_id='updateAnalysis')
async def update_analysis(
    analysis_id: int,
    analysis: AnalysisUpdateModel,
    connection: Connection = get_projectless_db_connection,
) -> bool:
    """Update status of analysis"""
    atable = AnalysisLayer(connection)
    await atable.update_analysis(
        analysis_id,
        status=analysis.status,
        output=analysis.output,
        outputs=analysis.outputs,
        meta=analysis.meta,
        active=analysis.active,
    )
    return True


@router.get(
    '/{project}/type/{analysis_type}/not-included-sample-ids',
    operation_id='getAllSampleIdsWithoutAnalysisType',
)
async def get_all_sample_ids_without_analysis_type(
    analysis_type: str,
    connection: Connection = get_project_db_connection(ReadAccessRoles),
):
    """get_all_sample_ids_without_analysis_type"""
    atable = AnalysisLayer(connection)
    assert connection.project_id
    sequencing_group_ids = (
        await atable.get_all_sequencing_group_ids_without_analysis_type(
            connection.project_id, analysis_type
        )
    )
    return {
        'sequencing_group_ids': sequencing_group_id_format_list(sequencing_group_ids)
    }


@router.get(
    '/{project}/incomplete',
    operation_id='getIncompleteAnalyses',
)
async def get_incomplete_analyses(
    connection: Connection = get_project_db_connection(ReadAccessRoles),
):
    """Get analyses with status queued or in-progress"""
    atable = AnalysisLayer(connection)
    assert connection.project_id
    results = await atable.get_incomplete_analyses(project=connection.project_id)
    return [r.to_external() for r in results]


@router.get(
    '/{project}/{analysis_type}/latest-complete',
    operation_id='getLatestCompleteAnalysisForType',
)
async def get_latest_complete_analysis_for_type(
    analysis_type: str,
    connection: Connection = get_project_db_connection(ReadAccessRoles),
):
    """Get (SINGLE) latest complete analysis for some analysis type"""
    alayer = AnalysisLayer(connection)
    assert connection.project_id
    analysis = await alayer.get_latest_complete_analysis_for_type(
        project=connection.project_id, analysis_type=analysis_type
    )
    return analysis.to_external()


@router.post(
    '/{project}/{analysis_type}/latest-complete',
    operation_id='getLatestCompleteAnalysisForTypePost',
)
async def get_latest_complete_analysis_for_type_post(
    analysis_type: str,
    meta: dict[str, Any] = Body(..., embed=True),  # type: ignore[assignment]
    connection: Connection = get_project_db_connection(ReadAccessRoles),
):
    """
    Get SINGLE latest complete analysis for some analysis type
    (you can specify meta attributes in this route)
    """
    alayer = AnalysisLayer(connection)
    assert connection.project_id
    analysis = await alayer.get_latest_complete_analysis_for_type(
        project=connection.project_id,
        analysis_type=analysis_type,
        meta=meta,
    )

    return analysis.to_external()


@router.get(
    '/{analysis_id}/details',
    operation_id='getAnalysisById',
    # mfranklin: uncomment when we support OpenAPI 3.1
    # response_model=Analysis
)
async def get_analysis_by_id(
    analysis_id: int, connection: Connection = get_projectless_db_connection
):
    """Get analysis by ID"""
    atable = AnalysisLayer(connection)
    result = await atable.get_analysis_by_id(analysis_id)
    return result.to_external()


@router.post('/query', operation_id='queryAnalyses')
async def query_analyses(
    query: AnalysisQueryModel, connection: Connection = get_projectless_db_connection
):
    """Get analyses by some criteria"""
    if not query.projects:
        raise ValueError('Must specify "projects"')

    projects = connection.get_and_check_access_to_projects_for_names(
        query.projects, ReadAccessRoles
    )
    project_name_map = {p.name: p.id for p in projects}
    atable = AnalysisLayer(connection)
    analyses = await atable.query(query.to_filter(project_name_map))
    return [a.to_external() for a in analyses]


@router.get('/analysis-runner', operation_id='getAnalysisRunnerLog')
async def get_analysis_runner_log(
    project_names: Annotated[list[str], Query()] = [],  # noqa
    # author: str = None, # not implemented yet, uncomment when we do
    ar_guid: str | None = None,
    connection: Connection = get_projectless_db_connection,
) -> list[AnalysisRunner]:
    """
    Get log for the analysis-runner, useful for checking this history of analysis
    """
    if not project_names and not ar_guid:
        raise ValueError('Must specify "project_names" or "ar_guid"')

    arlayer = AnalysisRunnerLayer(connection)
    projects = connection.get_and_check_access_to_projects_for_names(
        project_names, allowed_roles=ReadAccessRoles
    )
    project_ids = [p.id for p in projects if p.id]
    project_map = {p.id: p.name for p in projects if p.id and p.name}

    results = await arlayer.query(
        AnalysisRunnerFilter(
            project=GenericFilter(in_=project_ids) if project_ids else None,
            ar_guid=GenericFilter(eq=ar_guid) if ar_guid else None,
        )
    )
    return [a.to_external(project_map=project_map) for a in results]


@router.get(
    '/{project}/sample-cram-path-map',
    operation_id='getSamplesReadsMap',
    tags=['seqr'],
)
async def get_sample_reads_map(
    export_type: ExportType = ExportType.JSON,
    sequencing_types: Annotated[list[str], Query()] = [],  # noqa
    connection: Connection = get_project_db_connection(ReadAccessRoles),
):
    """
    Get map of ExternalSampleId  pathToCram  InternalSeqGroupID for seqr

    Note that, in JSON the result is

    Description:
        Column 1: Individual ID
        Column 2: gs:// Google bucket path or server filesystem path for this Individual
        Column 3: SequencingGroup ID for this file, if different from the Individual ID.
                    Used primarily for gCNV files to identify the sample in the batch path
    """

    at = AnalysisLayer(connection)
    assert connection.project_id
    objs = await at.get_sample_cram_path_map_for_seqr(
        project=connection.project_id, sequencing_types=sequencing_types
    )

    for r in objs:
        r['sequencing_group_id'] = sequencing_group_id_format(r['sequencing_group_id'])

    if export_type == ExportType.JSON:
        return objs

    keys = ['participant_id', 'output', 'sequencing_group_id']
    rows = [[r[k] for k in keys] for r in objs]

    output = io.StringIO()
    writer = csv.writer(output, delimiter=export_type.get_delimiter())
    writer.writerows(rows)

    basefn = f'{connection.project_id}-seqr-igv-paths-{date.today().isoformat()}'

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type=export_type.get_mime_type(),
        headers={
            'Content-Disposition': f'filename={basefn}{export_type.get_extension()}'
        },
    )


@router.post(
    '/cram-proportionate-map',
    operation_id='getProportionateMap',
    # response_model=list[ProportionalDateModel] # don't uncomment this, breaks python API
)
async def get_proportionate_map(
    start: str,
    projects: list[str],
    temporal_methods: list[ProportionalDateTemporalMethod],
    sequencing_types: list[str] | None = None,
    end: str = None,
    connection: Connection = get_projectless_db_connection,
):
    """
    Get proportionate map of project sizes over time, specifying the temporal
    methods to use. These will be given back as:
    {
        [temporalMethod]: {
            date: Date,
            projects: [{ project: string, percentage: float, size: int }]
        }
    }
    """
    project_list = connection.get_and_check_access_to_projects_for_names(
        projects, allowed_roles=ReadAccessRoles
    )
    project_ids = [p.id for p in project_list]

    start_date = parse_date_only_string(start) if start else None
    end_date = parse_date_only_string(end) if end else None

    at = AnalysisLayer(connection)
    results = await at.get_cram_size_proportionate_map(
        projects=project_ids,
        sequencing_types=sequencing_types,
        start_date=start_date,
        end_date=end_date,
        temporal_methods=temporal_methods,
    )

    return {k.value: v for k, v in results.items()}
