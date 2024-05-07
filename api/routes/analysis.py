import csv
import io
from datetime import date
from typing import Any

from fastapi import APIRouter
from fastapi.params import Body, Query
from pydantic import BaseModel
from starlette.responses import StreamingResponse

from api.utils.dates import parse_date_only_string
from api.utils.db import (
    Connection,
    get_project_readonly_connection,
    get_project_write_connection,
    get_projectless_db_connection,
)
from api.utils.export import ExportType
from db.python.layers.analysis import AnalysisLayer
from db.python.tables.analysis import AnalysisFilter
from db.python.tables.project import ProjectPermissionsTable
from db.python.utils import GenericFilter
from models.enums import AnalysisStatus
from models.models.analysis import (
    Analysis,
    AnalysisInternal,
    ProportionalDateTemporalMethod,
)
from models.utils.sample_id_format import sample_id_transform_to_raw_list
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
    active: bool = True
    # please don't use this, unless you're the analysis-runner,
    # the usage is tracked ... (Ծ_Ծ)
    author: str | None = None


class AnalysisUpdateModel(BaseModel):
    """Update analysis model"""

    status: AnalysisStatus
    output: str | None = None
    meta: dict[str, Any] | None = None
    active: bool | None = None


class AnalysisQueryModel(BaseModel):
    """Used to query for many analysis"""

    # sequencing_group_ids means it the analysis contains at least one of in the list
    sample_ids: list[str] | None = None
    sequencing_group_ids: list[str] | None
    # # sample_ids_all means the analysis contains ALL of the sample_ids
    # sample_ids_all: list[str] = None
    projects: list[str]
    type: str | None = None
    status: AnalysisStatus | None = None
    meta: dict[str, Any] | None = None
    output: str | None = None
    active: bool | None = None

    def to_filter(self, project_id_map: dict[str, int]) -> AnalysisFilter:
        """Convert to internal analysis filter"""
        return AnalysisFilter(
            sample_id=(
                GenericFilter(in_=sample_id_transform_to_raw_list(self.sample_ids))
                if self.sample_ids
                else None
            ),
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
    analysis: Analysis, connection: Connection = get_project_write_connection
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
        analysis_id, status=analysis.status, output=analysis.output, meta=analysis.meta
    )
    return True


@router.get(
    '/{project}/type/{analysis_type}/not-included-sample-ids',
    operation_id='getAllSampleIdsWithoutAnalysisType',
)
async def get_all_sample_ids_without_analysis_type(
    analysis_type: str,
    connection: Connection = get_project_readonly_connection,
):
    """get_all_sample_ids_without_analysis_type"""
    atable = AnalysisLayer(connection)
    assert connection.project
    sequencing_group_ids = (
        await atable.get_all_sequencing_group_ids_without_analysis_type(
            connection.project, analysis_type
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
    connection: Connection = get_project_readonly_connection,
):
    """Get analyses with status queued or in-progress"""
    atable = AnalysisLayer(connection)
    assert connection.project
    results = await atable.get_incomplete_analyses(project=connection.project)
    return [r.to_external() for r in results]


@router.get(
    '/{project}/{analysis_type}/latest-complete',
    operation_id='getLatestCompleteAnalysisForType',
)
async def get_latest_complete_analysis_for_type(
    analysis_type: str,
    connection: Connection = get_project_readonly_connection,
):
    """Get (SINGLE) latest complete analysis for some analysis type"""
    alayer = AnalysisLayer(connection)
    assert connection.project
    analysis = await alayer.get_latest_complete_analysis_for_type(
        project=connection.project, analysis_type=analysis_type
    )
    return analysis.to_external()


@router.post(
    '/{project}/{analysis_type}/latest-complete',
    operation_id='getLatestCompleteAnalysisForTypePost',
)
async def get_latest_complete_analysis_for_type_post(
    analysis_type: str,
    meta: dict[str, Any] = Body(..., embed=True),  # type: ignore[assignment]
    connection: Connection = get_project_readonly_connection,
):
    """
    Get SINGLE latest complete analysis for some analysis type
    (you can specify meta attributes in this route)
    """
    alayer = AnalysisLayer(connection)
    assert connection.project
    analysis = await alayer.get_latest_complete_analysis_for_type(
        project=connection.project,
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

    pt = ProjectPermissionsTable(connection)
    projects = await pt.get_and_check_access_to_projects_for_names(
        user=connection.author, project_names=query.projects, readonly=True
    )
    project_name_map = {p.name: p.id for p in projects}
    atable = AnalysisLayer(connection)
    analyses = await atable.query(query.to_filter(project_name_map))
    return [a.to_external() for a in analyses]


@router.get('/analysis-runner', operation_id='getAnalysisRunnerLog')
async def get_analysis_runner_log(
    project_names: list[str] = Query(None),  # type: ignore
    # author: str = None, # not implemented yet, uncomment when we do
    output_dir: str = None,
    ar_guid: str = None,
    connection: Connection = get_projectless_db_connection,
) -> list[AnalysisInternal]:
    """
    Get log for the analysis-runner, useful for checking this history of analysis
    """
    atable = AnalysisLayer(connection)
    project_ids = None
    if project_names:
        pt = ProjectPermissionsTable(connection)
        project_ids = await pt.get_project_ids_from_names_and_user(
            connection.author, project_names, readonly=True
        )

    results = await atable.get_analysis_runner_log(
        project_ids=project_ids,
        # author=author,
        output_dir=output_dir,
        ar_guid=ar_guid,
    )
    return [a.to_external() for a in results]


@router.get(
    '/{project}/sample-cram-path-map',
    operation_id='getSamplesReadsMap',
    tags=['seqr'],
)
async def get_sample_reads_map(
    export_type: ExportType = ExportType.JSON,
    sequencing_types: list[str] = Query(None),  # type: ignore
    connection: Connection = get_project_readonly_connection,
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
    assert connection.project
    objs = await at.get_sample_cram_path_map_for_seqr(
        project=connection.project, sequencing_types=sequencing_types
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

    basefn = f'{connection.project}-seqr-igv-paths-{date.today().isoformat()}'

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
    pt = ProjectPermissionsTable(connection)
    project_ids = await pt.get_project_ids_from_names_and_user(
        connection.author, projects, readonly=True
    )

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
