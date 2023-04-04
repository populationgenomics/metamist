import io
import csv
from datetime import date

from typing import Any

from fastapi import APIRouter
from fastapi.params import Body, Query
from pydantic import BaseModel
from starlette.responses import StreamingResponse
from api.utils.dates import parse_date_only_string

from api.utils.db import (
    get_projectless_db_connection,
    get_project_readonly_connection,
    get_project_write_connection,
    Connection,
)
from api.utils.export import ExportType
from db.python.layers.analysis import AnalysisLayer
from db.python.tables.project import ProjectPermissionsTable
from models.enums import AnalysisType, AnalysisStatus
from models.models.analysis import (
    Analysis,
    ProjectSizeModel,
    SampleSizeModel,
    DateSizeModel,
)
from models.utils.sample_id_format import (
    sample_id_transform_to_raw_list,
    sample_id_format_list,
    sample_id_format,
)


router = APIRouter(prefix='/analysis', tags=['analysis'])


class AnalysisModel(BaseModel):
    """
    Model for 'createNewAnalysis'
    """

    sample_ids: list[str]
    type: AnalysisType
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

    # sample_ids means it contains the analysis contains at least one of the sample_ids in the list
    sample_ids: list[str] | None
    # # sample_ids_all means the analysis contains ALL of the sample_ids
    # sample_ids_all: list[str] = None
    projects: list[str]
    type: AnalysisType | None = None
    status: AnalysisStatus | None = None
    meta: dict[str, Any] | None = None
    output: str | None = None
    active: bool | None = None


@router.put('/{project}/', operation_id='createAnalysis', response_model=int)
async def create_analysis(
    analysis: AnalysisModel, connection: Connection = get_project_write_connection
):
    """Create a new analysis"""

    atable = AnalysisLayer(connection)

    sample_ids = sample_id_transform_to_raw_list(analysis.sample_ids)

    analysis_id = await atable.insert_analysis(
        analysis_type=analysis.type,
        status=analysis.status,
        sample_ids=sample_ids,
        meta=analysis.meta,
        output=analysis.output,
        # analysis-runner: usage is tracked through `on_behalf_of`
        author=analysis.author,
    )

    return analysis_id


@router.patch('/{analysis_id}/', operation_id='updateAnalysisStatus')
async def update_analysis_status(
    analysis_id: int,
    analysis: AnalysisUpdateModel,
    connection: Connection = get_projectless_db_connection,
):
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
    analysis_type: AnalysisType,
    connection: Connection = get_project_readonly_connection,
):
    """get_all_sample_ids_without_analysis_type"""
    atable = AnalysisLayer(connection)
    assert connection.project
    result = await atable.get_all_sample_ids_without_analysis_type(
        connection.project, analysis_type
    )
    return {'sample_ids': sample_id_format_list(result)}


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
    if results:
        for result in results:
            if result.sample_ids:
                result.sample_ids = list(sample_id_format_list(result.sample_ids))

    return results


@router.get(
    '/{project}/{analysis_type}/latest-complete',
    operation_id='getLatestCompleteAnalysisForType',
)
async def get_latest_complete_analysis_for_type(
    analysis_type: AnalysisType,
    connection: Connection = get_project_readonly_connection,
):
    """Get (SINGLE) latest complete analysis for some analysis type"""
    alayer = AnalysisLayer(connection)
    assert connection.project
    analysis = await alayer.get_latest_complete_analysis_for_type(
        project=connection.project, analysis_type=analysis_type
    )
    if analysis and analysis.sample_ids:
        analysis.sample_ids = list(sample_id_format_list(analysis.sample_ids))

    return analysis


@router.post(
    '/{project}/{analysis_type}/latest-complete',
    operation_id='getLatestCompleteAnalysisForTypePost',
)
async def get_latest_complete_analysis_for_type_post(
    analysis_type: AnalysisType,
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
    if analysis and analysis.sample_ids:
        analysis.sample_ids = list(sample_id_format_list(analysis.sample_ids))

    return analysis


@router.post(
    '/{project}/{analysis_type}/latest-complete/for-samples',
    operation_id='getLatestAnalysisForSamplesAndType',
)
async def get_latest_analysis_for_samples_and_type(
    sample_ids: list[str],
    analysis_type: AnalysisType,
    allow_missing: bool = True,
    connection: Connection = get_project_readonly_connection,
):
    """
    Get latest complete analysis for samples and type.
    Should return one per sample.
    """
    atable = AnalysisLayer(connection)
    results = await atable.get_latest_complete_analysis_for_samples_and_type(
        analysis_type=analysis_type,
        sample_ids=sample_id_transform_to_raw_list(sample_ids),
        allow_missing=allow_missing,
    )

    if results:
        for result in results:
            result.sample_ids = sample_id_format_list(result.sample_ids)

    return results


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
    result.sample_ids = sample_id_format_list(result.sample_ids)
    return result


@router.post('/query', operation_id='queryAnalyses')
async def query_analyses(
    query: AnalysisQueryModel, connection: Connection = get_projectless_db_connection
):
    """Get analyses by some criteria"""
    if not query.projects:
        raise ValueError('Must specify "projects"')

    pt = ProjectPermissionsTable(connection=connection.connection)
    project_ids = await pt.get_project_ids_from_names_and_user(
        connection.author, query.projects, readonly=True
    )
    atable = AnalysisLayer(connection)
    analyses = await atable.query_analysis(
        sample_ids=sample_id_transform_to_raw_list(query.sample_ids)
        if query.sample_ids
        else None,
        project_ids=project_ids,
        analysis_type=query.type,
        status=query.status,
        meta=query.meta,
        output=query.output,
        active=query.active,
    )
    for analysis in analyses:
        analysis.sample_ids = sample_id_format_list(analysis.sample_ids)

    return analyses


@router.get('/analysis-runner', operation_id='getAnalysisRunnerLog')
async def get_analysis_runner_log(
    project_names: list[str] = Query(None),  # type: ignore
    author: str = None,
    output_dir: str = None,
    connection: Connection = get_projectless_db_connection,
) -> list[Analysis]:
    """
    Get log for the analysis-runner, useful for checking this history of analysis
    """
    atable = AnalysisLayer(connection)
    project_ids = None
    if project_names:
        pt = ProjectPermissionsTable(connection=connection.connection)
        project_ids = await pt.get_project_ids_from_names_and_user(
            connection.author, project_names, readonly=True
        )

    results = await atable.get_analysis_runner_log(
        project_ids=project_ids, author=author, output_dir=output_dir
    )
    return results


@router.get(
    '/{project}/sample-cram-path-map',
    operation_id='getSamplesReadsMap',
    tags=['seqr'],
)
async def get_sample_reads_map(
    export_type: ExportType = ExportType.JSON,
    sequence_types: list[str] = Query(None),  # type: ignore
    connection: Connection = get_project_readonly_connection,
):
    """
    Get map of ExternalSampleId  pathToCram  InternalSampleID for seqr

    Description:
        Column 1: Individual ID
        Column 2: gs:// Google bucket path or server filesystem path for this Individual
        Column 3: Sample ID for this file, if different from the Individual ID.
                    Used primarily for gCNV files to identify the sample in the batch path
    """

    at = AnalysisLayer(connection)
    assert connection.project
    objs = await at.get_sample_cram_path_map_for_seqr(
        project=connection.project, sequence_types=sequence_types
    )

    for r in objs:
        r['sample_id'] = sample_id_format(r['sample_id'])

    if export_type == ExportType.JSON:
        return objs

    keys = ['participant_id', 'output', 'sample_id']
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


@router.get('/sample-file-sizes', operation_id='getSampleFileSizes')
async def get_sample_file_sizes(
    project_names: list[str] = Query(None),  # type: ignore
    start_date: str = None,
    end_date: str = None,
    connection: Connection = get_projectless_db_connection,
) -> list[ProjectSizeModel]:
    """
    Get the per sample file size by type over the given projects and date range
    """
    atable = AnalysisLayer(connection)

    # Check access to projects
    project_ids = None
    pt = ProjectPermissionsTable(connection=connection.connection)
    project_ids = await pt.get_project_ids_from_names_and_user(
        connection.author, project_names, readonly=True
    )

    # Map from internal pids to project name
    prj_name_map = dict(zip(project_ids, project_names))

    # Convert dates
    start = parse_date_only_string(start_date)
    end = parse_date_only_string(end_date)

    # Get results with internal ids as keys
    results = await atable.get_sample_file_sizes(
        project_ids=project_ids, start_date=start, end_date=end
    )

    # Convert to the correct output type, converting internal ids to external
    fixed_pids: list[Any] = [
        ProjectSizeModel(
            project=prj_name_map[project_data['project']],
            samples=[
                SampleSizeModel(
                    sample=sample_id_format(s['sample']),
                    dates=[DateSizeModel(**d) for d in s['dates']],
                )
                for s in project_data['samples']
            ],
        )
        for project_data in results
    ]

    return fixed_pids
