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
    get_projectless_db_connection,
    get_project_readonly_connection,
    get_project_write_connection,
    Connection,
)
from api.utils.export import ExportType
from db.python.layers.analysis import AnalysisLayer
from db.python.tables.project import ProjectPermissionsTable
from models.enums import AnalysisStatus
from models.models.analysis import (
    AnalysisInternal,
    ProjectSizeModel,
    SequencingGroupSizeModel,
    DateSizeModel,
    Analysis,
)
from models.utils.sample_id_format import (
    sample_id_transform_to_raw_list,
    sample_id_format,
)
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_format_list,
    sequencing_group_id_transform_to_raw_list,
    sequencing_group_id_format,
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


@router.put('/{project}/', operation_id='createAnalysis', response_model=int)
async def create_analysis(
    analysis: Analysis, connection: Connection = get_project_write_connection
) -> int:
    """Create a new analysis"""

    atable = AnalysisLayer(connection)

    analysis_id = await atable.create_analysis(
        analysis.to_internal(),
        # analysis-runner: usage is tracked through `on_behalf_of`
        author=analysis.author,
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


@router.post(
    '/{project}/{analysis_type}/latest-complete/for-samples',
    operation_id='getLatestAnalysisForSamplesAndType',
)
async def get_latest_analysis_for_samples_and_type(
    sample_ids: list[str],
    analysis_type: str,
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

    return [r.to_external() for r in results]


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

    pt = ProjectPermissionsTable(connection=connection.connection)
    project_ids = await pt.get_project_ids_from_names_and_user(
        connection.author, query.projects, readonly=True
    )
    atable = AnalysisLayer(connection)
    analyses = await atable.query_analysis(
        sample_ids=sample_id_transform_to_raw_list(query.sample_ids)
        if query.sample_ids
        else None,
        sequencing_group_ids=sequencing_group_id_transform_to_raw_list(
            query.sequencing_group_ids
        ),
        project_ids=project_ids,
        analysis_type=query.type,
        status=query.status,
        meta=query.meta,
        output=query.output,
        active=query.active,
    )
    return [a.to_external() for a in analyses]


@router.get('/analysis-runner', operation_id='getAnalysisRunnerLog')
async def get_analysis_runner_log(
    project_names: list[str] = Query(None),  # type: ignore
    author: str = None,
    output_dir: str = None,
    connection: Connection = get_projectless_db_connection,
) -> list[AnalysisInternal]:
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
    return [a.to_external() for a in results]


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
        project=connection.project, sequence_types=sequence_types
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


@router.get('/sample-file-sizes', operation_id='getSequencingGroupFileSizes')
async def get_sequencing_group_file_sizes(
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
    results = await atable.get_sequencing_group_file_sizes(
        project_ids=project_ids, start_date=start, end_date=end
    )

    # Convert to the correct output type, converting internal ids to external
    fixed_pids: list[Any] = [
        ProjectSizeModel(
            project=prj_name_map[project_data['project']],
            samples=[
                SequencingGroupSizeModel(
                    sample=sample_id_format(s['sample']),
                    dates=[DateSizeModel(**d) for d in s['dates']],
                )
                for s in project_data['samples']
            ],
        )
        for project_data in results
    ]

    return fixed_pids
