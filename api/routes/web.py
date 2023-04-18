"""
Web routes
"""
from typing import Optional

from fastapi import APIRouter, Request
from pydantic import BaseModel

from db.python.layers.search import SearchLayer
from db.python.layers.seqr import SeqrLayer
from db.python.tables.project import ProjectPermissionsTable
from db.python.layers.web import WebLayer, NestedParticipant, SearchItem

from models.models.sample import sample_id_format
from models.models.search import SearchResponse

from api.utils.db import (
    get_project_readonly_connection,
    get_projectless_db_connection,
    get_project_write_connection,
    Connection,
)


class PagingLinks(BaseModel):
    """Model for PAGING"""

    self: str
    next: str | None
    token: str | None


class WebProject(BaseModel):
    """Minimal class to hold web info"""

    id: int
    name: str
    dataset: str
    meta: dict


class ProjectSummaryResponse(BaseModel):
    """Response for the project summary"""

    project: WebProject
    # high level stats
    total_participants: int
    total_samples: int
    total_samples_in_query: int
    total_sequences: int
    cram_seqr_stats: dict[str, dict[str, str]]  # {seqType: {seqr/seq/cram count: }}
    batch_sequence_stats: dict[str, dict[str, str]]  # {batch: {seqType: }}

    # for display
    participants: list[NestedParticipant]
    participant_keys: list[list[str]]
    sample_keys: list[list[str]]
    sequence_keys: list[list[str]]
    seqr_links: dict[str, str]
    seqr_sync_types: list[str]

    links: PagingLinks | None

    class Config:
        """Config for ProjectSummaryResponse"""

        fields = {'links': '_links'}


class SearchResponseModel(BaseModel):
    """Parent model class, allows flexibility later on"""

    responses: list[SearchResponse]


router = APIRouter(prefix='/web', tags=['web'])


@router.post(
    '/{project}/summary',
    response_model=ProjectSummaryResponse,
    operation_id='getProjectSummary',
)
async def get_project_summary(
    request: Request,
    grid_filter: list[SearchItem],
    limit: int = 20,
    token: Optional[int] = 0,
    connection: Connection = get_project_readonly_connection,
) -> ProjectSummaryResponse:
    """Creates a new sample, and returns the internal sample ID"""
    st = WebLayer(connection)

    summary = await st.get_project_summary(
        token=token, limit=limit, grid_filter=grid_filter
    )

    if len(summary.participants) == 0:
        return ProjectSummaryResponse(
            project=WebProject(**summary.project.__dict__),
            participants=[],
            participant_keys=[],
            sample_keys=[],
            sequence_keys=[],
            _links=None,
            total_samples=0,
            total_samples_in_query=0,
            total_participants=0,
            total_sequences=0,
            cram_seqr_stats={},
            batch_sequence_stats={},
            seqr_links=summary.seqr_links,
            seqr_sync_types=[],
        )

    participants = summary.participants

    collected_samples = sum(len(p.samples) for p in participants)
    new_token = None
    if collected_samples >= limit:
        new_token = max(int(sample.id) for p in participants for sample in p.samples)

    for participant in participants:
        for sample in participant.samples:
            sample.id = sample_id_format(sample.id)

    links = PagingLinks(
        next=str(request.base_url)
        + request.url.path.lstrip('/')
        + f'?token={new_token}'
        if new_token
        else None,
        self=str(request.url),
        token=str(new_token) if new_token else None,
    )

    return ProjectSummaryResponse(
        project=WebProject(**summary.project.__dict__),
        total_samples=summary.total_samples,
        total_samples_in_query=summary.total_samples_in_query,
        total_participants=summary.total_participants,
        total_sequences=summary.total_sequences,
        cram_seqr_stats=summary.cram_seqr_stats,
        batch_sequence_stats=summary.batch_sequence_stats,
        # other stuff
        participants=participants,
        participant_keys=summary.participant_keys,
        sample_keys=summary.sample_keys,
        sequence_keys=summary.assay_keys,
        seqr_links=summary.seqr_links,
        seqr_sync_types=summary.seqr_sync_types,
        _links=links,
    )


@router.get(
    '/search', response_model=SearchResponseModel, operation_id='searchByKeyword'
)
async def search_by_keyword(keyword: str, connection=get_projectless_db_connection):
    """
    This searches the keyword, in families, participants + samples in the projects
    that you are a part of (automatically).
    """
    # raise ValueError("Test")
    pt = ProjectPermissionsTable(connection.connection)
    projects = await pt.get_projects_accessible_by_user(
        connection.author, readonly=True
    )
    project_ids = list(projects.keys())
    responses = await SearchLayer(connection).search(keyword, project_ids=project_ids)

    for res in responses:
        res.data.project = projects.get(res.data.project, res.data.project)  # type: ignore

    return SearchResponseModel(responses=responses)


@router.post('/{project}/{sequence_type}/sync-dataset', operation_id='syncSeqrProject')
async def sync_seqr_project(
    sequence_type: str,
    sync_families: bool = True,
    sync_individual_metadata: bool = True,
    sync_individuals: bool = True,
    sync_es_index: bool = True,
    sync_saved_variants: bool = True,
    sync_cram_map: bool = True,
    post_slack_notification: bool = True,
    connection=get_project_write_connection,
):
    """
    Sync a metamist project with its seqr project (for a specific sequence type)
    """
    seqr = SeqrLayer(connection)
    try:
        data = await seqr.sync_dataset(
            sequence_type,
            sync_families=sync_families,
            sync_individual_metadata=sync_individual_metadata,
            sync_individuals=sync_individuals,
            sync_es_index=sync_es_index,
            sync_saved_variants=sync_saved_variants,
            sync_cram_map=sync_cram_map,
            post_slack_notification=post_slack_notification,
        )
        return {'success': 'errors' not in data, **data}
    except Exception as e:
        raise ConnectionError(f'Failed to synchronise seqr project: {str(e)}') from e
        # return {'success': False, 'message': str(e)}
