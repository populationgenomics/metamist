"""
Web routes
"""
from typing import Optional

from fastapi import APIRouter, Request
from pydantic import BaseModel

from api.utils.db import (
    get_project_readonly_connection,
    get_projectless_db_connection,
    get_project_write_connection,
    Connection,
)
from db.python.layers.search import SearchLayer
from db.python.layers.seqr import SeqrLayer
from db.python.layers.web import WebLayer, SearchItem
from db.python.tables.project import ProjectPermissionsTable
from models.models.search import SearchResponse
from models.models.web import ProjectsSummaryInternal, ProjectSummary, PagingLinks


class SearchResponseModel(BaseModel):
    """Parent model class, allows flexibility later on"""

    responses: list[SearchResponse]


router = APIRouter(prefix='/web', tags=['web'])


@router.post(
    '/{project}/summary',
    response_model=ProjectSummary,
    operation_id='getProjectSummary',
)
async def get_project_summary(
    request: Request,
    grid_filter: list[SearchItem],
    limit: int = 20,
    token: Optional[int] = 0,
    connection: Connection = get_project_readonly_connection,
) -> ProjectSummary:
    """Creates a new sample, and returns the internal sample ID"""
    st = WebLayer(connection)

    summary = await st.get_project_summary(
        token=token, limit=limit, grid_filter=grid_filter
    )

    participants = summary.participants

    collected_samples = sum(len(p.samples) for p in participants)
    new_token = None
    if collected_samples >= limit:
        new_token = max(int(sample.id) for p in participants for sample in p.samples)

    links = PagingLinks(
        next=str(request.base_url)
        + request.url.path.lstrip('/')
        + f'?token={new_token}'
        if new_token
        else None,
        self=str(request.url),
        token=str(new_token) if new_token else None,
    )

    return summary.to_external(links)


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


@router.post(
    '/{project}/{sequencing_type}/sync-dataset', operation_id='syncSeqrProject'
)
async def sync_seqr_project(
    sequencing_type: str,
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
            sequencing_type,
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


@router.post('/projects-summary', operation_id='getProjectsSummary')
async def get_projects_summary(
    projects: list[int] = None, sequencing_types: list[str] = None
):
    """
    Get the project summaries
    """
    return [
        ProjectsSummaryInternal(
            project=1,
            dataset='test-dataset-1',
            sequencing_type='genome',
            total_families=1,
            total_participants=3,
            total_samples=3,
            total_sequencing_groups=3,
            total_crams=3,
            latest_es_index_id=1,
            total_sgs_in_latest_es_index=3,
            latest_annotate_dataset_id=2,
            total_sgs_in_latest_annotate_dataset=3,
        ),
        ProjectsSummaryInternal(
            project=1,
            dataset='test-dataset-1',
            sequencing_type='exome',
            total_families=1,
            total_participants=1,
            total_samples=1,
            total_sequencing_groups=1,
            total_crams=1,
            latest_es_index_id=3,
            total_sgs_in_latest_es_index=1,
            latest_annotate_dataset_id=4,
            total_sgs_in_latest_annotate_dataset=1,
        ),
        ProjectsSummaryInternal(
            project=2,
            dataset='test-dataset-2',
            sequencing_type='genome',
            total_families=5,
            total_participants=5,
            total_samples=5,
            total_sequencing_groups=5,
            total_crams=4,
            latest_es_index_id=3,
            total_sgs_in_latest_es_index=4,
            latest_annotate_dataset_id=6,
            total_sgs_in_latest_annotate_dataset=4,
        ),
    ]
    return await WebLayer(get_projectless_db_connection).get_projects_summary(
        sequencing_types=sequencing_types
    )
