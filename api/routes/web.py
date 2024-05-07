"""
Web routes
"""
from typing import Optional

from fastapi import APIRouter, Request
from pydantic import BaseModel

from api.utils.db import (
    Connection,
    get_project_readonly_connection,
    get_project_write_connection,
    get_projectless_db_connection,
)
from db.python.layers.search import SearchLayer
from db.python.layers.seqr import SeqrLayer
from db.python.layers.web import SearchItem, WebLayer
from db.python.tables.project import ProjectPermissionsTable
from models.enums.web import SeqrDatasetType
from models.models.search import SearchResponse
from models.models.web import PagingLinks, ProjectSummary


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
    pt = ProjectPermissionsTable(connection)
    projects = await pt.get_projects_accessible_by_user(
        connection.author, readonly=True
    )
    pmap = {p.id: p for p in projects}
    responses = await SearchLayer(connection).search(
        keyword, project_ids=list(pmap.keys())
    )

    for res in responses:
        if res.data.project in pmap:
            # the solution to the type issue is to create internal / external models
            # and convert between them for transport
            res.data.project = pmap[res.data.project].name  # type: ignore
        else:
            res.data.project = str(res.data.project)

    return SearchResponseModel(responses=responses)


@router.post(
    '/{project}/{sequencing_type}/sync-dataset', operation_id='syncSeqrProject'
)
async def sync_seqr_project(
    sequencing_type: str,
    es_index_types: list[SeqrDatasetType],
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
    es_index_types: list of any of 'Haplotypecaller', 'SV_Caller', 'Mitochondria_Caller'
    """
    seqr = SeqrLayer(connection)
    try:
        data = await seqr.sync_dataset(
            sequencing_type,
            sync_families=sync_families,
            sync_individual_metadata=sync_individual_metadata,
            sync_individuals=sync_individuals,
            sync_es_index=sync_es_index,
            es_index_types=es_index_types,
            sync_saved_variants=sync_saved_variants,
            sync_cram_map=sync_cram_map,
            post_slack_notification=post_slack_notification,
        )
        return {'success': 'errors' not in data, **data}
    except Exception as e:
        raise ConnectionError(f'Failed to synchronise seqr project: {str(e)}') from e
        # return {'success': False, 'message': str(e)}
