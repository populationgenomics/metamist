# pylint: disable=kwarg-superseded-by-positional-arg
"""
Web routes
"""

import asyncio

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
from db.python.layers.web import WebLayer
from db.python.tables.participant import ParticipantFilter
from db.python.tables.project import ProjectPermissionsTable
from db.python.utils import GenericFilter, GenericMetaFilter
from models.enums.web import SeqrDatasetType
from models.models.search import SearchResponse
from models.models.web import ProjectParticipantGridResponse, ProjectSummary


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
    connection: Connection = get_project_readonly_connection,
) -> ProjectSummary:
    """Creates a new sample, and returns the internal sample ID"""
    st = WebLayer(connection)

    summary = await st.get_project_summary()

    return summary.to_external()


class ProjectParticipantGridFilter(BaseModel):
    class ParticipantGridFamilyFilter(BaseModel):
        """
        Participant sample filter model
        """

        id: GenericFilter[int] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None

    class ParticipantGridSampleFilter(BaseModel):
        """
        Participant sample filter model
        """

        id: GenericFilter[int] | None = None
        type: GenericFilter[str] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None

    class ParticipantGridSequencingGroupFilter(BaseModel):
        """
        Participant sequencing group filter model
        """

        id: GenericFilter[int] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None
        type: GenericFilter[str] | None = None
        technology: GenericFilter[str] | None = None
        platform: GenericFilter[str] | None = None

    class ParticipantGridAssayFilter(BaseModel):
        """
        Participant assay filter model
        """

        id: GenericFilter[int] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None
        type: GenericFilter[str] | None = None
        technology: GenericFilter[str] | None = None
        platform: GenericFilter[str] | None = None

    id: GenericFilter[int] | None = None
    meta: GenericMetaFilter | None = None
    external_id: GenericFilter[str] | None = None
    reported_sex: GenericFilter[int] | None = None
    reported_gender: GenericFilter[str] | None = None
    karyotype: GenericFilter[str] | None = None

    family: ParticipantGridFamilyFilter | None = None
    sample: ParticipantGridSampleFilter | None = None
    sequencing_group: ParticipantGridSequencingGroupFilter | None = None
    assay: ParticipantGridAssayFilter | None = None


@router.post(
    '/{project}/participants',
    response_model=ProjectParticipantGridResponse,
    operation_id='getProjectParticipantsGridWithLimit',
)
async def get_project_summary_with_limit(
    request: Request,
    limit: int,
    query: ProjectParticipantGridFilter,
    token: int | None = None,
    connection=get_project_readonly_connection,
):

    if not connection.project:
        raise ValueError("No project was detected through the authentication")

    wlayer = WebLayer(connection)
    id_query = query.id
    if token:
        id_query = id_query or GenericFilter()
        id_query.gt = int(token)
    pfilter = ParticipantFilter(
        id=id_query,
        external_id=query.external_id,
        reported_sex=query.reported_sex,
        reported_gender=query.reported_gender,
        karyotype=query.karyotype,
        meta=query.meta,
        project=GenericFilter(eq=connection.project),
        # nested models
        family=(
            ParticipantFilter.ParticipantFamilyFilter(
                id=query.family.id,
                external_id=query.family.external_id,
                meta=query.family.meta,
            )
            if query.family
            else None
        ),
        sample=(
            ParticipantFilter.ParticipantSampleFilter(
                id=query.sample.id,
                type=query.sample.type,
                external_id=query.sample.external_id,
                meta=query.sample.meta,
            )
            if query.sample
            else None
        ),
        sequencing_group=(
            ParticipantFilter.ParticipantSequencingGroupFilter(
                id=query.sequencing_group.id,
                type=query.sequencing_group.type,
                external_id=query.sequencing_group.external_id,
                meta=query.sequencing_group.meta,
                technology=query.sequencing_group.technology,
                platform=query.sequencing_group.platform,
            )
            if query.sequencing_group
            else None
        ),
        assay=(
            ParticipantFilter.ParticipantAssayFilter(
                id=query.assay.id,
                type=query.assay.type,
                external_id=query.assay.external_id,
                meta=query.assay.meta,
            )
            if query.assay
            else None
        ),
    )

    participants, pcount = await asyncio.gather(
        wlayer.query_participants(pfilter, limit=limit),
        wlayer.count_participants(pfilter),
    )

    # then have to get all nested objects

    return ProjectParticipantGridResponse.construct(
        participants=participants,
        total_results=pcount,
        token_base_url=str(request.base_url) + request.url.path.lstrip('/'),
        current_url=str(request.url),
        request_limit=limit,
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
