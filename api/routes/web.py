# pylint: disable=kwarg-superseded-by-positional-arg
"""
Web routes
"""

import asyncio
import csv
import io
from datetime import date

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from api.utils.db import (
    Connection,
    get_project_readonly_connection,
    get_project_write_connection,
    get_projectless_db_connection,
)
from api.utils.export import ExportType
from db.python.layers.search import SearchLayer
from db.python.layers.seqr import SeqrLayer
from db.python.layers.web import WebLayer
from db.python.tables.participant import ParticipantFilter
from db.python.tables.project import ProjectPermissionsTable
from db.python.utils import GenericFilter, GenericMetaFilter
from models.enums.web import SeqrDatasetType
from models.models.project import ProjectId
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
    connection: Connection = get_project_readonly_connection,
) -> ProjectSummary:
    """Creates a new sample, and returns the internal sample ID"""
    st = WebLayer(connection)

    summary = await st.get_project_summary()

    return summary.to_external()


class ProjectParticipantGridFilter(BaseModel):
    """filters for participant grid"""

    class ParticipantGridParticipantFilter(BaseModel):
        """participant filter option for participant grid"""

        id: GenericFilter[int] | None = None
        meta: GenericMetaFilter | None = None
        external_id: GenericFilter[str] | None = None
        reported_sex: GenericFilter[int] | None = None
        reported_gender: GenericFilter[str] | None = None
        karyotype: GenericFilter[str] | None = None

    class ParticipantGridFamilyFilter(BaseModel):
        """family filter option for participant grid"""

        id: GenericFilter[int] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None

    class ParticipantGridSampleFilter(BaseModel):
        """sample filter option for participant grid"""

        id: GenericFilter[int] | None = None
        type: GenericFilter[str] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None

    class ParticipantGridSequencingGroupFilter(BaseModel):
        """sequencing group filter option for participant grid"""

        id: GenericFilter[int] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None
        type: GenericFilter[str] | None = None
        technology: GenericFilter[str] | None = None
        platform: GenericFilter[str] | None = None

    class ParticipantGridAssayFilter(BaseModel):
        """assay filter option for participant grid"""

        id: GenericFilter[int] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None
        type: GenericFilter[str] | None = None
        technology: GenericFilter[str] | None = None
        platform: GenericFilter[str] | None = None

    family: ParticipantGridFamilyFilter | None = None
    participant: ParticipantGridParticipantFilter | None = None
    sample: ParticipantGridSampleFilter | None = None
    sequencing_group: ParticipantGridSequencingGroupFilter | None = None
    assay: ParticipantGridAssayFilter | None = None

    def to_internal(
        self, project: ProjectId, id_query: GenericFilter[int] | None = None
    ) -> ParticipantFilter:
        """Convert to participant internal filter object"""
        return ParticipantFilter(
            id=id_query or (self.participant.id if self.participant else None),
            external_id=self.participant.external_id if self.participant else None,
            reported_sex=self.participant.reported_sex if self.participant else None,
            reported_gender=(
                self.participant.reported_gender if self.participant else None
            ),
            karyotype=self.participant.karyotype if self.participant else None,
            meta=self.participant.meta if self.participant else None,
            project=GenericFilter(eq=project),
            # nested models
            family=(
                ParticipantFilter.ParticipantFamilyFilter(
                    id=self.family.id,
                    external_id=self.family.external_id,
                    meta=self.family.meta,
                )
                if self.family
                else None
            ),
            sample=(
                ParticipantFilter.ParticipantSampleFilter(
                    id=self.sample.id,
                    type=self.sample.type,
                    external_id=self.sample.external_id,
                    meta=self.sample.meta,
                )
                if self.sample
                else None
            ),
            sequencing_group=(
                ParticipantFilter.ParticipantSequencingGroupFilter(
                    id=self.sequencing_group.id,
                    type=self.sequencing_group.type,
                    external_id=self.sequencing_group.external_id,
                    meta=self.sequencing_group.meta,
                    technology=self.sequencing_group.technology,
                    platform=self.sequencing_group.platform,
                )
                if self.sequencing_group
                else None
            ),
            assay=(
                ParticipantFilter.ParticipantAssayFilter(
                    id=self.assay.id,
                    type=self.assay.type,
                    external_id=self.assay.external_id,
                    meta=self.assay.meta,
                )
                if self.assay
                else None
            ),
        )


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
    """Get project summary (from query) with some limit"""

    if not connection.project:
        raise ValueError('No project was detected through the authentication')

    wlayer = WebLayer(connection)
    id_query = query.participant.id if query.participant else None
    if token:
        id_query = id_query or GenericFilter()
        id_query.gt = int(token)
    pfilter = query.to_internal(id_query=id_query, project=connection.project)

    participants, pcount = await asyncio.gather(
        wlayer.query_participants(pfilter, limit=limit),
        wlayer.count_participants(pfilter),
    )

    # then have to get all nested objects

    return ProjectParticipantGridResponse.from_params(
        participants=participants,
        total_results=pcount,
        token_base_url=str(request.base_url) + request.url.path.lstrip('/'),
        current_url=str(request.url),
        request_limit=limit,
    )


@router.post(
    '/{project}/participants/export/{export_type}',
    operation_id='exportProjectParticipants',
)
async def export_project_summary(
    export_type: ExportType,
    query: ProjectParticipantGridFilter,
    connection=get_project_readonly_connection,
):
    """Get project summary (from query) with some limit"""

    if not connection.project:
        raise ValueError('No project was detected through the authentication')

    wlayer = WebLayer(connection)
    pfilter = query.to_internal(project=connection.project)

    participants = await wlayer.query_participants(pfilter, limit=None)

    # then have to get all nested objects

    keys = ['p.id', 'p.external_id', 'p.meta']
    rows = [(p.id, p.external_id, p.meta) for p in participants]

    output = io.StringIO()
    writer = csv.writer(output, delimiter=export_type.get_delimiter())
    writer.writerow(keys)
    writer.writerows(rows)

    basefn = f'{connection.project}-project-summary-{connection.author}-{date.today().isoformat()}'

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type=export_type.get_mime_type(),
        headers={
            'Content-Disposition': f'filename={basefn}{export_type.get_extension()}'
        },
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
        keyword, project_ids=[p for p in pmap.keys() if p]
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
