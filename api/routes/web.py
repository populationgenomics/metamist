# pylint: disable=kwarg-superseded-by-positional-arg
"""
Web routes
"""

import asyncio
import csv
import io
from datetime import date
from typing import Any, Generator

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

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
from models.base import SMBase
from models.enums.web import SeqrDatasetType
from models.models.participant import NestedParticipant
from models.models.project import ProjectId
from models.models.search import SearchResponse
from models.models.web import ProjectParticipantGridResponse, ProjectSummary
from models.utils.sample_id_format import sample_id_transform_to_raw
from models.utils.sequencing_group_id_format import sequencing_group_id_transform_to_raw


class SearchResponseModel(SMBase):
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


class ProjectParticipantGridFilter(SMBase):
    """filters for participant grid"""

    class ParticipantGridParticipantFilter(SMBase):
        """participant filter option for participant grid"""

        id: GenericFilter[int] | None = None
        meta: GenericMetaFilter | None = None
        external_id: GenericFilter[str] | None = None
        reported_sex: GenericFilter[int] | None = None
        reported_gender: GenericFilter[str] | None = None
        karyotype: GenericFilter[str] | None = None

    class ParticipantGridFamilyFilter(SMBase):
        """family filter option for participant grid"""

        id: GenericFilter[int] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None

    class ParticipantGridSampleFilter(SMBase):
        """sample filter option for participant grid"""

        id: GenericFilter[str] | None = None
        type: GenericFilter[str] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None

    class ParticipantGridSequencingGroupFilter(SMBase):
        """sequencing group filter option for participant grid"""

        id: GenericFilter[str] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None
        type: GenericFilter[str] | None = None
        technology: GenericFilter[str] | None = None
        platform: GenericFilter[str] | None = None

    class ParticipantGridAssayFilter(SMBase):
        """assay filter option for participant grid"""

        id: GenericFilter[int] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None
        type: GenericFilter[str] | None = None

    family: ParticipantGridFamilyFilter | None = None
    participant: ParticipantGridParticipantFilter | None = None
    sample: ParticipantGridSampleFilter | None = None
    sequencing_group: ParticipantGridSequencingGroupFilter | None = None
    assay: ParticipantGridAssayFilter | None = None

    def to_internal(self, project: ProjectId) -> ParticipantFilter:
        """Convert to participant internal filter object"""
        return ParticipantFilter(
            id=self.participant.id if self.participant else None,
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
                    id=(
                        self.sample.id.transform(sample_id_transform_to_raw)
                        if self.sample.id
                        else None
                    ),
                    type=self.sample.type,
                    external_id=self.sample.external_id,
                    meta=self.sample.meta,
                )
                if self.sample
                else None
            ),
            sequencing_group=(
                ParticipantFilter.ParticipantSequencingGroupFilter(
                    id=(
                        self.sequencing_group.id.transform(
                            sequencing_group_id_transform_to_raw
                        )
                        if self.sequencing_group.id
                        else None
                    ),
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
    limit: int,
    query: ProjectParticipantGridFilter,
    skip: int = 0,
    connection=get_project_readonly_connection,
):
    """Get project summary (from query) with some limit"""

    if not connection.project:
        raise ValueError('No project was detected through the authentication')

    wlayer = WebLayer(connection)
    pfilter = query.to_internal(project=connection.project)

    participants, pcount = await asyncio.gather(
        wlayer.query_participants(pfilter, limit=limit, skip=skip),
        wlayer.count_participants(pfilter),
    )

    # then have to get all nested objects

    return ProjectParticipantGridResponse.from_params(
        participants=participants,
        total_results=pcount,
    )


class ExportProjectParticipantFields(SMBase):
    """fields for exporting project participants"""

    family_keys: list[str]
    participant_keys: list[str]
    sample_keys: list[str]
    sequencing_group_keys: list[str]
    assay_keys: list[str]


@router.post(
    '/{project}/participants/export/{export_type}',
    operation_id='exportProjectParticipants',
)
async def export_project_participants(
    export_type: ExportType,
    query: ProjectParticipantGridFilter,
    fields: ExportProjectParticipantFields | None = None,
    connection=get_project_readonly_connection,
):
    """Get project summary (from query) with some limit"""

    if not connection.project:
        raise ValueError('No project was detected through the authentication')

    wlayer = WebLayer(connection)
    pfilter = query.to_internal(project=connection.project)

    participants_internal = await wlayer.query_participants(pfilter, limit=None)
    participants = [p.to_external() for p in participants_internal]

    if export_type == ExportType.JSON:
        return participants

    # then have to get all nested objects
    output = io.StringIO()
    writer = csv.writer(output, delimiter=export_type.get_delimiter())

    for row in prepare_participants_for_export(participants, fields=fields):
        writer.writerow(row)

    basefn = f'{connection.project}-project-summary-{connection.author}-{date.today().isoformat()}'

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type=export_type.get_mime_type(),
        headers={
            'Content-Disposition': f'filename={basefn}{export_type.get_extension()}'
        },
    )


def get_field_from_obj(obj, field: str) -> str | None:
    """Get field from object"""
    if field.startswith('meta.'):
        if not hasattr(obj, 'meta'):
            raise ValueError(f'Object {type(obj)} does not have meta field: {obj}')

        return obj.meta.get(field.removeprefix('meta.'), None)
    if not hasattr(obj, field):
        raise ValueError(f'Object {type(obj)} does not have field: {field}')

    return getattr(obj, field, None)


def prepare_field_for_export(field_value: Any) -> str:
    """Prepare field for export"""
    if isinstance(field_value, (list, tuple)):
        return ', '.join(prepare_field_for_export(f) for f in field_value)
    if isinstance(field_value, dict):
        # special case if the key is empty, then just return the value
        return ', '.join(
            f'{k}: {prepare_field_for_export(v)}' if k else v
            for k, v in field_value.items()
        )

    return str(field_value)


def prepare_participants_for_export(
    participants: list[NestedParticipant], fields: ExportProjectParticipantFields | None
) -> Generator[tuple[str, ...], None, None]:
    """Prepare participants for export"""
    if not fields:
        new_fields = ProjectParticipantGridResponse.get_entity_keys(participants)
        fields = ExportProjectParticipantFields(
            family_keys=[t[0] for t in new_fields.family_keys],
            participant_keys=[t[0] for t in new_fields.participant_keys],
            sample_keys=[t[0] for t in new_fields.sample_keys],
            sequencing_group_keys=[t[0] for t in new_fields.sequencing_group_keys],
            assay_keys=[t[0] for t in new_fields.assay_keys],
        )

    header = (
        *('family.' + fk for fk in fields.family_keys),
        *('participant.' + pk for pk in fields.participant_keys),
        *('sample.' + sk for sk in fields.sample_keys),
        *('sequencing_group.' + sgk for sgk in fields.sequencing_group_keys),
        *('assay.' + ak for ak in fields.assay_keys),
    )
    yield header
    for participant in participants:
        prow = []
        for field in fields.family_keys:
            prow.append(
                ', '.join(
                    prepare_field_for_export(get_field_from_obj(f, field))
                    for f in participant.families
                )
            )
        for field in fields.participant_keys:
            prow.append(
                prepare_field_for_export(get_field_from_obj(participant, field))
            )

        for sample in participant.samples:
            srow = []
            for field in fields.sample_keys:
                srow.append(prepare_field_for_export(get_field_from_obj(sample, field)))

            for sg in sample.sequencing_groups or []:
                sgrow = []
                for field in fields.sequencing_group_keys:
                    sgrow.append(
                        prepare_field_for_export(get_field_from_obj(sg, field))
                    )

                for assay in sg.assays or []:
                    arow = []
                    for field in fields.assay_keys:
                        arow.append(
                            prepare_field_for_export(get_field_from_obj(assay, field))
                        )

                    yield (
                        *prow,
                        *srow,
                        *sgrow,
                        *arow,
                    )


@router.get(
    '/search', response_model=SearchResponseModel, operation_id='searchByKeyword'
)
async def search_by_keyword(keyword: str, connection=get_projectless_db_connection):
    """
    This searches the keyword, in families, participants + samples in the projects
    that you are a part of (automatically).
    """
    # raise ValueError('Test')
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
