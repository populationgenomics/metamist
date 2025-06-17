# pylint: disable=kwarg-superseded-by-positional-arg
"""
Web routes
"""

import asyncio
import csv
import io
from typing import Any, Generator

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from api.utils.db import (
    Connection,
    get_project_db_connection,
    get_projectless_db_connection,
)
from api.utils.export import ExportType
from db.python.filters.web import ProjectParticipantGridFilter
from db.python.layers.search import SearchLayer
from db.python.layers.seqr import SeqrLayer
from db.python.layers.web import WebLayer
from models.base import SMBase
from models.enums.web import MetaSearchEntityPrefix, SeqrDatasetType
from models.models.participant import NestedParticipant
from models.models.project import FullWriteAccessRoles, ReadAccessRoles
from models.models.search import SearchResponse
from models.models.web import (
    ProjectParticipantGridField,
    ProjectParticipantGridResponse,
    ProjectSummary,
    ProjectWebReport,
)


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
    connection: Connection = get_project_db_connection(ReadAccessRoles),
) -> ProjectSummary:
    """Creates a new sample, and returns the internal sample ID"""
    st = WebLayer(connection)

    summary = await st.get_project_summary()

    return summary.to_external()


@router.post(
    '/{project}/participants/schema',
    # response_model=,
    operation_id='getProjectParticipantsFilterSchema',
)
async def get_project_project_participants_filter_schema(
    _=get_project_db_connection(ReadAccessRoles),
):
    """Get project summary (from query) with some limit"""
    return ProjectParticipantGridFilter.model_json_schema()


@router.post(
    '/{project}/participants',
    response_model=ProjectParticipantGridResponse,
    operation_id='getProjectParticipantsGridWithLimit',
)
async def get_project_participants_grid_with_limit(
    limit: int,
    query: ProjectParticipantGridFilter,
    skip: int = 0,
    connection: Connection = get_project_db_connection(ReadAccessRoles),
):
    """Get project summary (from query) with some limit"""

    if not connection.project_id:
        raise ValueError('No project was detected through the authentication')

    wlayer = WebLayer(connection)
    pfilter = query.to_internal(project=connection.project_id)

    participants, pcount = await asyncio.gather(
        wlayer.query_participants(pfilter, limit=limit, skip=skip),
        wlayer.count_participants(pfilter),
    )

    return ProjectParticipantGridResponse.from_params(
        participants=participants,
        total_results=pcount,
        filter_fields=query,
    )


class ExportProjectParticipantFields(SMBase):
    """fields for exporting project participants"""

    fields: dict[MetaSearchEntityPrefix, list[ProjectParticipantGridField]]


@router.post(
    '/{project}/participants/export/{export_type}',
    operation_id='exportProjectParticipants',
)
async def export_project_participants(
    export_type: ExportType,
    query: ProjectParticipantGridFilter,
    fields: ExportProjectParticipantFields | None = None,
    connection: Connection = get_project_db_connection(ReadAccessRoles),
):
    """Get project summary (from query) with some limit"""

    if not connection.project_id:
        raise ValueError('No project was detected through the authentication')

    wlayer = WebLayer(connection)
    pfilter = query.to_internal(project=connection.project_id)

    participants_internal = await wlayer.query_participants(pfilter, limit=None)
    participants = [p.to_external() for p in participants_internal]

    if export_type == ExportType.JSON:
        return participants

    # then have to get all nested objects
    output = io.StringIO()
    writer = csv.writer(output, delimiter=export_type.get_delimiter())

    for row in prepare_participants_for_export(participants, fields=fields):
        writer.writerow(row)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type=export_type.get_mime_type(),
        # content-disposition doesn't work here :(
        headers={},
    )


@router.get(
    '/{project}/webReports/{sequencing_type}',
    operation_id='getProjectWebReports',
    response_model=list[ProjectWebReport],
)
async def get_project_web_reports(
    sequencing_types: list[str] | None = None,
    stages: list[str] | None = None,
    connection: Connection = get_project_db_connection(ReadAccessRoles),
) -> list[ProjectWebReport]:
    """Get web reports for the project and sequencing type"""
    if not connection.project_id:
        raise ValueError('No project was detected through the authentication')

    wlayer = WebLayer(connection)
    reports = await wlayer.get_project_web_reports(
        sequencing_types=sequencing_types, stages=stages
    )

    return [r.to_external() for r in reports]


def get_field_from_obj(obj, field: ProjectParticipantGridField) -> str | None:
    """Get field from object"""
    if field.key.startswith('meta.'):
        if not hasattr(obj, 'meta'):
            raise ValueError(f'Object {type(obj)} does not have meta field: {obj}')

        return obj.meta.get(field.key.removeprefix('meta.'), None)
    if not hasattr(obj, field.key):
        raise ValueError(f'Object {type(obj)} does not have field: {field}')

    return getattr(obj, field.key, None)


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
    _fields = fields.fields if fields else None
    if not _fields:
        # empty field, because we don't really care about the fields
        _fields = ProjectParticipantGridResponse.get_entity_keys(
            participants, ProjectParticipantGridFilter()
        )

    def get_visible_fields(key: MetaSearchEntityPrefix):
        fs = _fields.get(key, [])
        return [f for f in fs if f.is_visible]

    family_keys = get_visible_fields(MetaSearchEntityPrefix.FAMILY)
    participant_keys = get_visible_fields(MetaSearchEntityPrefix.PARTICIPANT)
    sample_keys = get_visible_fields(MetaSearchEntityPrefix.SAMPLE)
    sequencing_group_keys = get_visible_fields(MetaSearchEntityPrefix.SEQUENCING_GROUP)
    assay_keys = get_visible_fields(MetaSearchEntityPrefix.ASSAY)

    header = (
        *('family.' + fk.key for fk in family_keys),
        *('participant.' + pk.key for pk in participant_keys),
        *('sample.' + sk.key for sk in sample_keys),
        *('sequencing_group.' + sgk.key for sgk in sequencing_group_keys),
        *('assay.' + ak.key for ak in assay_keys),
    )
    yield header
    for participant in participants:
        prow = []
        for field in family_keys:
            prow.append(
                ', '.join(
                    prepare_field_for_export(get_field_from_obj(f, field))
                    for f in participant.families
                )
            )
        for field in participant_keys:
            prow.append(
                prepare_field_for_export(get_field_from_obj(participant, field))
            )

        for sample in participant.samples:
            srow = []
            for field in sample_keys:
                srow.append(prepare_field_for_export(get_field_from_obj(sample, field)))

            for sg in sample.sequencing_groups or []:
                sgrow = []
                for field in sequencing_group_keys:
                    sgrow.append(
                        prepare_field_for_export(get_field_from_obj(sg, field))
                    )

                for assay in sg.assays or []:
                    arow = []
                    for field in assay_keys:
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
async def search_by_keyword(
    keyword: str, connection: Connection = get_projectless_db_connection
):
    """
    This searches the keyword, in families, participants + samples in the projects
    that you are a part of (automatically).
    """
    # raise ValueError("Test")
    projects = connection.all_projects()
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
    connection: Connection = get_project_db_connection(FullWriteAccessRoles),
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


@router.get(
    '/{project}/{sequencing_type}/seqr-family-guid-map',
    operation_id='getSeqrFamilyGuidMap',
)
async def get_seqr_family_guid_map(
    sequencing_type: str,
    connection: Connection = get_project_db_connection(ReadAccessRoles),
):
    """
    Get the mapping of seqr family GUIDs to internal family IDs
    """
    seqr = SeqrLayer(connection)
    if not connection.project_id:
        raise ValueError('Project not set')

    return await seqr.get_family_guid_map(sequencing_type=sequencing_type)
