from typing import List, Optional, Dict

import io
import csv
from datetime import date

from fastapi import APIRouter
from fastapi.params import Query
from starlette.responses import StreamingResponse
from pydantic import BaseModel

from api.utils import get_projectless_db_connection
from api.utils.db import (
    get_project_write_connection,
    get_project_readonly_connection,
    Connection,
)
from api.utils.extensions import FileExtension
from db.python.layers.participant import ParticipantLayer
from models.models.sample import sample_id_format

router = APIRouter(prefix='/participant', tags=['participant'])


class ParticipantUpdateModel(BaseModel):
    """Update participant model"""

    reported_sex: Optional[int] = None
    reported_gender: Optional[str] = None
    karyotype: Optional[str] = None
    meta: Optional[Dict] = None


@router.post(
    '/{project}/fill-in-missing-participants', operation_id='fillInMissingParticipants'
)
async def fill_in_missing_participants(
    connection: Connection = get_project_write_connection,
):
    """
    Create a corresponding participant (if required)
    for each sample within a project, useful for then importing a pedigree
    """
    participant_layer = ParticipantLayer(connection)

    return {'success': await participant_layer.fill_in_missing_participants()}


@router.get(
    '/{project}/individual-metadata-seqr/{export_type}',
    operation_id='getIndividualMetadataForSeqr',
    response_class=StreamingResponse,
)
async def get_individual_metadata_template_for_seqr(
    project: str,
    export_type: FileExtension,
    external_participant_ids: Optional[List[str]] = Query(default=None),  # type: ignore[assignment]
    # pylint: disable=invalid-name
    replace_with_participant_external_ids: bool = True,
    connection: Connection = get_project_readonly_connection,
):
    """Get individual metadata template for SEQR as a CSV"""
    participant_layer = ParticipantLayer(connection)
    assert connection.project
    rows = await participant_layer.get_seqr_individual_template(
        project=connection.project,
        external_participant_ids=external_participant_ids,
        replace_with_participant_external_ids=replace_with_participant_external_ids,
    )

    output = io.StringIO()
    writer = csv.writer(output, delimiter=export_type.get_delimiter())
    writer.writerows(rows)

    basefn = f'{project}-{date.today().isoformat()}'
    ext = export_type.get_extension()
    return StreamingResponse(
        iter(output.getvalue()),
        media_type=export_type.get_mime_type(),
        headers={'Content-Disposition': f'filename={basefn}{ext}'},
    )


@router.post(
    '/{project}/id-map/external',
    operation_id='getParticipantIdMapByExternalIds',
)
async def get_id_map_by_external_ids(
    external_participant_ids: List[str],
    allow_missing: bool = False,
    connection: Connection = get_project_readonly_connection,
):
    """Get ID map of participants, by external_id"""
    player = ParticipantLayer(connection)
    return await player.get_id_map_by_external_ids(
        external_participant_ids,
        allow_missing=allow_missing,
        project=connection.project,
    )


@router.post('/update-many', operation_id='updateManyParticipants')
async def update_many_participant_external_ids(
    internal_to_external_id: Dict[int, str],
    connection: Connection = get_projectless_db_connection,
):
    """Update external_ids of participants by providing an update map"""
    player = ParticipantLayer(connection)
    return await player.update_many_participant_external_ids(internal_to_external_id)


@router.get(
    '/{project}/external-pid-to-internal-sample-id',
    operation_id='getExternalParticipantIdToInternalSampleId',
)
async def get_external_participant_id_to_internal_sample_id(
    connection: Connection = get_project_readonly_connection,
):
    """
    Get a map of {external_participant_id} -> {internal_sample_id}
    useful to matching joint-called samples in the matrix table to the participant

    Return a list not dictionary, because dict could lose
    participants with multiple samples.
    """
    player = ParticipantLayer(connection)
    assert connection.project
    m = await player.get_external_participant_id_to_internal_sample_id_map(
        project=connection.project
    )
    return [[pid, sample_id_format(sid)] for pid, sid in m]


@router.get(
    '/{project}/external-pid-to-internal-sample-id/{export_type}',
    operation_id='getExternalParticipantIdToInternalSampleId',
    response_class=StreamingResponse,
)
async def get_external_participant_id_to_internal_sample_id_export(
    project: str,
    export_type: FileExtension,
    connection: Connection = get_project_readonly_connection,
):
    """Get csv / tsv export of external_participant_id to internal_sample_id"""
    player = ParticipantLayer(connection)
    # this wants project ID (connection.project)
    assert connection.project
    m = await player.get_external_participant_id_to_internal_sample_id_map(
        project=connection.project
    )
    rows = [[pid, sample_id_format(sid)] for pid, sid in m]

    output = io.StringIO()
    writer = csv.writer(output, delimiter=export_type.get_delimiter())
    writer.writerows(rows)

    ext = export_type.get_extension()
    filename = f'{project}-participant-to-sample-map-{date.today().isoformat()}{ext}'
    return StreamingResponse(
        iter(output.getvalue()),
        media_type=export_type.get_mime_type(),
        headers={'Content-Disposition': f'filename={filename}'},
    )


@router.post('/{participant_id}/update-participant', operation_id='updateParticipant')
async def update_participant(
    participant_id: int,
    participant: ParticipantUpdateModel,
    connection: Connection = get_projectless_db_connection,
):
    """Update Participant Data"""
    participant_layer = ParticipantLayer(connection)

    return {
        'success': await participant_layer.update_single_participant(
            participant_id=participant_id,
            reported_sex=participant.reported_sex,
            reported_gender=participant.reported_gender,
            karyotype=participant.karyotype,
            meta=participant.meta,
        )
    }
