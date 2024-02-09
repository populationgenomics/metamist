import csv
import io
from datetime import date

from fastapi import APIRouter
from fastapi.params import Query
from starlette.responses import JSONResponse, StreamingResponse

from api.utils import get_projectless_db_connection
from api.utils.db import (
    Connection,
    get_project_readonly_connection,
    get_project_write_connection,
)
from api.utils.export import ExportType
from db.python.layers.participant import ParticipantLayer
from models.models.participant import ParticipantUpsert
from models.models.sample import sample_id_format
from models.models.sequencing_group import sequencing_group_id_format

router = APIRouter(prefix='/participant', tags=['participant'])


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
    '/{project}/individual-metadata-seqr',
    operation_id='getIndividualMetadataForSeqr',
    tags=['seqr'],
)
async def get_individual_metadata_template_for_seqr(
    project: str,
    export_type: ExportType = ExportType.JSON,
    external_participant_ids: list[str] | None = Query(default=None),  # type: ignore[assignment]
    # pylint: disable=invalid-name
    replace_with_participant_external_ids: bool = True,
    connection: Connection = get_project_readonly_connection,
):
    """Get individual metadata template for SEQR as a CSV"""
    participant_layer = ParticipantLayer(connection)
    assert connection.project
    resp = await participant_layer.get_seqr_individual_template(
        project=connection.project,
        external_participant_ids=external_participant_ids,
        replace_with_participant_external_ids=replace_with_participant_external_ids,
    )

    if export_type == ExportType.JSON:
        return JSONResponse(resp)

    json_rows = resp['rows']
    headers = resp['headers']
    col_header_map = resp['header_map']

    output = io.StringIO()
    writer = csv.writer(output, delimiter=export_type.get_delimiter())
    rows = [
        [col_header_map[h] for h in headers],
        *[[row.get(kh, '') for kh in headers] for row in json_rows],
    ]
    writer.writerows(rows)

    basefn = f'{project}-{date.today().isoformat()}'
    ext = export_type.get_extension()

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type=export_type.get_mime_type(),
        headers={'Content-Disposition': f'filename={basefn}{ext}'},
    )


@router.post(
    '/{project}/id-map/external',
    operation_id='getParticipantIdMapByExternalIds',
)
async def get_id_map_by_external_ids(
    external_participant_ids: list[str],
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
    internal_to_external_id: dict[int, str],
    connection: Connection = get_projectless_db_connection,
):
    """Update external_ids of participants by providing an update map"""
    player = ParticipantLayer(connection)
    return await player.update_many_participant_external_ids(internal_to_external_id)


@router.get(
    '/{project}/external-pid-to-sg-id',
    operation_id='getExternalParticipantIdToSequencingGroupId',
    tags=['seqr'],
)
async def get_external_participant_id_to_sequencing_group_id(
    project: str,
    export_type: ExportType = ExportType.JSON,
    flip_columns: bool = False,
    connection: Connection = get_project_readonly_connection,
):
    """
    Get csv / tsv export of external_participant_id to internal_sample_id

    Get a map of {external_participant_id} -> {internal_sample_id}
    useful to matching joint-called samples in the matrix table to the participant

    Return a list not dictionary, because dict could lose
    participants with multiple samples.

    :param flip_columns: Set to True when exporting for seqr
    """
    player = ParticipantLayer(connection)
    # this wants project ID (connection.project)
    assert connection.project
    m = await player.get_external_participant_id_to_internal_sequencing_group_id_map(
        project=connection.project
    )

    rows = [[pid, sequencing_group_id_format(sgid)] for pid, sgid in m]
    if flip_columns:
        rows = [r[::-1] for r in rows]

    if export_type == ExportType.JSON:
        return rows

    output = io.StringIO()
    writer = csv.writer(output, delimiter=export_type.get_delimiter())
    writer.writerows(rows)

    ext = export_type.get_extension()
    filename = f'{project}-participant-to-sequencing-group-map-{date.today().isoformat()}{ext}'
    return StreamingResponse(
        # stream the whole file at once, because it's all in memory anyway
        iter([output.getvalue()]),
        media_type=export_type.get_mime_type(),
        headers={'Content-Disposition': f'filename={filename}'},
    )


@router.post('/{participant_id}/update-participant', operation_id='updateParticipant')
async def update_participant(
    participant_id: int,
    participant: ParticipantUpsert,
    connection: Connection = get_projectless_db_connection,
):
    """Update Participant Data"""
    participant_layer = ParticipantLayer(connection)

    participant.id = participant_id

    return {
        'success': await participant_layer.upsert_participant(participant.to_internal())
    }


@router.put(
    '/{project}/upsert-many',
    operation_id='upsertParticipants',
)
async def upsert_participants(
    participants: list[ParticipantUpsert],
    connection: Connection = get_project_write_connection,
):
    """
    Upserts a list of participants with samples and sequences
    Returns the list of internal sample IDs
    """
    pt = ParticipantLayer(connection)
    results = await pt.upsert_participants([p.to_internal() for p in participants])
    return [p.to_external() for p in results]


@router.post(
    '/{project}',
    # response_model=list[ParticipantModel],
    operation_id='getParticipants',
)
async def get_participants(
    external_participant_ids: list[str] = None,
    internal_participant_ids: list[int] = None,
    connection: Connection = get_project_readonly_connection,
):
    """Get participants, default ALL participants in project"""
    player = ParticipantLayer(connection)
    participants = await player.get_participants(
        project=connection.project,
        external_participant_ids=external_participant_ids,
        internal_participant_ids=internal_participant_ids,
    )
    return [p.to_external() for p in participants]


@router.post(
    '/{participant_id}/update-participant-family',
    operation_id='updateParticipantFamily',
)
async def update_participant_family(
    participant_id: int,
    old_family_id: int,
    new_family_id: int,
    connection: Connection = get_projectless_db_connection,
):
    """
    Change a participants family from old_family_id
    to new_family_id, maintaining all other fields.
    The new_family_id must already exist.
    """
    player = ParticipantLayer(connection)

    return await player.update_participant_family(
        participant_id=participant_id,
        old_family_id=old_family_id,
        new_family_id=new_family_id,
    )
