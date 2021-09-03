from typing import List, Optional

import io
import csv
from datetime import date

from fastapi import APIRouter
from fastapi.params import Query
from starlette.responses import StreamingResponse

from api.utils.db import (
    get_project_write_connection,
    get_project_readonly_connection,
    Connection,
)
from api.utils.export import ExportType
from db.python.layers.participant import ParticipantLayer

router = APIRouter(prefix='/participant', tags=['participant'])


@router.post(
    '/{project}/fill-in-missing-participants', operation_id='fillInMissingParticipants'
)
async def fill_in_missing_participants(
    connection: Connection = get_project_write_connection,
):
    """Get sample by external ID"""
    participant_layer = ParticipantLayer(connection)

    return {'success': await participant_layer.fill_in_missing_participants()}


@router.get(
    '/{project}/individual-metadata-seqr/{export_type}',
    operation_id='getIndividualMetadataForSeqr',
    response_class=StreamingResponse,
)
async def get_individual_metadata_template_for_seqr(
    project: str,
    export_type: ExportType,
    external_participant_ids: Optional[List[str]] = Query(default=None),
    # pylint: disable=invalid-name
    replace_with_participant_external_ids: bool = True,
    connection: Connection = get_project_readonly_connection,
):
    """Get individual metadata template for SEQR as a CSV"""
    participant_layer = ParticipantLayer(connection)
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
