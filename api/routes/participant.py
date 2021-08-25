from typing import List, Optional

import io
import csv
from datetime import date

from fastapi import APIRouter
from fastapi.params import Query
from starlette.responses import StreamingResponse

from api.utils.db import get_project_db_connection, Connection
from db.python.layers.participant import ParticipantLayer

router = APIRouter(prefix='/participant', tags=['participant'])


@router.post(
    '/{project}/fill-in-missing-participants', operation_id='fillInMissingParticipants'
)
async def fill_in_missing_participants(
    connection: Connection = get_project_db_connection,
):
    """Get sample by external ID"""
    participant_layer = ParticipantLayer(connection)

    return {'success': await participant_layer.fill_in_missing_participants()}


@router.get(
    '/{project}/individual-metadata-template/seqr',
    operation_id='getIndividualMetadataTemplateForSeqr',
    response_class=StreamingResponse,
)
async def get_individual_metadata_template_for_seqr(
    project: str,
    external_participant_ids: Optional[List[str]] = Query(default=None),
    delimeter: str = ',',
    # pylint: disable=invalid-name
    replace_with_participant_external_ids: bool = True,
    connection: Connection = get_project_db_connection,
):
    """Get individual metadata template for SEQR as a CSV"""
    participant_layer = ParticipantLayer(connection)
    rows = await participant_layer.get_seqr_individual_template(
        project=connection.project,
        external_participant_ids=external_participant_ids,
        replace_with_participant_external_ids=replace_with_participant_external_ids,
    )

    if delimeter == '\\t':
        delimeter = '\t'

    output = io.StringIO()
    writer = csv.writer(output, delimiter=delimeter)
    writer.writerows(rows)

    ext = '.csv'
    if delimeter == '\t':
        ext = '.tsv'

    basefn = f'{project}-{date.today().isoformat()}'

    return StreamingResponse(
        iter(output.getvalue()),
        media_type='text/csv',
        headers={'Content-Disposition': f'filename={basefn}.{ext}'},
    )
