import codecs
import csv
from typing import List, Optional
import io
from datetime import date

from fastapi import APIRouter, UploadFile, File, Query
from starlette.responses import StreamingResponse

from api.utils.db import get_db_connection, Connection
from db.python.layers.family import FamilyLayer

router = APIRouter(prefix='/family', tags=['family'])


@router.post('/pedigree', operation_id='importPedigree')
async def import_pedigree(
    file: UploadFile = File(...),
    has_header: bool = False,
    connection: Connection = get_db_connection,
):
    """Get sample by external ID"""
    family_layer = FamilyLayer(connection)
    reader = csv.reader(codecs.iterdecode(file.file, 'utf-8-sig'), delimiter='\t')
    headers = None
    if has_header:
        headers = next(reader)

    rows = [r for r in reader if not r[0].startswith('#')]
    if len(rows[0]) == 1:
        raise ValueError(
            'Only one column was detected in the pedigree, ensure the file is TAB separated (\\t)'
        )

    return {'success': await family_layer.import_pedigree(headers, rows)}


@router.get('/pedigree', operation_id='getPedigree', response_class=StreamingResponse)
async def get_pedigree(
    internal_family_ids: List[int] = Query(None),
    # pylint: disable=invalid-name
    replace_with_participant_external_ids: bool = False,
    # pylint: disable=invalid-name
    replace_with_family_external_ids: bool = False,
    empty_participant_value: Optional[str] = '',
    connection: Connection = get_db_connection,
):
    """
    Generate tab-separated Pedigree file for ALL families
    unless internal_family_ids is specified.

    Allow replacement of internal participant and family IDs
    with their external counterparts.
    """
    family_layer = FamilyLayer(connection)
    pedigree_rows = await family_layer.get_pedigree(
        family_ids=internal_family_ids,
        replace_with_participant_external_ids=replace_with_participant_external_ids,
        replace_with_family_external_ids=replace_with_family_external_ids,
        empty_participant_value=empty_participant_value,
    )

    output = io.StringIO()
    writer = csv.writer(output, delimiter='\t')
    writer.writerows(pedigree_rows)

    basefn = f'{connection.project}-{date.today().isoformat()}'

    if internal_family_ids:
        basefn += '-'.join(str(fm) for fm in internal_family_ids)

    return StreamingResponse(
        iter(output.getvalue()),
        media_type='text/csv',
        headers={'Content-Disposition': f'filename={basefn}.ped'},
    )
