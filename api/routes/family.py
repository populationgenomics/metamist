import codecs
import csv

from fastapi import APIRouter, UploadFile, File

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
