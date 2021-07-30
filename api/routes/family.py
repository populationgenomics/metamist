import csv
import codecs

from fastapi import APIRouter, UploadFile, File

from db.python.layers.family import FamilyLayer

from api.utils.db import get_db_connection, Connection
from models.models.sample import sample_id_format

router = APIRouter(prefix='/family', tags=['family'])


@router.post('/pedigree', operation_id='importPedigree')
async def import_pedigree(
    file: UploadFile = File(...),
    has_header=False,
    connection: Connection = get_db_connection,
):
    """Get sample by external ID"""
    family_layer = FamilyLayer(connection)
    reader = csv.reader(codecs.iterdecode(file.file, 'utf-8-sig'))
    headers = None
    if has_header:
        headers = next(reader)

    return {'success': await family_layer.import_pedigree(headers, list(reader))}
