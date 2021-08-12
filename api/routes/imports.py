import csv
import codecs

from fastapi import APIRouter, UploadFile, File

from db.python.layers.imports import ImportLayer

from api.utils.db import get_project_db_connection, Connection
from models.models.sample import sample_id_format

router = APIRouter(prefix='/import', tags=['import'])


@router.post('/{project}/airtable-manifest', operation_id='importAirtableManifest')
async def import_airtable_manifest(
    file: UploadFile = File(...), connection: Connection = get_project_db_connection
):
    """Get sample by external ID"""
    import_layer = ImportLayer(connection)
    csvreader = csv.reader(codecs.iterdecode(file.file, 'utf-8-sig'))
    headers = next(csvreader)

    sample_ids = await import_layer.import_airtable_manifest_csv(headers, csvreader)
    return {'success': True, 'sample_ids': sample_id_format(sample_ids)}
