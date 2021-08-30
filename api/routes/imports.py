import csv
import codecs

from fastapi import APIRouter, UploadFile, File

from models.models.sample import sample_id_format
from db.python.layers.imports import ImportLayer
from db.python.layers.participant import (
    ParticipantLayer,
    ExtraParticipantImporterHandler,
)
from api.utils.db import get_project_db_connection, Connection

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


@router.post(
    '/{project}/individual-metadata-manifest',
    operation_id='importIndividualMetadataManifest',
)
async def import_individual_metadata_manifest(
    file: UploadFile = File(...),
    extra_participants_method: ExtraParticipantImporterHandler = ExtraParticipantImporterHandler.FAIL,
    connection: Connection = get_project_db_connection,
):
    """
    Import individual metadata manifest

    :param extra_participants_method: If extra participants are in the uploaded file,
        add a PARTICIPANT entry for them
    """
    player = ParticipantLayer(connection)
    csvreader = csv.reader(codecs.iterdecode(file.file, 'utf-8-sig'))
    headers = next(csvreader)

    await player.generic_individual_metadata_importer(
        headers, list(csvreader), extra_participants_method=extra_participants_method
    )
    return {'success': True}
