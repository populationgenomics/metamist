import csv
import codecs
from typing import Optional

from fastapi import APIRouter, UploadFile, File

from models.models.sample import sample_id_format_list
from db.python.layers.imports import ImportLayer
from db.python.layers.participant import (
    ParticipantLayer,
    ExtraParticipantImporterHandler,
)
from api.utils.db import get_project_write_connection, Connection

router = APIRouter(prefix='/import', tags=['import'])


@router.post('/{project}/airtable-manifest', operation_id='importAirtableManifest')
async def import_airtable_manifest(
    file: UploadFile = File(...), connection: Connection = get_project_write_connection
):
    """Get sample by external ID"""
    import_layer = ImportLayer(connection)
    csvreader = csv.reader(codecs.iterdecode(file.file, 'utf-8-sig'))
    headers = next(csvreader)

    sample_ids = await import_layer.import_airtable_manifest_csv(headers, csvreader)
    return {'success': True, 'sample_ids': sample_id_format_list(sample_ids)}


@router.post(
    '/{project}/individual-metadata-manifest',
    operation_id='importIndividualMetadataManifest',
)
async def import_individual_metadata_manifest(
    file: UploadFile = File(...),
    delimiter: Optional[str] = None,
    extra_participants_method: ExtraParticipantImporterHandler = ExtraParticipantImporterHandler.FAIL,
    connection: Connection = get_project_write_connection,
):
    """
    Import individual metadata manifest

    :param extra_participants_method: If extra participants are in the uploaded file,
        add a PARTICIPANT entry for them
    """

    if not delimiter:
        if file.filename.endswith(".csv"):
            delimiter = ','
        elif file.filename.endswith('.tsv'):
            delimiter = '\t'
        else:
            raise ValueError('Unable to determine the delimiter of the uploaded file, please specify one')

    delimiter = delimiter.replace('\\t', '\t')

    player = ParticipantLayer(connection)
    csvreader = csv.reader(codecs.iterdecode(file.file, 'utf-8-sig'), delimiter=delimiter)
    headers = next(csvreader)

    await player.generic_individual_metadata_importer(
        headers, list(csvreader), extra_participants_method=extra_participants_method
    )
    return {'success': True}
