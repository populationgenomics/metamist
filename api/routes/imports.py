import codecs
import csv
from typing import Optional

from fastapi import APIRouter, File, UploadFile

from api.utils.db import Connection, get_project_write_connection
from api.utils.extensions import guess_delimiter_by_upload_file_obj
from db.python.layers.participant import (
    ExtraParticipantImporterHandler,
    ParticipantLayer,
)

router = APIRouter(prefix='/import', tags=['import'])


@router.post(
    '/{project}/individual-metadata-manifest',
    operation_id='importIndividualMetadataManifest',
    tags=['seqr'],
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

    delimiter = guess_delimiter_by_upload_file_obj(file, default_delimiter=delimiter)

    player = ParticipantLayer(connection)
    csvreader = csv.reader(
        codecs.iterdecode(file.file, 'utf-8-sig'), delimiter=delimiter
    )
    headers = next(csvreader)

    await player.generic_individual_metadata_importer(
        headers, list(csvreader), extra_participants_method=extra_participants_method
    )
    return {'success': True}
