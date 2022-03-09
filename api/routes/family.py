# pylint: disable=invalid-name
import io
import re
import csv
import codecs
from enum import Enum
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, UploadFile, File, Query
from pydantic import BaseModel
from starlette.responses import StreamingResponse

from api.utils import get_projectless_db_connection
from api.utils.db import (
    get_project_readonly_connection,
    get_project_write_connection,
    Connection,
)
from api.utils.extensions import guess_delimiter_by_filename
from db.python.layers.family import FamilyLayer
from models.models.family import Family
from models.models.sample import sample_id_transform_to_raw_list

router = APIRouter(prefix='/family', tags=['family'])


class ContentType(Enum):
    """Enum for available content type options for get pedigree endpoint"""

    CSV = 'csv'
    TSV = 'tsv'
    JSON = 'json'


class FamilyUpdateModel(BaseModel):
    """Model for updating a family"""

    id: int
    external_id: Optional[str] = None
    description: Optional[str] = None
    coded_phenotype: Optional[str] = None


@router.post('/{project}/pedigree', operation_id='importPedigree')
async def import_pedigree(
    file: UploadFile = File(...),
    has_header: bool = False,
    create_missing_participants: bool = False,
    perform_sex_check: bool = True,
    connection: Connection = get_project_write_connection,
):
    """Import a pedigree"""
    delimiter = guess_delimiter_by_filename(file.filename)
    family_layer = FamilyLayer(connection)
    reader = csv.reader(codecs.iterdecode(file.file, 'utf-8-sig'), delimiter=delimiter)
    headers = None
    if has_header:
        headers = next(reader)

    rows = [r for r in reader if not r[0].startswith('#')]
    if len(rows[0]) == 1:
        raise ValueError(
            'Only one column was detected in the pedigree, ensure the file is TAB separated (\\t)'
        )

    return {
        'success': await family_layer.import_pedigree(
            headers,
            rows,
            create_missing_participants=create_missing_participants,
            perform_sex_check=perform_sex_check,
        )
    }


@router.get(
    '/{project}/pedigree',
    operation_id='getPedigree',
)
async def get_pedigree(
    internal_family_ids: List[int] = Query(None),
    response_type: ContentType = ContentType.JSON,
    replace_with_participant_external_ids: bool = True,
    replace_with_family_external_ids: bool = True,
    include_header: bool = True,
    empty_participant_value: Optional[str] = '',
    connection: Connection = get_project_readonly_connection,
):
    """
    Generate tab-separated Pedigree file for ALL families
    unless internal_family_ids is specified.

    Allow replacement of internal participant and family IDs
    with their external counterparts.
    """

    family_layer = FamilyLayer(connection)
    assert connection.project
    pedigree_rows = await family_layer.get_pedigree(
        project=connection.project,
        family_ids=internal_family_ids,
        replace_with_participant_external_ids=replace_with_participant_external_ids,
        replace_with_family_external_ids=replace_with_family_external_ids,
        empty_participant_value=empty_participant_value,
        include_header=True,
    )

    if response_type in (ContentType.CSV, ContentType.TSV):
        delim = '\t' if response_type == ContentType.TSV else ','
        output = io.StringIO()
        writer = csv.writer(output, delimiter=delim)

        if not include_header:
            pedigree_rows.pop(0)

        writer.writerows(pedigree_rows)

        basefn = f'{connection.project}-{date.today().isoformat()}'

        if internal_family_ids:
            basefn += '-'.join(str(fm) for fm in internal_family_ids)

        return StreamingResponse(
            iter(output.getvalue()),
            media_type=f'text/{response_type}',
            headers={'Content-Disposition': f'filename={basefn}.ped'},
        )

    # Return json by default
    def key_convert(string):
        snake = string.lower().replace(' ', '_').replace('-', '_')
        return re.sub(r'^[^a-zA-Z0-9]+', '', snake)

    header = pedigree_rows.pop(0)
    header = [key_convert(x) for x in header]
    data = [dict(zip(header, x)) for x in pedigree_rows]
    return data


@router.post('/{project}/', operation_id='getFamilies')
async def get_families(
    participant_ids: Optional[List[int]] = Query(None),
    sample_ids: Optional[List[str]] = Query(None),
    connection: Connection = get_project_readonly_connection,
) -> List[Family]:
    """Get families for some project"""
    family_layer = FamilyLayer(connection)
    sample_ids_raw = sample_id_transform_to_raw_list(sample_ids) if sample_ids else None

    return await family_layer.get_families(
        participant_ids=participant_ids, sample_ids=sample_ids_raw
    )


@router.post('/', operation_id='updateFamily')
async def update_family(
    family: FamilyUpdateModel,
    connection: Connection = get_projectless_db_connection,
):
    """Update information for a single family"""
    family_layer = FamilyLayer(connection)
    return {
        'success': await family_layer.update_family(
            id_=family.id,
            external_id=family.external_id,
            description=family.description,
            coded_phenotype=family.coded_phenotype,
        )
    }


@router.post('/{project}/family-template', operation_id='importFamilies')
async def import_families(
    file: UploadFile = File(...),
    has_header: bool = True,
    delimiter='\t',
    connection: Connection = get_project_write_connection,
):
    """Import a family csv"""
    delimiter = guess_delimiter_by_filename(file.filename, default_delimiter=delimiter)

    family_layer = FamilyLayer(connection)
    reader = csv.reader(codecs.iterdecode(file.file, 'utf-8-sig'), delimiter=delimiter)
    headers = None
    if has_header:
        headers = next(reader)

    rows = [r for r in reader if not r[0].startswith('#')]
    if len(rows[0]) == 1:
        raise ValueError(
            'Only one column was detected in the pedigree, ensure the file is TAB separated (\\t)'
        )
    success = await family_layer.import_families(headers, rows)
    return {'success': success}
