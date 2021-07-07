from typing import List

from fastapi import APIRouter, Body

from db.python.tables.sample import SampleTable, Sample

from api.utils.db import get_db_connection, Connection


router = APIRouter(prefix='/sample', tags=['sample'])


@router.post('/id-map', operation_id='getSampleIdMap')
async def get_sample_id_map(
    external_ids: List[str] = Body(..., embed=True),
    connection: Connection = get_db_connection,
):
    """Get map of sample IDs, { [externalId]: internal_sample_id }"""
    st = SampleTable(connection)
    result = await st.get_sample_id_map(external_ids)
    return result


@router.get(
    '/{id_}/details', response_model=Sample, operation_id='getSampleByExternalId'
)
async def get_sample_by_external_id(
    id_: str, connection: Connection = get_db_connection
):
    """Get sample by external ID"""
    st = SampleTable(connection)
    result = await st.get_single_by_external_id(id_)
    return result
