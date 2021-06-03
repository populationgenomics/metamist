from fastapi import APIRouter

from db.python.tables.sample import SampleTable, Sample

from api.utils.db import get_db_connection, Connection


router = APIRouter(prefix='/sample', tags=['sample'])


@router.get('/{id_}', response_model=Sample, operation_id='getSampleByExternalId')
async def get_sample_by_external_id(
    id_: str, connection: Connection = get_db_connection
):
    """Get sample by external ID"""
    st = SampleTable(connection)
    result = await st.get_single_by_external_id(id_)
    return result
