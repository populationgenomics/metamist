from typing import Optional, List, Dict

from fastapi import APIRouter, Body
from pydantic import BaseModel

from models.enums import SampleType
from db.python.tables.sample import SampleTable, Sample

from api.utils.db import get_db_connection, Connection


class NewSample(BaseModel):
    """Model for creating new sample"""

    external_id: str
    type: SampleType
    meta: Dict = {}
    participant_id: Optional[int] = None


router = APIRouter(prefix='/sample', tags=['sample'])


@router.post('/', response_model=int, operation_id='createNewSample')
async def create_new_sample(
    sample: NewSample, connection: Connection = get_db_connection
) -> int:
    """Creates a new sample, and returns the internal sample ID"""
    st = SampleTable(connection)
    async with connection.connection.transaction():
        internal_id = await st.insert_sample(
            external_id=sample.external_id,
            sample_type=sample.type,
            active=True,
            meta=sample.meta,
            participant_id=sample.participant_id,
        )
        return internal_id


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
