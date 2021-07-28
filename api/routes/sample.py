from typing import Optional, List, Dict

from fastapi import APIRouter, Body
from pydantic import BaseModel

from models.enums import SampleType
from models.models.sample import sample_id_transform_to_raw, sample_id_format
from db.python.tables.sample import SampleTable, Sample

from api.utils.db import get_db_connection, Connection


class NewSample(BaseModel):
    """Model for creating new sample"""

    external_id: str
    type: SampleType
    meta: Dict = {}
    participant_id: Optional[int] = None


router = APIRouter(prefix='/sample', tags=['sample'])


@router.put('/', response_model=str, operation_id='createNewSample')
async def create_new_sample(
    sample: NewSample, connection: Connection = get_db_connection
) -> str:
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
        return sample_id_format(internal_id)


@router.post('/id-map/external', operation_id='getSampleIdMapByExternal')
async def get_sample_id_map_by_external(
    allow_missing: bool = False,
    external_ids: List[str] = Body(..., embed=True),
    connection: Connection = get_db_connection,
):
    """Get map of sample IDs, { [externalId]: internal_sample_id }"""
    st = SampleTable(connection)
    result = await st.get_sample_id_map_by_external_ids(
        external_ids, allow_missing=(allow_missing or False)
    )
    return {k: sample_id_format(v) for k, v in result.items()}


@router.post('/id-map/internal', operation_id='getSampleIdMapByInternal')
async def get_sample_id_map_by_internal(
    internal_ids: List[str] = Body(..., embed=True),
    connection: Connection = get_db_connection,
):
    """Get map of sample IDs, { [externalId]: internal_sample_id }"""
    st = SampleTable(connection)
    internal_ids_raw = sample_id_transform_to_raw(internal_ids)
    result = await st.get_sample_id_map_by_internal_ids(internal_ids_raw)
    return {sample_id_format(k): v for k, v in result.items()}


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
