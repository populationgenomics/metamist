from typing import Optional, List, Dict

from fastapi import APIRouter, Query
from pydantic import BaseModel

from models.enums import SampleType
from models.models.sample import sample_id_transform_to_raw, sample_id_format
from db.python.tables.sample import SampleTable, Sample
from db.python.tables.project import ProjectTable

from api.utils.db import (
    get_project_db_connection,
    Connection,
    get_projectless_db_connection,
)


class NewSample(BaseModel):
    """Model for creating new sample"""

    external_id: str
    type: SampleType
    meta: Dict = {}
    participant_id: Optional[int] = None


class SampleUpdateModel(BaseModel):
    """Update model for Sample"""

    meta: Optional[Dict] = {}
    type: Optional[SampleType] = None
    participant_id: Optional[int] = None
    active: Optional[bool] = None


router = APIRouter(prefix='/sample', tags=['sample'])


@router.put('/{project}/', response_model=str, operation_id='createNewSample')
async def create_new_sample(
    sample: NewSample, connection: Connection = get_project_db_connection
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


@router.get('/{project}/id-map/external', operation_id='getSampleIdMapByExternal')
async def get_sample_id_map_by_external(
    allow_missing: bool = False,
    external_ids: List[str] = Query(...),
    connection: Connection = get_project_db_connection,
):
    """Get map of sample IDs, { [externalId]: internal_sample_id }"""
    st = SampleTable(connection)
    result = await st.get_sample_id_map_by_external_ids(
        external_ids, allow_missing=(allow_missing or False)
    )
    return {k: sample_id_format(v) for k, v in result.items()}


@router.get('/id-map/internal', operation_id='getSampleIdMapByInternal')
async def get_sample_id_map_by_internal(
    internal_ids: List[str] = Query(...),
    connection: Connection = get_projectless_db_connection,
):
    """
    Get map of sample IDs, { [internal_id]: external_sample_id }
    Without specifying a project, you might see duplicate external identifiers
    """
    st = SampleTable(connection)
    internal_ids_raw = sample_id_transform_to_raw(internal_ids)
    result = await st.get_sample_id_map_by_internal_ids(internal_ids_raw)
    return {sample_id_format(k): v for k, v in result.items()}


@router.get(
    '/{project}/id-map/internal/all', operation_id='getAllSampleIdMapByInternal'
)
async def get_all_sample_id_map_by_internal(
    connection: Connection = get_project_db_connection,
):
    """Get map of ALL sample IDs, { [internal_id]: external_sample_id }"""
    st = SampleTable(connection)
    result = await st.get_all_sample_id_map_by_internal_ids()
    return {sample_id_format(k): v for k, v in result.items()}


@router.get(
    '/{project}/{external_id}/details',
    response_model=Sample,
    operation_id='getSampleByExternalId',
)
async def get_sample_by_external_id(
    external_id: str, connection: Connection = get_project_db_connection
):
    """Get sample by external ID"""
    st = SampleTable(connection)
    result = await st.get_single_by_external_id(external_id)
    return result


@router.post('/', operation_id='getSamples')
async def get_samples_by_criteria(
    sample_ids: List[str] = None,
    meta: Dict = None,
    participant_ids: List[int] = None,
    project_ids: List[str] = None,
    connection: Connection = get_projectless_db_connection,
):
    """
    Get list of samples (dict) by some mixture of (AND'd) criteria
    """
    st = SampleTable(connection)
    if not project_ids:
        raise ValueError('You must specify at least one project id for this method')

    pt = ProjectTable(connection.connection)
    internal_project_id_map = await pt.get_project_id_map()
    project_ids = [internal_project_id_map[pid] for pid in project_ids]

    sample_ids_raw = sample_id_transform_to_raw(sample_ids) if sample_ids else None

    result = await st.get_samples_by(
        sample_ids=sample_ids_raw,
        meta=meta,
        participant_ids=participant_ids,
        project_ids=project_ids,
    )

    for sample in result:
        sample.id = sample_id_format(sample.id)

    return result


@router.patch('/{id_}', operation_id='updateSample')
async def update_sample(
    id_: str,
    model: SampleUpdateModel,
    connection: Connection = get_projectless_db_connection,
):
    """Update sample with id"""
    st = SampleTable(connection)
    result = await st.update_sample(
        id_=sample_id_transform_to_raw(id_),
        meta=model.meta,
        participant_id=model.participant_id,
        type_=model.type,
        active=model.active,
    )
    return result
