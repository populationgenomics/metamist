from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Body
from pydantic import BaseModel

from models.enums import SampleType
from models.models.sample import (
    sample_id_transform_to_raw,
    sample_id_format,
    sample_id_transform_to_raw_list,
)

from db.python.layers.sample import SampleBatchUpsertBody, SampleLayer
from db.python.tables.project import ProjectPermissionsTable

from api.utils.db import (
    get_project_write_connection,
    get_project_readonly_connection,
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
    sample: NewSample, connection: Connection = get_project_write_connection
) -> str:
    """Creates a new sample, and returns the internal sample ID"""
    st = SampleLayer(connection)
    async with connection.connection.transaction():
        internal_id = await st.insert_sample(
            external_id=sample.external_id,
            sample_type=sample.type,
            active=True,
            meta=sample.meta,
            participant_id=sample.participant_id,
            # already checked on get_project_write_connection
            check_project_id=False,
        )
        return sample_id_format(internal_id)


@router.put(
    '/{project}/batch', response_model=Dict[str, Any], operation_id='batchUpsertSamples'
)
async def batch_upsert_samples(
    samples: SampleBatchUpsertBody,
    connection: Connection = get_project_write_connection,
) -> Dict[str, Any]:
    """Upserts a list of samples with sequences, and returns the list of internal sample IDs"""

    # Convert id in samples to int
    for sample in samples.samples:
        if sample.id:
            sample.id = sample_id_transform_to_raw(sample.id)

    async with connection.connection.transaction():
        # Table interfaces
        st = SampleLayer(connection)

        results = await st.batch_upsert_samples(samples)

        # Map sids back from ints to strs
        for iid, seqs in results.items():
            data = {'sample_id': sample_id_format(iid), 'sequences': seqs}
            results[iid] = data

        return results


@router.post('/{project}/id-map/external', operation_id='getSampleIdMapByExternal')
async def get_sample_id_map_by_external(
    external_ids: List[str],
    allow_missing: bool = False,
    connection: Connection = get_project_readonly_connection,
):
    """Get map of sample IDs, { [externalId]: internal_sample_id }"""
    st = SampleLayer(connection)
    result = await st.get_sample_id_map_by_external_ids(
        external_ids, allow_missing=(allow_missing or False)
    )
    return {k: sample_id_format(v) for k, v in result.items()}


@router.post('/id-map/internal', operation_id='getSampleIdMapByInternal')
async def get_sample_id_map_by_internal(
    internal_ids: List[str],
    connection: Connection = get_projectless_db_connection,
):
    """
    Get map of sample IDs, { [internal_id]: external_sample_id }
    Without specifying a project, you might see duplicate external identifiers
    """
    st = SampleLayer(connection)
    internal_ids_raw = sample_id_transform_to_raw_list(internal_ids)
    result = await st.get_sample_id_map_by_internal_ids(internal_ids_raw)
    return {sample_id_format(k): v for k, v in result.items()}


@router.get(
    '/{project}/id-map/internal/all', operation_id='getAllSampleIdMapByInternal'
)
async def get_all_sample_id_map_by_internal(
    connection: Connection = get_project_readonly_connection,
):
    """Get map of ALL sample IDs, { [internal_id]: external_sample_id }"""
    st = SampleLayer(connection)
    assert connection.project
    result = await st.get_all_sample_id_map_by_internal_ids(project=connection.project)
    return {sample_id_format(k): v for k, v in result.items()}


@router.get(
    '/{project}/{external_id}/details',
    # Don't support this until openapi 3.1
    # response_model=Sample,
    operation_id='getSampleByExternalId',
)
async def get_sample_by_external_id(
    external_id: str, connection: Connection = get_project_readonly_connection
):
    """Get sample by external ID"""
    st = SampleLayer(connection)
    assert connection.project
    result = await st.get_single_by_external_id(external_id, project=connection.project)
    return result


@router.post('/', operation_id='getSamples')
async def get_samples_by_criteria(
    sample_ids: List[str] = None,
    meta: Dict = None,
    participant_ids: List[int] = None,
    project_ids: List[str] = None,
    active: bool = Body(default=True),
    connection: Connection = get_projectless_db_connection,
):
    """
    Get list of samples (dict) by some mixture of (AND'd) criteria
    """
    st = SampleLayer(connection)

    pt = ProjectPermissionsTable(connection.connection)
    pids: Optional[List[int]] = None
    if project_ids:
        pids = await pt.get_project_ids_from_names_and_user(
            connection.author, project_ids, readonly=True
        )

    sample_ids_raw = sample_id_transform_to_raw_list(sample_ids) if sample_ids else None

    result = await st.get_samples_by(
        sample_ids=sample_ids_raw,
        meta=meta,
        participant_ids=participant_ids,
        project_ids=pids,
        active=active,
        check_project_ids=True,
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
    st = SampleLayer(connection)
    result = await st.update_sample(
        id_=sample_id_transform_to_raw(id_),
        meta=model.meta,
        participant_id=model.participant_id,
        type_=model.type,
        active=model.active,
    )
    return result


@router.patch('/{id_keep}/{id_merge}', operation_id='mergeSamples')
async def merge_samples(
    id_keep: str,
    id_merge: str,
    connection: Connection = get_projectless_db_connection,
):
    """
    Merge one sample into another, this function achieves the merge
    by rewriting all sample_ids of {id_merge} with {id_keep}. You must
    carefully consider if analysis objects need to be deleted, or other
    implications BEFORE running this method.
    """
    st = SampleLayer(connection)
    result = await st.merge_samples(
        id_keep=sample_id_transform_to_raw(id_keep),
        id_merge=sample_id_transform_to_raw(id_merge),
    )
    return result


@router.get('/{id_}/history', operation_id='getHistoryOfSample')
async def get_history_of_sample(
    id_: str, connection: Connection = get_projectless_db_connection
):
    """Get full history of sample from internal ID"""
    st = SampleLayer(connection)
    internal_id = sample_id_transform_to_raw(id_)
    result = await st.get_history_of_sample(internal_id)

    return result
