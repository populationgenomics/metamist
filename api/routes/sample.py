from typing import Optional, Any

from fastapi import APIRouter, Body

from api.utils.db import (
    get_project_write_connection,
    get_project_readonly_connection,
    Connection,
    get_projectless_db_connection,
)
from db.python.layers.sample import SampleLayer
from db.python.tables.project import ProjectPermissionsTable
from models.models.sample import SampleUpsert
from models.utils.sample_id_format import (
    # Sample,
    sample_id_transform_to_raw,
    sample_id_format,
    sample_id_transform_to_raw_list,
)

router = APIRouter(prefix='/sample', tags=['sample'])

# region CREATES


@router.put('/{project}/', response_model=str, operation_id='createSample')
async def create_sample(
    sample: SampleUpsert, connection: Connection = get_project_write_connection
) -> str:
    """Creates a new sample, and returns the internal sample ID"""
    st = SampleLayer(connection)
    internal_sid = await st.upsert_sample(sample.to_internal())
    return sample_id_format(internal_sid.id)


@router.put(
    '/{project}/upsert-many',
    response_model=dict[str, Any],
    operation_id='upsertSamples',
)
async def upsert_samples(
    samples: list[SampleUpsert],
    connection: Connection = get_project_write_connection,
):
    """
    Upserts a list of samples with sequencing-groups,
    and returns the list of internal sample IDs
    """

    # Table interfaces
    st = SampleLayer(connection)

    internal_samples = [sample.to_internal() for sample in samples]
    upserted = await st.upsert_samples(internal_samples)

    return [s.to_external() for s in upserted]


# endregion CREATES

# region GETS


@router.post('/{project}/id-map/external', operation_id='getSampleIdMapByExternal')
async def get_sample_id_map_by_external(
    external_ids: list[str],
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
    internal_ids: list[str],
    connection: Connection = get_projectless_db_connection,
):
    """
    Get map of sample IDs, { [internal_id]: external_sample_id }
    Without specifying a project, you might see duplicate external identifiers
    """
    st = SampleLayer(connection)
    internal_ids_raw = sample_id_transform_to_raw_list(internal_ids)
    result = await st.get_internal_to_external_sample_id_map(internal_ids_raw)
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
    return result.to_external()


@router.post('/', operation_id='getSamples')
async def get_samples(
    sample_ids: list[str] = None,
    meta: dict = None,
    participant_ids: list[int] = None,
    project_ids: list[str] = None,
    active: bool = Body(default=True),
    connection: Connection = get_projectless_db_connection,
):
    """
    Get list of samples (dict) by some mixture of (AND'd) criteria
    """
    st = SampleLayer(connection)

    pt = ProjectPermissionsTable(connection.connection)
    pids: Optional[list[int]] = None
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

    return [r.to_external() for r in result]


@router.patch('/{id_}', operation_id='updateSample')
async def update_sample(
    id_: str,
    sample: SampleUpsert,
    connection: Connection = get_projectless_db_connection,
):
    """Update sample with id"""
    st = SampleLayer(connection)
    sample.id = id_
    await st.upsert_sample(sample.to_internal())
    return sample


@router.post('/samples-create-date', operation_id='getSamplesCreateDate')
async def get_samples_create_date(
    sample_ids: list[str],
    connection: Connection = get_projectless_db_connection,
):
    """Get full history of sample from internal ID"""
    st = SampleLayer(connection)

    # Check access permissions
    sample_ids_raw = sample_id_transform_to_raw_list(sample_ids) if sample_ids else None

    # Convert to raw ids and query the start dates for all of them
    result = await st.get_samples_create_date(sample_ids_raw)

    return {sample_id_format(k): v for k, v in result.items()}


# endregion GETS

# region OTHER


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


# endregion OTHER
