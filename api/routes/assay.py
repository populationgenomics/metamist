# pylint: disable=too-many-arguments

from fastapi import APIRouter

from api.utils import get_project_readonly_connection
from api.utils.db import (
    Connection,
    get_projectless_db_connection,
)
from db.python.tables.project import ProjectPermissionsTable
from db.python.layers.assay import (
    AssayLayer,
)
from models.models.assay import AssayUpsert
from models.utils.sample_id_format import (
    sample_id_format,
    sample_id_transform_to_raw,
    sample_id_transform_to_raw_list,
)

router = APIRouter(prefix='/assay', tags=['assay'])


@router.put('/', operation_id='createAssay')
async def create_assay(
    assay: AssayUpsert, connection: Connection = get_projectless_db_connection
):
    """Create new assay, attached to a sample"""
    seq_table = AssayLayer(connection)
    return await seq_table.upsert_assay(assay.to_internal())


@router.patch('/', operation_id='updateAssay')
async def update_assay(
    assay: AssayUpsert,
    connection: Connection = get_projectless_db_connection,
):
    """Update assay for ID"""
    if not assay.id:
        raise ValueError('Assay must have an ID to update')

    assay_layer = AssayLayer(connection)
    await assay_layer.upsert_assay(assay.to_internal())

    return assay.id


@router.get('/{assay_id}/details', operation_id='getAssayById')
async def get_assay_by_id(
    assay_id: int, connection: Connection = get_projectless_db_connection
):
    """Get assay by ID"""
    assay_layer = AssayLayer(connection)
    resp = await assay_layer.get_assay_by_id(assay_id)
    return resp.to_external()


@router.get(
    '/{project}/external_id/{external_id}/details', operation_id='getAssayByExternalId'
)
async def get_assay_by_external_id(
    external_id: str, connection=get_project_readonly_connection
):
    """Get an assay by ONE of its external identifiers"""
    assay_layer = AssayLayer(connection)
    resp = await assay_layer.get_assay_by_external_id(external_assay_id=external_id)
    return resp.to_external()


@router.post('/criteria', operation_id='getAssaysByCriteria')
async def get_assays_by_criteria(
    sample_ids: list[str] = None,
    assay_ids: list[int] = None,
    external_assay_ids: list[str] = None,
    assay_meta: dict = None,
    sample_meta: dict = None,
    projects: list[str] = None,
    active: bool = True,
    assay_types: list[str] = None,
    connection: Connection = get_projectless_db_connection,
):
    """Get assays by criteria"""
    assay_layer = AssayLayer(connection)
    pt = ProjectPermissionsTable(connection.connection)

    pids: list[int] | None = None
    if projects:
        pids = await pt.get_project_ids_from_names_and_user(
            connection.author, projects, readonly=True
        )

    unwrapped_sample_ids: list[int] | None = None
    if sample_ids:
        unwrapped_sample_ids = sample_id_transform_to_raw_list(sample_ids)

    result = await assay_layer.get_assays_by(
        sample_ids=unwrapped_sample_ids,
        assay_ids=assay_ids,
        external_assay_ids=external_assay_ids,
        assay_meta=assay_meta,
        sample_meta=sample_meta,
        project_ids=pids,
        active=active,
        assay_types=assay_types,
    )

    return [a.to_external() for a in result]


@router.get(
    '/ids-for-sample-by-type',
    operation_id='getAssayIdsForSampleIdByType',
)
async def get_assay_ids_for_sample_id(
    sample_id: str,
    connection: Connection = get_projectless_db_connection,
) -> dict[str, list[int]]:
    """Get all assay IDs for internal Sample ID"""
    assay_layer = AssayLayer(connection)
    sample_id_raw = sample_id_transform_to_raw(sample_id)
    assay_ids_map = await assay_layer.get_assay_ids_for_sample_id(sample_id_raw)
    return assay_ids_map


@router.post(
    '/ids-for-samples-by-type',
    operation_id='getAssayIdsForSampleIdsByType',
)
async def get_assay_ids_for_sample_ids_by_type(
    sample_ids: list[str],
    connection: Connection = get_projectless_db_connection,
) -> dict[str, dict[str, list[int]]]:
    """Get all assay IDs keyed by internal Sample ID"""
    assay_layer = AssayLayer(connection)
    sample_ids_raw = sample_id_transform_to_raw_list(sample_ids)
    assay_id_map = await assay_layer.get_assay_ids_for_sample_ids_by_type(
        sample_ids_raw
    )
    return {sample_id_format(k): v for k, v in assay_id_map.items()}
