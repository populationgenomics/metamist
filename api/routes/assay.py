# pylint: disable=too-many-arguments

from fastapi import APIRouter

from api.utils import get_project_readonly_connection
from api.utils.db import Connection, get_projectless_db_connection
from db.python.layers.assay import AssayLayer
from db.python.tables.assay import AssayFilter
from db.python.tables.project import ProjectPermissionsTable
from db.python.utils import GenericFilter
from models.base import SMBase
from models.models.assay import AssayUpsert
from models.utils.sample_id_format import sample_id_transform_to_raw_list

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


class AssayQueryCriteria(SMBase):
    """Crieria for filtering for assays"""

    sample_ids: list[str] | None = None
    assay_ids: list[int] | None = None
    external_assay_ids: list[str] | None = None
    assay_meta: dict | None = None
    sample_meta: dict | None = None
    projects: list[str] | None = None
    assay_types: list[str] | None = None


@router.post('/criteria', operation_id='getAssaysByCriteria')
async def get_assays_by_criteria(
    criteria: AssayQueryCriteria,
    connection: Connection = get_projectless_db_connection,
):
    """Get assays by criteria"""
    assay_layer = AssayLayer(connection)
    pt = ProjectPermissionsTable(connection)

    pids: list[int] | None = None
    if criteria.projects:
        pids = await pt.get_project_ids_from_names_and_user(
            connection.author, criteria.projects, readonly=True
        )

    unwrapped_sample_ids: list[int] | None = None
    if criteria.sample_ids:
        unwrapped_sample_ids = sample_id_transform_to_raw_list(criteria.sample_ids)

    filter_ = AssayFilter(
        sample_id=(
            GenericFilter(in_=unwrapped_sample_ids) if unwrapped_sample_ids else None
        ),
        id=GenericFilter(in_=criteria.assay_ids) if criteria.assay_ids else None,
        external_id=(
            GenericFilter(in_=criteria.external_assay_ids)
            if criteria.external_assay_ids
            else None
        ),
        meta=criteria.assay_meta,
        sample_meta=criteria.sample_meta,
        project=GenericFilter(in_=pids) if pids else None,
        type=GenericFilter(in_=criteria.assay_types) if criteria.assay_types else None,
    )

    result = await assay_layer.query(filter_)

    return [a.to_external() for a in result]
