"""
Web routes
"""

from fastapi import APIRouter
from api.utils.db import (
    BqConnection,
    PubSubConnection,
    get_projectless_bq_connection,
    get_projectless_pubsub_connection,
)
from db.python.layers.etl import EtlLayer, EtlPubSub
from models.enums import EtlStatus
from models.models.etl import EtlRecord

router = APIRouter(prefix='/etl', tags=['etl'])


@router.get('/record/{request_id}', operation_id='getEtlRecordById')
async def get_etl_record_by_id(
    request_id: str,
    connection: BqConnection = get_projectless_bq_connection,
):
    """Get ETL record by request_id"""
    if not request_id:
        raise ValueError('request_id must be provided')

    etl_layer = EtlLayer(connection)
    return await etl_layer.get_etl_record(request_id)


@router.get(
    '/summary',
    response_model=list[EtlRecord],
    operation_id='getEtlSummary',
)
async def get_etl_summary(
    source_type: str = None,
    status: EtlStatus = None,
    from_date: str = None,
    connection: BqConnection = get_projectless_bq_connection,
) -> list[EtlRecord]:
    """Get ETL process summary by source_type / status / from_date"""

    etl_layer = EtlLayer(connection)
    summary = await etl_layer.get_etl_summary(source_type, status, from_date)
    return summary


@router.post(
    '/resubmit',
    response_model=str,
    operation_id='etlResubmit',
)
async def etl_resubmit(
    request_id: str = None,
    bq_onnection: BqConnection = get_projectless_bq_connection,
    pubsub_connection: PubSubConnection = get_projectless_pubsub_connection,
):
    """Resubmit record to Transform/Load process by request_id
    request_id is output of Load process
    Only already loaded requests can be resubmitted

    {"request_id": "640e8f2e-4e20-4959-8620-7b7741265895"}

    """
    if not request_id:
        raise ValueError('request_id must be provided')

    # check if request_id is valid
    etl_layer = EtlLayer(bq_onnection)
    rec = await etl_layer.get_etl_record(request_id)
    if rec is None:
        raise ValueError(f'Invalid request_id: {request_id}')

    # do nod reload already sucessfull records
    if rec.status == EtlStatus.SUCCESS:
        raise ValueError(f'Already sucessfully loaded request_id: {request_id}')

    # only reload failed requests
    pubsub = EtlPubSub(pubsub_connection)
    return await pubsub.publish(
        {
            'request_id': request_id,
        }
    )
