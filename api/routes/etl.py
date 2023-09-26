"""
Web routes
"""
from typing import Any, Optional

from api.utils.db import (
    BqConnection,
    PubSubConnection,
    get_projectless_bq_connection,
    get_projectless_pubsub_connection
)
from db.python.layers.etl import EtlLayer, EtlPubSub
from fastapi import APIRouter, Request
from models.models.etl import EtlSummary
from models.models.search import SearchItem

router = APIRouter(prefix='/etl', tags=['etl'])


@router.post(
    '/summary',
    response_model=list[EtlSummary],
    operation_id='getEtlSummary',
)
async def get_etl_summary(
    request: Request,
    grid_filter: list[SearchItem],
    limit: int = 20,
    token: Optional[int] = 0,
    connection: BqConnection = get_projectless_bq_connection,
) -> EtlSummary:
    """Creates a new sample, and returns the internal sample ID"""
    
    st = EtlLayer(connection)
    summary = await st.get_etl_summary(
        token=token, limit=limit, grid_filter=grid_filter
    )
    return summary


@router.post(
    '/reload',
    response_model=str,
    operation_id='etlReload',
)
async def etl_reload(
    request: Request,
    request_id: str = None,
    connection: PubSubConnection = get_projectless_pubsub_connection,
) -> str:
    """Resubmit request to the topic
    
    resubmit record to the topic:
    
    {"request_id": "640e8f2e-4e20-4959-8620-7b7741265895"}
    
    """
    
    msg = {
        'request_id': request_id,
    }
    
    pubsub = EtlPubSub(connection)
    result = await pubsub.publish(
        msg=msg,
    )
    return result
