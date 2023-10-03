"""
Billing routes
"""

import json
from fastapi import APIRouter
from api.utils.db import (
    BqConnection,
    get_projectless_bq_connection,
)
from db.python.layers.billing import BillingLayer
from models.models.billing import BillingRecord

router = APIRouter(prefix='/billing', tags=['billing'])


@router.post(
    '/search',
    response_model=list[BillingRecord],
    operation_id='postBillingSearch',
)
async def post_search(
    filter: str = None,
    connection: BqConnection = get_projectless_bq_connection,
) -> list[BillingRecord]:
    """Get Billing records"""

    filter_dict = {}
    if filter:
        filter_dict = json.loads(filter)

    billing_layer = BillingLayer(connection)
    records = await billing_layer.search_records(filter_dict)
    return records
