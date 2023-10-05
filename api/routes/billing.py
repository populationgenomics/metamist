"""
Billing routes
"""

from fastapi import APIRouter
from api.utils.db import (
    BqConnection,
    get_projectless_bq_connection,
)
from db.python.layers.billing import BillingLayer
from models.models.billing import (
    BillingQueryModel,
    BillingTopicCostCategoryRecord,
    BillingRowRecord,
)

router = APIRouter(prefix='/billing', tags=['billing'])


@router.get(
    '/topic',
    response_model=list[str],
    operation_id='getTopic',
)
async def get_topic(
    connection: BqConnection = get_projectless_bq_connection,
) -> list[str]:
    """Get list of all topics in database"""

    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_topic()
    return records


@router.get(
    '/category',
    response_model=list[str],
    operation_id='getCostCategory',
)
async def get_cost_category(
    connection: BqConnection = get_projectless_bq_connection,
) -> list[str]:
    """Get list of all topics in database"""

    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_cost_category()
    return records


@router.get(
    '/cost-by-topic',
    response_model=list[BillingTopicCostCategoryRecord],
    operation_id='getCostByTopic',
)
async def get_cost_by_topic(
    start_date: str | None = '2023-03-01',
    end_date: str | None = '2023-03-01',
    topic: str | None = 'hail',
    cost_category: str | None = None,
    order_by: str | None = 'day ASC, cost_category ASC',
    limit: int = 10,
    connection: BqConnection = get_projectless_bq_connection,
) -> list[BillingTopicCostCategoryRecord]:
    """Get Billing cost by topic, cost category and time interval"""

    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_cost_by_topic(
        start_date, end_date, topic, cost_category, order_by, limit
    )
    return records


@router.post(
    '/query', response_model=list[BillingRowRecord], operation_id='queryBilling'
)
async def query_billing(
    query: BillingQueryModel,
    limit: int = 10,
    connection: BqConnection = get_projectless_bq_connection,
) -> list[BillingRowRecord]:
    """
    Get Billing records by some criteria, date is required to minimize BQ cost

    E.g.

        {
            "topic": ["hail"],
            "date": "2023-03-02",
            "cost_category": ["Hail compute Credit"]
        }

    """

    billing_layer = BillingLayer(connection)
    records = await billing_layer.query(query.to_filter(), limit)
    return records
