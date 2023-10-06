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
    BillingRowRecord,
    BillingTotalCostRecord,
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
    """Get list of all service description / cost categories in database"""

    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_cost_category()
    return records


@router.get(
    '/sku',
    response_model=list[str],
    operation_id='getSku',
)
async def get_sku(
    limit: int = 10,
    offset: int | None = None,
    connection: BqConnection = get_projectless_bq_connection,
) -> list[str]:
    """
    Get list of all SKUs in database
    There is over 400 Skus so limit is required
    Results are sorted ASC
    """

    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_sku(limit, offset)
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


@router.get(
    '/total-cost',
    response_model=list[BillingTotalCostRecord],
    operation_id='getTotalCost',
)
async def get_total_cost(
    fields: str = 'day,topic',
    start_date: str = '2023-03-01',
    end_date: str = '2023-03-05',
    topic: str | None = None,
    cost_category: str | None = None,
    sku: str | None = None,
    order_by: str | None = '+day,-topic',
    limit: int | None = None,
    offset: int | None = None,
    connection: BqConnection = get_projectless_bq_connection,
) -> list[BillingTotalCostRecord]:
    """Get Total cost of selected fields for requested time interval

    Here are few examples:

    1. Get total topic for month of March 2023, ordered by cost DESC:

            fields: topic
            start_date: 2023-03-01
            end_date: 2023-03-31
            order_by: -cost

    2. Get total cost by day and topic for March 2023, order by day ASC and topic DESC:

            fields: day,topic
            start_date: 2023-03-01
            end_date: 2023-03-31
            order_by: +day,-topic

    3. Get total cost of sku and cost_category for topic 'hail', March 2023,
       order by cost DESC with Limit and Offset:

            fields: sku,cost_category
            start_date: 2023-03-01
            end_date: 2023-03-31
            order_by: -cost
            topic: hail
            limit: 5
            offset: 10

    """

    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_total_cost(
        fields, start_date, end_date, topic, cost_category, sku, order_by, limit, offset
    )
    return records
