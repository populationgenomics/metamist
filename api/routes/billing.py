"""
Billing routes
"""
# pylint: disable=too-many-arguments

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
    '/topics',
    response_model=list[str],
    operation_id='getTopics',
)
async def get_topics(
    connection: BqConnection = get_projectless_bq_connection,
) -> list[str]:
    """Get list of all topics in database"""

    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_topics()
    return records


@router.get(
    '/categories',
    response_model=list[str],
    operation_id='getCostCategories',
)
async def get_cost_categories(
    connection: BqConnection = get_projectless_bq_connection,
) -> list[str]:
    """Get list of all service description / cost categories in database"""

    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_cost_categories()
    return records


@router.get(
    '/skus',
    response_model=list[str],
    operation_id='getSkus',
)
async def get_skus(
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
    records = await billing_layer.get_skus(limit, offset)
    return records


@router.get(
    '/datasets',
    response_model=list[str],
    operation_id='getDatasets',
)
async def get_datasets(
    connection: BqConnection = get_projectless_bq_connection,
) -> list[str]:
    """
    Get list of all datasets in database
    Results are sorted ASC
    """

    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_datasets()
    return records


@router.get(
    '/sequencing_types',
    response_model=list[str],
    operation_id='getSequencingTypes',
)
async def get_sequencing_types(
    connection: BqConnection = get_projectless_bq_connection,
) -> list[str]:
    """
    Get list of all sequencing_types in database
    Results are sorted ASC
    """

    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_sequencing_types()
    return records


@router.get(
    '/stages',
    response_model=list[str],
    operation_id='getStages',
)
async def get_stages(
    connection: BqConnection = get_projectless_bq_connection,
) -> list[str]:
    """
    Get list of all stages in database
    Results are sorted ASC
    """

    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_stages()
    return records


@router.get(
    '/sequencing_groups',
    response_model=list[str],
    operation_id='getSequencingGroups',
)
async def get_sequencing_groups(
    connection: BqConnection = get_projectless_bq_connection,
) -> list[str]:
    """
    Get list of all sequencing_groups in database
    Results are sorted ASC
    """

    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_sequencing_groups()
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
    ar_guid: str | None = None,
    dataset: str | None = None,
    batch_id: str | None = None,
    sequencing_type: str | None = None,
    stage: str | None = None,
    sequencing_group: str | None = None,
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

    4. Get total cost per dataset for month of March, order by cost DESC:

            fields: dataset
            start_date: 2023-03-01
            end_date: 2023-03-31
            order_by: -cost

    5. Get total cost of daily cost for dataset 'acute-care', March 2023,
       order by cost DESC:

            fields: day,dataset
            dataset: acute-care
            start_date: 2023-03-01
            end_date: 2023-03-31
            order_by: +day

    6. Get total cost for given batch_id and category for month of March,
       order by cost DESC:

            fields: cost_category,batch_id,sku
            start_date: 2023-03-01
            end_date: 2023-03-31
            order_by: -cost
            batch_id: 423094

    7. Get total sequencing_type for month of March 2023, ordered by cost DESC:

            fields: sequencing_type
            start_date: 2023-03-01
            end_date: 2023-03-31
            order_by: -cost

    8. Get total stage for month of March 2023, ordered by cost DESC:

            fields: stage
            start_date: 2023-03-01
            end_date: 2023-03-31
            order_by: -cost

    9. Get total sequencing_group for month of March 2023, ordered by cost DESC:

            fields: sequencing_group
            start_date: 2023-03-01
            end_date: 2023-03-31
            order_by: -cost

    """

    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_total_cost(
        fields,
        start_date,
        end_date,
        topic,
        cost_category,
        sku,
        ar_guid,
        dataset,
        batch_id,
        sequencing_type,
        stage,
        sequencing_group,
        order_by,
        limit,
        offset,
    )
    return records
