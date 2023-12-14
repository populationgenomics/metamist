"""
Billing routes
"""
from fastapi import APIRouter
from async_lru import alru_cache

from api.settings import BILLING_CACHE_RESPONSE_TTL
from api.utils.db import (
    BqConnection,
    get_author,
)
from db.python.layers.billing import BillingLayer
from models.models.billing import (
    BillingColumn,
    BillingCostBudgetRecord,
    BillingQueryModel,
    BillingRowRecord,
    BillingTotalCostRecord,
    BillingTotalCostQueryModel,
)


router = APIRouter(prefix='/billing', tags=['billing'])


@router.get(
    '/gcp-projects',
    response_model=list[str],
    operation_id='getGcpProjects',
)
@alru_cache(ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_gcp_projects(
    author: str = get_author,
) -> list[str]:
    """Get list of all GCP projects in database"""
    connection = BqConnection(author)
    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_gcp_projects()
    return records


@router.get(
    '/topics',
    response_model=list[str],
    operation_id='getTopics',
)
@alru_cache(ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_topics(
    author: str = get_author,
) -> list[str]:
    """Get list of all topics in database"""
    connection = BqConnection(author)
    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_topics()
    return records


@router.get(
    '/categories',
    response_model=list[str],
    operation_id='getCostCategories',
)
@alru_cache(ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_cost_categories(
    author: str = get_author,
) -> list[str]:
    """Get list of all service description / cost categories in database"""
    connection = BqConnection(author)
    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_cost_categories()
    return records


@router.get(
    '/skus',
    response_model=list[str],
    operation_id='getSkus',
)
@alru_cache(ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_skus(
    limit: int = 10,
    offset: int | None = None,
    author: str = get_author,
) -> list[str]:
    """
    Get list of all SKUs in database
    There is over 400 Skus so limit is required
    Results are sorted ASC
    """
    connection = BqConnection(author)
    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_skus(limit, offset)
    return records


@router.get(
    '/datasets',
    response_model=list[str],
    operation_id='getDatasets',
)
@alru_cache(ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_datasets(
    author: str = get_author,
) -> list[str]:
    """
    Get list of all datasets in database
    Results are sorted ASC
    """
    connection = BqConnection(author)
    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_datasets()
    return records


@router.get(
    '/sequencing-types',
    response_model=list[str],
    operation_id='getSequencingTypes',
)
@alru_cache(ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_sequencing_types(
    author: str = get_author,
) -> list[str]:
    """
    Get list of all sequencing_types in database
    Results are sorted ASC
    """
    connection = BqConnection(author)
    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_sequencing_types()
    return records


@router.get(
    '/stages',
    response_model=list[str],
    operation_id='getStages',
)
@alru_cache(ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_stages(
    author: str = get_author,
) -> list[str]:
    """
    Get list of all stages in database
    Results are sorted ASC
    """
    connection = BqConnection(author)
    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_stages()
    return records


@router.get(
    '/sequencing-groups',
    response_model=list[str],
    operation_id='getSequencingGroups',
)
@alru_cache(ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_sequencing_groups(
    author: str = get_author,
) -> list[str]:
    """
    Get list of all sequencing_groups in database
    Results are sorted ASC
    """
    connection = BqConnection(author)
    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_sequencing_groups()
    return records


@router.get(
    '/invoice-months',
    response_model=list[str],
    operation_id='getInvoiceMonths',
)
@alru_cache(ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_invoice_months(
    author: str = get_author,
) -> list[str]:
    """
    Get list of all invoice months in database
    Results are sorted DESC
    """
    connection = BqConnection(author)
    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_invoice_months()
    return records


@router.post(
    '/query', response_model=list[BillingRowRecord], operation_id='queryBilling'
)
@alru_cache(maxsize=10, ttl=BILLING_CACHE_RESPONSE_TTL)
async def query_billing(
    query: BillingQueryModel,
    limit: int = 10,
    author: str = get_author,
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
    connection = BqConnection(author)
    billing_layer = BillingLayer(connection)
    records = await billing_layer.query(query.to_filter(), limit)
    return records


@router.post(
    '/total-cost',
    response_model=list[BillingTotalCostRecord],
    operation_id='getTotalCost',
)
@alru_cache(maxsize=10, ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_total_cost(
    query: BillingTotalCostQueryModel,
    author: str = get_author,
) -> list[BillingTotalCostRecord]:
    """Get Total cost of selected fields for requested time interval

    Here are few examples of requests:

    1. Get total topic for month of March 2023, ordered by cost DESC:

        {
            "fields": ["topic"],
            "start_date": "2023-03-01",
            "end_date": "2023-03-31",
            "order_by": {"cost": true},
            "source": "aggregate"
        }

    2. Get total cost by day and topic for March 2023, order by day ASC and topic DESC:

        {
            "fields": ["day", "topic"],
            "start_date": "2023-03-01",
            "end_date": "2023-03-05",
            "order_by": {"day": false, "topic": true},
            "limit": 10
        }

    3. Get total cost of sku and cost_category for topic 'hail', March 2023,
       order by cost DESC with Limit and Offset:

        {
            "fields": ["sku", "cost_category"],
            "start_date": "2023-03-01",
            "end_date": "2023-03-31",
            "filters": { "topic": "hail"},
            "order_by": {"cost": true},
            "limit": 5,
            "offset": 10
        }

    4. Get total cost per dataset for month of March, order by cost DESC:

        {
            "fields": ["dataset"],
            "start_date": "2023-03-01",
            "end_date": "2023-03-31",
            "order_by": {"cost": true}
        }

    5. Get total cost of daily cost for dataset 'acute-care', March 2023,
       order by day ASC and dataset DESC:

        {
            "fields": ["day", "dataset"],
            "start_date": "2023-03-01",
            "end_date": "2023-03-31",
            "filters": { "dataset": "acute-care"},
            "order_by": {"day": false, "dataset": true}
        }

    6. Get total cost for given batch_id and category for month of March,
       order by cost DESC:

        {
            "fields": ["cost_category", "batch_id", "sku"],
            "start_date": "2023-03-01",
            "end_date": "2023-03-31",
            "filters": { "batch_id": "423094"},
            "order_by": {"cost": true},
            "limit": 5
        }

    7. Get total sequencing_type for month of March 2023, ordered by cost DESC:

        {
            "fields": ["sequencing_type"],
            "start_date": "2023-03-01",
            "end_date": "2023-03-31",
            "order_by": {"cost": true}
        }

    8. Get total stage for month of March 2023, ordered by cost DESC:

        {
            "fields": ["stage"],
            "start_date": "2023-03-01",
            "end_date": "2023-03-31",
            "order_by": {"cost": true}
        }

    9. Get total sequencing_group for month of March 2023, ordered by cost DESC:

        {
            "fields": ["sequencing_group"],
            "start_date": "2023-03-01",
            "end_date": "2023-03-31",
            "order_by": {"cost": true}
        }

    10. Get total gcp_project for month of March 2023, ordered by cost DESC:

        {
            "fields": ["gcp_project"],
            "start_date": "2023-03-01",
            "end_date": "2023-03-31",
            "order_by": {"cost": true},
            "source": "gcp_billing"
        }

    11. Get total cost by sku for given ar_guid, order by cost DESC:

        {
            "fields": ["sku"],
            "start_date": "2023-10-23",
            "end_date": "2023-10-23",
            "filters": { "ar_guid": "4e53702e-8b6c-48ea-857f-c5d33b7e72d7"},
            "order_by": {"cost": true}
        }

    """

    connection = BqConnection(author)
    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_total_cost(query)
    return records


@router.get(
    '/running-cost/{field}',
    response_model=list[BillingCostBudgetRecord],
    operation_id='getRunningCost',
)
@alru_cache(ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_running_costs(
    field: BillingColumn,
    invoice_month: str | None = None,
    source: str | None = None,
    author: str = get_author,
) -> list[BillingCostBudgetRecord]:
    """
    Get running cost for specified fields in database
    e.g. fields = ['gcp_project', 'topic']
    """

    # TODO replace alru_cache with async-cache?
    # so we can skip author for caching?
    # pip install async-cache
    # @AsyncTTL(time_to_live=BILLING_CACHE_RESPONSE_TTL, maxsize=1024, skip_args=2)

    connection = BqConnection(author)
    billing_layer = BillingLayer(connection)
    records = await billing_layer.get_running_cost(field, invoice_month, source)
    return records
