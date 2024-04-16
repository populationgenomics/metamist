"""
Billing routes
"""

from async_lru import alru_cache
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from api.settings import BILLING_CACHE_RESPONSE_TTL, BQ_AGGREG_VIEW
from api.utils.db import BqConnection, get_author
from db.python.layers.billing import BillingLayer
from models.enums import BillingSource
from models.models import (
    AnalysisCostRecord,
    BillingColumn,
    BillingCostBudgetRecord,
    BillingTotalCostQueryModel,
    BillingTotalCostRecord,
)

router = APIRouter(prefix='/billing', tags=['billing'])


def _get_billing_layer_from(author: str) -> BillingLayer:
    """
    Initialise billing
    """
    if not is_billing_enabled():
        raise ValueError('Billing is not enabled')

    connection = BqConnection(author)
    billing_layer = BillingLayer(connection)
    return billing_layer


@router.get(
    '/is-billing-enabled',
    response_model=bool,
    operation_id='isBillingEnabled',
)
def is_billing_enabled() -> bool:
    """
    Return true if billing ie enabled, false otherwise
    """
    return BQ_AGGREG_VIEW is not None


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
    billing_layer = _get_billing_layer_from(author)
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
    billing_layer = _get_billing_layer_from(author)
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
    billing_layer = _get_billing_layer_from(author)
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
    billing_layer = _get_billing_layer_from(author)
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
    billing_layer = _get_billing_layer_from(author)
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
    billing_layer = _get_billing_layer_from(author)
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
    billing_layer = _get_billing_layer_from(author)
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
    billing_layer = _get_billing_layer_from(author)
    records = await billing_layer.get_sequencing_groups()
    return records


@router.get(
    '/compute-categories',
    response_model=list[str],
    operation_id='getComputeCategories',
)
@alru_cache(ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_compute_categories(
    author: str = get_author,
) -> list[str]:
    """
    Get list of all compute categories in database
    Results are sorted ASC
    """
    billing_layer = _get_billing_layer_from(author)
    records = await billing_layer.get_compute_categories()
    return records


@router.get(
    '/cromwell-sub-workflow-names',
    response_model=list[str],
    operation_id='getCromwellSubWorkflowNames',
)
@alru_cache(ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_cromwell_sub_workflow_names(
    author: str = get_author,
) -> list[str]:
    """
    Get list of all cromwell_sub_workflow_names in database
    Results are sorted ASC
    """
    billing_layer = _get_billing_layer_from(author)
    records = await billing_layer.get_cromwell_sub_workflow_names()
    return records


@router.get(
    '/wdl-task-names',
    response_model=list[str],
    operation_id='getWdlTaskNames',
)
@alru_cache(ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_wdl_task_names(
    author: str = get_author,
) -> list[str]:
    """
    Get list of all wdl_task_names in database
    Results are sorted ASC
    """
    billing_layer = _get_billing_layer_from(author)
    records = await billing_layer.get_wdl_task_names()
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
    billing_layer = _get_billing_layer_from(author)
    records = await billing_layer.get_invoice_months()
    return records


@router.get(
    '/namespaces',
    response_model=list[str],
    operation_id='getNamespaces',
)
@alru_cache(ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_namespaces(
    author: str = get_author,
) -> list[str]:
    """
    Get list of all namespaces in database
    Results are sorted DESC
    """
    billing_layer = _get_billing_layer_from(author)
    records = await billing_layer.get_namespaces()
    return records


@router.get(
    '/cost-by-ar-guid/{ar_guid}',
    response_model=list[AnalysisCostRecord],
    operation_id='costByArGuid',
)
@alru_cache(maxsize=10, ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_cost_by_ar_guid(
    ar_guid: str,
    author: str = get_author,
) -> JSONResponse:
    """Get Hail Batch costs by AR GUID"""
    billing_layer = _get_billing_layer_from(author)
    records = await billing_layer.get_cost_by_ar_guid(ar_guid)
    headers = {'x-bq-cost': str(billing_layer.connection.cost)}
    json_compatible_item_data = jsonable_encoder(records)
    return JSONResponse(content=json_compatible_item_data, headers=headers)


@router.get(
    '/cost-by-batch-id/{batch_id}',
    response_model=list[AnalysisCostRecord],
    operation_id='costByBatchId',
)
@alru_cache(maxsize=10, ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_cost_by_batch_id(
    batch_id: str,
    author: str = get_author,
) -> JSONResponse:
    """Get Hail Batch costs by Batch ID"""
    billing_layer = _get_billing_layer_from(author)
    records = await billing_layer.get_cost_by_batch_id(batch_id)
    headers = {'x-bq-cost': str(billing_layer.connection.cost)}
    json_compatible_item_data = jsonable_encoder(records)
    return JSONResponse(content=json_compatible_item_data, headers=headers)


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

    12. Get total cost by compute_category order by cost DESC:

        {
            "fields": ["compute_category"],
            "start_date": "2023-11-10",
            "end_date": "2023-11-10",
            "order_by": {"cost": true}
        }

    13. Get total cost by cromwell_sub_workflow_name, order by cost DESC:

        {
            "fields": ["cromwell_sub_workflow_name"],
            "start_date": "2023-11-10",
            "end_date": "2023-11-10",
            "order_by": {"cost": true}
        }

    14. Get total cost by sku for given cromwell_workflow_id, order by cost DESC:

        {
            "fields": ["sku"],
            "start_date": "2023-11-10",
            "end_date": "2023-11-10",
            "filters": {"cromwell_workflow_id": "cromwell-00448f7b-8ef3-4d22-80ab-e302acdb2d28"},
            "order_by": {"cost": true}
        }

    15. Get total cost by sku for given goog_pipelines_worker, order by cost DESC:

        {
            "fields": ["goog_pipelines_worker"],
            "start_date": "2023-11-10",
            "end_date": "2023-11-10",
            "order_by": {"cost": true}
        }

    16. Get total cost by sku for given wdl_task_name, order by cost DESC:

        {
            "fields": ["wdl_task_name"],
            "start_date": "2023-11-10",
            "end_date": "2023-11-10",
            "order_by": {"cost": true}
        }

    17. Get total cost by sku for provided ID, which can be any of
    [ar_guid, batch_id, sequencing_group or cromwell_workflow_id],
    order by cost DESC:

        {
            "fields": ["sku", "ar_guid", "batch_id", "sequencing_group", "cromwell_workflow_id"],
            "start_date": "2023-11-01",
            "end_date": "2023-11-30",
            "filters": {
                "ar_guid": "855a6153-033c-4398-8000-46ed74c02fe8",
                "batch_id": "429518",
                "sequencing_group": "cpg246751",
                "cromwell_workflow_id": "cromwell-e252f430-4143-47ec-a9c0-5f7face1b296"
            },
            "filters_op": "OR",
            "order_by": {"cost": true}
        }

    18. Get weekly total cost by sku for selected cost_category, order by day ASC:

        {
            "fields": ["sku"],
            "start_date": "2022-11-01",
            "end_date": "2023-12-07",
            "filters": {
                "cost_category": "Cloud Storage"
            },
            "order_by": {"day": false},
            "time_periods": "week"
        }

    """
    billing_layer = _get_billing_layer_from(author)
    records = await billing_layer.get_total_cost(query)
    return [BillingTotalCostRecord.from_json(record) for record in records]


@router.get(
    '/running-cost/{field}',
    response_model=list[BillingCostBudgetRecord],
    operation_id='getRunningCost',
)
@alru_cache(ttl=BILLING_CACHE_RESPONSE_TTL)
async def get_running_costs(
    field: BillingColumn,
    invoice_month: str | None = None,
    source: BillingSource | None = None,
    author: str = get_author,
) -> list[BillingCostBudgetRecord]:
    """
    Get running cost for specified fields in database
    e.g. fields = ['gcp_project', 'topic', 'wdl_task_names', 'cromwell_sub_workflow_name', 'compute_category']
    """
    # TODO replace alru_cache with async-cache?
    # so we can skip author for caching?
    # pip install async-cache
    # @AsyncTTL(time_to_live=BILLING_CACHE_RESPONSE_TTL, maxsize=1024, skip_args=2)

    billing_layer = _get_billing_layer_from(author)
    records = await billing_layer.get_running_cost(field, invoice_month, source)
    return records
