import datetime
from enum import Enum

from db.python.tables.billing import BillingFilter
from db.python.utils import GenericFilter
from models.base import SMBase


class BillingQueryModel(SMBase):
    """Used to query for billing"""

    # topic is cluster index, provide some values to make it more efficient
    topic: list[str] | None = None

    # make date required, to avoid full table scan
    date: str

    cost_category: list[str] | None = None

    def to_filter(self) -> BillingFilter:
        """Convert to internal analysis filter"""
        return BillingFilter(
            topic=GenericFilter(in_=self.topic) if self.topic else None,
            date=GenericFilter(eq=self.date),
            cost_category=GenericFilter(in_=self.cost_category)
            if self.cost_category
            else None,
        )

    def __hash__(self):
        """Create hash for this object to use in caching"""
        return hash(self.json())


class BillingRowRecord(SMBase):
    """Return class for the Billing record"""

    id: str
    topic: str | None
    service_id: str | None
    service_description: str | None

    sku_id: str | None
    sku_description: str | None

    usage_start_time: datetime.datetime | None
    usage_end_time: datetime.datetime | None

    gcp_project_id: str | None
    gcp_project_number: str | None
    gcp_project_name: str | None

    # labels
    dataset: str | None
    batch_id: str | None
    job_id: str | None
    batch_name: str | None
    sequencing_type: str | None
    stage: str | None
    sequencing_group: str | None

    export_time: datetime.datetime | None
    cost: str | None
    currency: str | None
    currency_conversion_rate: str | None
    invoice_month: str | None
    cost_type: str | None

    class Config:
        """Config for BillingRowRecord Response"""

        orm_mode = True

    @staticmethod
    def from_json(record):
        """Create BillingRowRecord from json"""

        record['service'] = record['service'] if record['service'] else {}
        record['project'] = record['project'] if record['project'] else {}
        record['invoice'] = record['invoice'] if record['invoice'] else {}
        record['sku'] = record['sku'] if record['sku'] else {}

        labels = {}

        if record['labels']:
            for lbl in record['labels']:
                labels[lbl['key']] = lbl['value']

        record['labels'] = labels

        return BillingRowRecord(
            id=record['id'],
            topic=record['topic'],
            service_id=record['service'].get('id'),
            service_description=record['service'].get('description'),
            sku_id=record['sku'].get('id'),
            sku_description=record['sku'].get('description'),
            usage_start_time=record['usage_start_time'],
            usage_end_time=record['usage_end_time'],
            gcp_project_id=record['project'].get('id'),
            gcp_project_number=record['project'].get('number'),
            gcp_project_name=record['project'].get('name'),
            # labels
            dataset=record['labels'].get('dataset'),
            batch_id=record['labels'].get('batch_id'),
            job_id=record['labels'].get('job_id'),
            batch_name=record['labels'].get('batch_name'),
            sequencing_type=record['labels'].get('sequencing_type'),
            stage=record['labels'].get('stage'),
            sequencing_group=record['labels'].get('sequencing_group'),
            export_time=record['export_time'],
            cost=record['cost'],
            currency=record['currency'],
            currency_conversion_rate=record['currency_conversion_rate'],
            invoice_month=record['invoice'].get('month', ''),
            cost_type=record['cost_type'],
        )


class BillingColumn(str, Enum):
    """List of billing columns"""

    # base view columns
    TOPIC = 'topic'
    PROJECT = 'gcp_project'
    DAY = 'day'
    COST_CATEGORY = 'cost_category'
    SKU = 'sku'
    AR_GUID = 'ar_guid'
    CURRENCY = 'currency'
    COST = 'cost'
    INVOICE_MONTH = 'invoice_month'

    # extended, filtered view columns
    DATASET = 'dataset'
    BATCH_ID = 'batch_id'
    SEQUENCING_TYPE = 'sequencing_type'
    STAGE = 'stage'
    SEQUENCING_GROUP = 'sequencing_group'
    COMPUTE_CATEGORY = 'compute_category'
    CROMWELL_SUB_WORKFLOW_NAME = 'cromwell_sub_workflow_name'
    CROMWELL_WORKFLOW_ID = 'cromwell_workflow_id'
    GOOG_PIPELINES_WORKER = 'goog_pipelines_worker'
    WDL_TASK_NAME = 'wdl_task_name'

    @classmethod
    def extended_cols(cls) -> list[str]:
        """Return list of extended column names"""
        return [
            'dataset',
            'batch_id',
            'sequencing_type',
            'stage',
            'sequencing_group',
            'ar_guid',
            'compute_category',
            'cromwell_sub_workflow_name',
            'cromwell_workflow_id',
            'goog_pipelines_worker',
            'wdl_task_name',
        ]

    @staticmethod
    def generate_all_title(record) -> str:
        """Generate Column as All Title"""
        if record == BillingColumn.PROJECT:
            return 'All GCP Projects'

        return f'All {record.title()}s'


class BillingTotalCostQueryModel(SMBase):
    """
    Used to query for billing total cost
    TODO: needs to be fully implemented, esp. to_filter
    """

    # required
    fields: list[BillingColumn]
    start_date: str
    end_date: str
    # optional, can be aggregate or gcp_billing
    source: str | None = None

    # optional
    filters: dict[BillingColumn, str] | None = None
    # optional, AND or OR
    filters_op: str | None = None

    # order by, reverse= TRUE for DESC, FALSE for ASC
    order_by: dict[BillingColumn, bool] | None = None
    limit: int | None = None
    offset: int | None = None

    def __hash__(self):
        """Create hash for this object to use in caching"""
        return hash(self.json())


class BillingTotalCostRecord(SMBase):
    """Return class for the Billing Total Cost record"""

    day: datetime.date | None
    topic: str | None
    gcp_project: str | None
    cost_category: str | None
    sku: str | None
    ar_guid: str | None
    # extended columns
    dataset: str | None
    batch_id: str | None
    sequencing_type: str | None
    stage: str | None
    sequencing_group: str | None
    compute_category: str | None
    cromwell_sub_workflow_name: str | None
    cromwell_workflow_id: str | None
    goog_pipelines_worker: str | None
    wdl_task_name: str | None

    cost: float
    currency: str | None

    @staticmethod
    def from_json(record):
        """Create BillingTopicCostCategoryRecord from json"""
        return BillingTotalCostRecord(
            day=record.get('day'),
            topic=record.get('topic'),
            gcp_project=record.get('gcp_project'),
            cost_category=record.get('cost_category'),
            sku=record.get('sku'),
            ar_guid=record.get('ar_guid'),
            dataset=record.get('dataset'),
            batch_id=record.get('batch_id'),
            sequencing_type=record.get('sequencing_type'),
            stage=record.get('stage'),
            sequencing_group=record.get('sequencing_group'),
            compute_category=record.get('compute_category'),
            cromwell_sub_workflow_name=record.get('cromwell_sub_workflow_name'),
            cromwell_workflow_id=record.get('cromwell_workflow_id'),
            goog_pipelines_worker=record.get('goog_pipelines_worker'),
            wdl_task_name=record.get('wdl_task_name'),
            cost=record.get('cost'),
            currency=record.get('currency'),
        )


class BillingCostDetailsRecord(SMBase):
    """_summary_"""

    cost_group: str
    cost_category: str
    daily_cost: float | None
    monthly_cost: float | None

    @staticmethod
    def from_json(record):
        """Create BillingCostDetailsRecord from json"""
        return BillingCostDetailsRecord(
            cost_group=record.get('cost_group'),
            cost_category=record.get('cost_category'),
            daily_cost=record.get('daily_cost'),
            monthly_cost=record.get('monthly_cost'),
        )


class BillingCostBudgetRecord(SMBase):
    """Return class for the Billing Total Budget / Cost record"""

    field: str | None
    total_monthly: float | None
    total_daily: float | None

    compute_monthly: float | None
    compute_daily: float | None
    storage_monthly: float | None
    storage_daily: float | None
    details: list[BillingCostDetailsRecord] | None
    budget_spent: float | None

    last_loaded_day: str | None

    @staticmethod
    def from_json(record):
        """Create BillingTopicCostCategoryRecord from json"""
        return BillingCostBudgetRecord(
            field=record.get('field'),
            total_monthly=record.get('total_monthly'),
            total_daily=record.get('total_daily'),
            compute_monthly=record.get('compute_monthly'),
            compute_daily=record.get('compute_daily'),
            storage_monthly=record.get('storage_monthly'),
            storage_daily=record.get('storage_daily'),
            details=[
                BillingCostDetailsRecord.from_json(row) for row in record.get('details')
            ],
            budget_spent=record.get('budget_spent'),
            last_loaded_day=record.get('last_loaded_day'),
        )
