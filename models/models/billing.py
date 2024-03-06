import datetime
from enum import Enum

from db.python.tables.bq.billing_filter import BillingFilter
from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from models.base import SMBase
from models.enums.billing import BillingSource, BillingTimeColumn, BillingTimePeriods


class BillingInternal(SMBase):
    """Model for Analysis"""

    id: str | None
    ar_guid: str | None
    gcp_project: str | None
    topic: str | None
    batch_id: str | None
    cost_category: str | None
    cost: float | None
    day: datetime.date | None

    @staticmethod
    def from_db(**kwargs):
        """
        Convert from db keys, mainly converting id to id_
        """
        return BillingInternal(
            id=kwargs.get('id'),
            ar_guid=kwargs.get('ar_guid', kwargs.get('ar-guid')),
            gcp_project=kwargs.get('gcp_project'),
            topic=kwargs.get('topic'),
            batch_id=kwargs.get('batch_id'),
            cost_category=kwargs.get('cost_category'),
            cost=kwargs.get('cost'),
            day=kwargs.get('day'),
        )


class BillingColumn(str, Enum):
    """List of billing columns"""

    # raw view columns
    ID = 'id'
    TOPIC = 'topic'
    SERVICE = 'service'
    SKU = 'sku'
    USAGE_START_TIME = 'usage_start_time'
    USAGE_END_TIME = 'usage_end_time'
    PROJECT = 'project'
    LABELS = 'labels'
    SYSTEM_LABELS = 'system_labels'
    LOCATION = 'location'
    EXPORT_TIME = 'export_time'
    COST = 'cost'
    CURRENCY = 'currency'
    CURRENCY_CONVERSION_RATE = 'currency_conversion_rate'
    USAGE = 'usage'
    CREDITS = 'credits'
    INVOICE = 'invoice'
    COST_TYPE = 'cost_type'
    ADJUSTMENT_INFO = 'adjustment_info'

    # base view columns
    # TOPIC = 'topic'
    # SKU = 'sku'
    # CURRENCY = 'currency'
    # COST = 'cost'
    # LABELS = 'labels'
    GCP_PROJECT = 'gcp_project'
    DAY = 'day'
    COST_CATEGORY = 'cost_category'
    AR_GUID = 'ar_guid'
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
    NAMESPACE = 'namespace'

    @classmethod
    def can_group_by(cls, value: 'BillingColumn') -> bool:
        """
        Return True if column can be grouped by
        TODO: If any new columns are added above and cannot be in a group by, add them here
        This could be record, array or struct type
        """
        return value not in (
            BillingColumn.COST,
            BillingColumn.SERVICE,
            # BillingColumn.SKU,
            BillingColumn.PROJECT,
            BillingColumn.LABELS,
            BillingColumn.SYSTEM_LABELS,
            BillingColumn.LOCATION,
            BillingColumn.USAGE,
            BillingColumn.CREDITS,
            BillingColumn.INVOICE,
            BillingColumn.ADJUSTMENT_INFO,
        )

    @classmethod
    def is_extended_column(cls, value: 'BillingColumn') -> bool:
        """Return True if column is extended"""
        return value in (
            BillingColumn.DATASET,
            BillingColumn.BATCH_ID,
            BillingColumn.SEQUENCING_TYPE,
            BillingColumn.STAGE,
            BillingColumn.SEQUENCING_GROUP,
            BillingColumn.COMPUTE_CATEGORY,
            BillingColumn.CROMWELL_SUB_WORKFLOW_NAME,
            BillingColumn.CROMWELL_WORKFLOW_ID,
            BillingColumn.GOOG_PIPELINES_WORKER,
            BillingColumn.WDL_TASK_NAME,
            BillingColumn.NAMESPACE,
        )

    @classmethod
    def str_to_enum(cls, value: str) -> 'BillingColumn':
        """Convert string to enum"""
        # all column names have underscore in SQL, but dash in UI / stored data
        adjusted_value = value.replace('-', '_')
        str_to_enum = {v.value: v for k, v in BillingColumn.__members__.items()}
        return str_to_enum[adjusted_value]

    @classmethod
    def raw_cols(cls) -> list[str]:
        """Return list of raw column names"""
        return [
            # BillingColumn.ID.value,
            BillingColumn.TOPIC.value,
            # BillingColumn.SERVICE.value,
            BillingColumn.SKU.value,
            BillingColumn.USAGE_START_TIME.value,
            BillingColumn.USAGE_END_TIME.value,
            # BillingColumn.PROJECT.value,
            BillingColumn.LABELS.value,
            # BillingColumn.SYSTEM_LABELS.value,
            # BillingColumn.LOCATION.value,
            # BillingColumn.EXPORT_TIME.value,
            BillingColumn.COST.value,
            BillingColumn.CURRENCY.value,
            # BillingColumn.CURRENCY_CONVERSION_RATE.value,
            # BillingColumn.USAGE.value,
            # BillingColumn.CREDITS.value,
            # BillingColumn.INVOICE.value,
            # BillingColumn.COST_TYPE.value,
            # BillingColumn.ADJUSTMENT_INFO.value,
        ]

    @classmethod
    def standard_cols(cls) -> list[str]:
        """Return list of standard column names"""
        return [
            BillingColumn.TOPIC.value,
            BillingColumn.GCP_PROJECT.value,
            BillingColumn.SKU.value,
            BillingColumn.CURRENCY.value,
            BillingColumn.COST.value,
            BillingColumn.LABELS.value,
            BillingColumn.DAY.value,
            BillingColumn.COST_CATEGORY.value,
            BillingColumn.AR_GUID.value,
            BillingColumn.INVOICE_MONTH.value,
        ]

    @classmethod
    def extended_cols(cls) -> list[str]:
        """Return list of extended column names"""
        return [
            BillingColumn.DATASET.value,
            BillingColumn.BATCH_ID.value,
            BillingColumn.SEQUENCING_TYPE.value,
            BillingColumn.STAGE.value,
            BillingColumn.SEQUENCING_GROUP.value,
            BillingColumn.AR_GUID.value,
            BillingColumn.COMPUTE_CATEGORY.value,
            BillingColumn.CROMWELL_SUB_WORKFLOW_NAME.value,
            BillingColumn.CROMWELL_WORKFLOW_ID.value,
            BillingColumn.GOOG_PIPELINES_WORKER.value,
            BillingColumn.WDL_TASK_NAME.value,
            BillingColumn.NAMESPACE.value,
        ]

    @staticmethod
    def generate_all_title(record) -> str:
        """Generate Column as All Title"""
        if record == BillingColumn.GCP_PROJECT:
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
    # optional, can be raw, aggregate or gcp_billing
    source: BillingSource | None = None

    # optional
    filters: dict[BillingColumn, str | list | dict] | None = None
    # optional, AND or OR
    filters_op: str | None = None
    group_by: bool = True

    # order by, reverse= TRUE for DESC, FALSE for ASC
    order_by: dict[BillingColumn, bool] | None = None
    limit: int | None = None
    offset: int | None = None

    # default to day, can be day, week, month, invoice_month
    time_column: BillingTimeColumn | None = None
    time_periods: BillingTimePeriods | None = None

    # optional, show the min cost, e.g. 0.01, if not set, will show all
    min_cost: float | None = None

    def __hash__(self):
        """Create hash for this object to use in caching"""
        return hash(self.json())

    def to_filter(self) -> BillingFilter:
        """
        Convert to internal analysis filter
        """
        billing_filter = BillingFilter()
        if self.filters:
            # add filters as attributes
            for fk, fv in self.filters.items():
                # fk is BillColumn, fv is value
                setattr(billing_filter, fk.value, GenericBQFilter(eq=fv))

        return billing_filter


class BillingTotalCostRecord(SMBase):
    """Return class for the Billing Total Cost record"""

    day: datetime.date | None
    topic: str | None
    gcp_project: str | None
    cost_category: str | None
    sku: str | dict | None
    invoice_month: str | None
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
    namespace: str | None

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
            invoice_month=record.get('invoice_month'),
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
            namespace=record.get('namespace'),
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
    budget: float | None

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
            budget=record.get('budget'),
            last_loaded_day=record.get('last_loaded_day'),
        )


class BillingBatchCostRecord(SMBase):
    """Return class for the Billing Cost by batch_id/ar_guid"""

    total: dict | None
    topics: list[dict] | None
    categories: list[dict] | None
    batches: list[dict] | None
    skus: list[dict] | None
    seq_groups: list[dict] | None

    wdl_tasks: list[dict] | None
    cromwell_sub_workflows: list[dict] | None
    cromwell_workflows: list[dict] | None
    dataproc: list[dict] | None

    @staticmethod
    def from_json(record):
        """Create BillingBatchCostRecord from json"""
        return BillingBatchCostRecord(
            total=record.get('total'),
            topics=record.get('topics'),
            categories=record.get('categories'),
            batches=record.get('batches'),
            skus=record.get('skus'),
            seq_groups=record.get('seq_groups'),
            wdl_tasks=record.get('wdl_tasks'),
            cromwell_sub_workflows=record.get('cromwell_sub_workflows'),
            cromwell_workflows=record.get('cromwell_workflows'),
            dataproc=record.get('dataproc'),
        )

    # ar_guid: str | None
    # batch_id: str | None
    # batch_name: str | None
    # jobs: int | None
    # job_id: int | None

    # topic: str | None
    # namespace: str | None

    # compute_category: str | None
    # cromwell_workflow_id: str | None
    # cromwell_sub_workflow_name: str | None
    # wdl_task_name: str | None

    # start_time: datetime.datetime | None
    # end_time: datetime.datetime | None
    # cost: float | None

    # details: list[dict] | None
    # labels: dict | None

    # @staticmethod
    # def from_json(record):
    #     """Create BillingBatchCostRecord from json"""
    #     return BillingBatchCostRecord(
    #         ar_guid=record.get('ar_guid'),
    #         batch_id=record.get('batch_id'),
    #         batch_name=record.get('batch_name'),
    #         jobs=record.get('jobs'),
    #         job_id=record.get('job_id'),
    #         topic=record.get('topic'),
    #         namespace=record.get('namespace'),
    #         compute_category=record.get('compute_category'),
    #         cromwell_workflow_id=record.get('cromwell_workflow_id'),
    #         cromwell_sub_workflow_name=record.get('cromwell_sub_workflow_name'),
    #         wdl_task_name=record.get('wdl_task_name'),
    #         start_time=record.get('start_time'),
    #         end_time=record.get('end_time'),
    #         cost=record.get('cost'),
    #         details=record.get('details'),
    #         labels=json.loads(record.get('labels')),
    #     )
