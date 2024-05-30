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
            BillingColumn.TOPIC.value,
            BillingColumn.SKU.value,
            BillingColumn.USAGE_START_TIME.value,
            BillingColumn.USAGE_END_TIME.value,
            BillingColumn.LABELS.value,
            BillingColumn.COST.value,
            BillingColumn.CURRENCY.value,
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
                # if fv is a list, then use IN filter
                if isinstance(fv, list):
                    setattr(billing_filter, fk.value, GenericBQFilter(in_=fv))
                else:
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


class AnalysisCostRecordTotal(SMBase):
    """A model of attributes about the total for an analysis"""

    ar_guid: str | None
    cost: float | None
    usage_start_time: datetime.datetime | None
    usage_end_time: datetime.datetime | None


class AnalysisCostRecordSku(SMBase):
    """Cost for an SKU"""

    sku: str
    cost: float


class AnalysisCostRecordSeqGroup(SMBase):
    """
    Cost of a sequencing_group for some stage
        sequencing_group = None: cost of all jobs for a stage without a sg
        stage = None: cost of all jobs for a sg without a stage
    """

    sequencing_group: str | None
    stage: str | None
    cost: float


class AnalysisCostRecordBatchJob(SMBase):
    """Hail batch container cost record"""

    job_id: str
    job_name: str | None
    skus: list[AnalysisCostRecordSku]
    cost: float
    usage_start_time: datetime.datetime
    usage_end_time: datetime.datetime

    @classmethod
    def from_dict(cls, d: dict):
        return AnalysisCostRecordBatchJob(
            job_id=d['job_id'],
            job_name=d.get('job_name'),
            skus=[AnalysisCostRecordSku.from_dict(row) for row in d.get('skus') or []],
            cost=d['cost'],
            usage_start_time=d['usage_start_time'],
            usage_end_time=d['usage_end_time'],
        )


class AnalysisCostRecordBatch(SMBase):
    """
    A list of these is on the 'BillingBatchCostRecord'
    """

    batch_id: str
    batch_name: str | None
    cost: float
    usage_start_time: datetime.datetime
    usage_end_time: datetime.datetime
    jobs_cnt: int
    skus: list[AnalysisCostRecordSku]
    jobs: list[AnalysisCostRecordBatchJob]
    seq_groups: list[AnalysisCostRecordSeqGroup]

    @classmethod
    def from_dict(cls, d: dict):
        return AnalysisCostRecordBatch(
            batch_id=d['batch_id'],
            batch_name=d.get('batch_name'),
            cost=d['cost'],
            usage_start_time=d['usage_start_time'],
            usage_end_time=d['usage_end_time'],
            jobs_cnt=d['jobs_cnt'],
            skus=[AnalysisCostRecordSku.from_dict(row) for row in d.get('skus') or []],
            jobs=[
                AnalysisCostRecordBatchJob.from_dict(row) for row in d.get('jobs') or []
            ],
            seq_groups=[
                AnalysisCostRecordSeqGroup.from_dict(row)
                for row in d.get('seq_groups') or []
            ],
        )


class AnalysisCostRecordCategory(SMBase):
    """Category, cost, workflows"""

    category: str
    cost: float
    workflows: int | None

    @classmethod
    def from_dict(cls, d: dict):
        return AnalysisCostRecordCategory(**dict(d))


class AnalysisCostRecordTopic(SMBase):
    """Topic, cost"""

    topic: str
    cost: float

    @classmethod
    def from_dict(cls, d: dict):
        return AnalysisCostRecordTopic(**dict(d))


class AnalysisCostRecordWdlTask(SMBase):
    """Cost of a WDL task"""

    wdl_task_name: str
    cost: float
    usage_start_time: datetime.datetime
    usage_end_time: datetime.datetime
    skus: list[AnalysisCostRecordSku]


class AnalysisCostRecordCromwellSubworkflow(SMBase):
    """Cost of a Cromwell subworkflow"""

    cromwell_sub_workflow_name: str
    cost: float
    usage_start_time: datetime.datetime
    usage_end_time: datetime.datetime
    skus: list[AnalysisCostRecordSku]


class AnalysisCostRecordCromwellWorkflow(SMBase):
    """Cost of a Cromwell workflow"""

    cromwell_workflow_id: str
    cost: float
    usage_start_time: datetime.datetime
    usage_end_time: datetime.datetime
    skus: list[AnalysisCostRecordSku]


class AnalysisCostRecord(SMBase):
    """Return class for the Billing Cost by batch_id/ar_guid"""

    total: AnalysisCostRecordTotal | None
    topics: list[AnalysisCostRecordTopic] | None
    categories: list[AnalysisCostRecordCategory] | None
    batches: list[AnalysisCostRecordBatch] | None
    skus: list[AnalysisCostRecordSku] | None
    seq_groups: list[AnalysisCostRecordSeqGroup] | None

    wdl_tasks: list[AnalysisCostRecordWdlTask] | None
    cromwell_sub_workflows: list[AnalysisCostRecordCromwellSubworkflow] | None
    cromwell_workflows: list[AnalysisCostRecordCromwellWorkflow] | None
    dataproc: list[dict] | None

    @classmethod
    def from_dict(cls, d: dict):
        """Create BillingBatchCostRecord from json"""

        return AnalysisCostRecord(
            total=AnalysisCostRecordTotal.from_dict(d['total']),
            topics=[
                AnalysisCostRecordTopic.from_dict(row) for row in d.get('topics') or []
            ],
            categories=[
                AnalysisCostRecordCategory.from_dict(row)
                for row in d.get('categories') or []
            ],
            batches=[
                AnalysisCostRecordBatch.from_dict(row) for row in d.get('batches') or []
            ],
            skus=[AnalysisCostRecordSku.from_dict(row) for row in d.get('skus') or []],
            seq_groups=[
                AnalysisCostRecordSeqGroup.from_dict(row)
                for row in d.get('seq_groups') or []
            ],
            wdl_tasks=[
                AnalysisCostRecordWdlTask.from_dict(row)
                for row in d.get('wdl_tasks') or []
            ],
            cromwell_sub_workflows=[
                AnalysisCostRecordCromwellSubworkflow.from_dict(row)
                for row in d.get('cromwell_sub_workflows') or []
            ],
            cromwell_workflows=[
                AnalysisCostRecordCromwellWorkflow.from_dict(row)
                for row in d.get('cromwell_workflows') or []
            ],
            dataproc=d.get('dataproc'),
        )
