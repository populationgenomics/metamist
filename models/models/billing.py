import datetime
from decimal import Decimal

from pydantic.tools import parse_obj_as
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
            topic=GenericFilter(eq='","'.join(self.topic)) if self.topic else None,
            date=GenericFilter(eq=self.date),
            cost_category=GenericFilter(eq='","'.join(self.cost_category))
            if self.cost_category
            else None,
        )


class BillingTopicCostCategoryRecord(SMBase):
    """Return class for the Billing record"""

    day: datetime.date
    topic: str
    cost_category: str | None
    cost: Decimal
    currency: str

    @staticmethod
    def from_json(record):
        """Create BillingTopicCostCategoryRecord from json"""
        return parse_obj_as(BillingTopicCostCategoryRecord, record)


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
