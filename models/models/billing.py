import json
from models.base import SMBase


class BillingRecord(SMBase):
    """Return class for the Billing record"""

    id: str
    topic: str
    service_id: str
    service_description: str

    gcp_project_id: str
    gcp_project_name: str

    dataset: str
    batch_id: str
    job_id: str
    batch_name: str

    cost: str
    currency: str
    invoice_month: str
    cost_type: str

    class Config:
        """Config for BillingRecord Response"""

        orm_mode = True

    @staticmethod
    def from_json(record):
        """Create BillingRecord from json"""

        print('\n\n ========================= \n\n')

        print(type(record))
        print(record)

        print('\n\n ========================= \n\n')

        record['service'] = record['service'] if record['service'] else {}
        record['project'] = record['project'] if record['project'] else {}
        record['invoice'] = record['invoice'] if record['invoice'] else {}

        labels = {}

        if record['labels']:
            for lbl in record['labels']:
                labels[lbl['key']] = lbl['value']

        record['labels'] = labels

        return BillingRecord(
            id=record['id'],
            topic=record['topic'],
            service_id=record['service'].get('id', ''),
            service_description=record['service'].get('description', ''),
            gcp_project_id=record['project'].get('id', ''),
            gcp_project_name=record['project'].get('name', ''),
            dataset=record['labels'].get('dataset', ''),
            batch_id=record['labels'].get('batch_id', ''),
            job_id=record['labels'].get('job_id', ''),
            batch_name=record['labels'].get('batch_name', ''),
            cost=record['cost'],
            currency=record['currency'],
            invoice_month=record['invoice'].get('month', ''),
            cost_type=record['cost_type'],
        )
