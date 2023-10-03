from typing import Any

from models.models import BillingRecord

from db.python.gcp_connect import BqDbBase
from db.python.layers.bq_base import BqBaseLayer


class BillingLayer(BqBaseLayer):
    """Billing layer"""

    async def search_records(
        self,
        filter: dict[str, Any] | None = None,
    ) -> list[BillingRecord] | None:
        """
        Get ETL record for the given request_id
        """
        billing_db = BillingDb(self.connection)
        return await billing_db.search_records(filter)


class BillingDb(BqDbBase):
    """Db layer for billing related routes"""

    async def search_records(
        self,
        filter: dict[str, Any] | None = None,
    ) -> list[BillingRecord] | None:
        """Get Billing record from BQ"""

        filter_parts = []

        for k, v in filter.items():
            # value is a tupple of (operator, value)
            op_value = list(v)
            filter_parts.append(f'{k} {op_value[0]} {op_value[1]}')

        filter = ' AND '.join(filter_parts)

        if filter_parts:
            filter = f'WHERE {filter}'

        _query = f"""
        SELECT * 
        FROM `{self._connection.gcp_project}.billing_aggregate.aggregate` 
        {filter}
        LIMIT 10
        """

        query_job_result = list(self._connection.connection.query(_query).result())

        if query_job_result:
            return [BillingRecord.from_json(dict(row)) for row in query_job_result]

        raise ValueError('No record found')
