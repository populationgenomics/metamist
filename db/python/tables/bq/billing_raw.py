from datetime import datetime

from api.settings import BQ_AGGREG_RAW
from db.python.tables.bq.billing_base import BillingBaseTable
from db.python.tables.bq.billing_filter import BillingFilter
from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from models.models import BillingTotalCostQueryModel


class BillingRawTable(BillingBaseTable):
    """Billing Raw (Consolidated) Biq Query table"""

    table_name = BQ_AGGREG_RAW

    def get_table_name(self):
        """Get table name"""
        return self.table_name

    @staticmethod
    def _query_to_partitioned_filter(
        query: BillingTotalCostQueryModel,
    ) -> BillingFilter:
        """
        Raw BQ billing table is partitioned by usage_end_time
        """
        billing_filter = query.to_filter()

        # initial partition filter
        billing_filter.usage_end_time = GenericBQFilter[datetime](
            gte=datetime.strptime(query.start_date, '%Y-%m-%d')
            if query.start_date
            else None,
            lte=datetime.strptime(query.end_date, '%Y-%m-%d')
            if query.end_date
            else None,
        )
        return billing_filter
