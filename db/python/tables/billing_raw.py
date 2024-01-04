from api.settings import BQ_AGGREG_RAW
from db.python.tables.billing_base import BillingBaseTable


class BillingRawTable(BillingBaseTable):
    """Billing Raw (Consolidated) Biq Query table"""

    table_name = BQ_AGGREG_RAW

    def get_table_name(self):
        """Get table name"""
        return self.table_name
