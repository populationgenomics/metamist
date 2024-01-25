from enum import Enum


class BillingSource(str, Enum):
    """List of billing sources"""

    RAW = 'raw'
    AGGREGATE = 'aggregate'
    EXTENDED = 'extended'
    BUDGET = 'budget'
    GCP_BILLING = 'gcp_billing'
    BATCHES = 'batches'


class BillingTimePeriods(str, Enum):
    """List of billing grouping time periods"""

    # grouping time periods
    DAY = 'day'
    WEEK = 'week'
    MONTH = 'month'
    INVOICE_MONTH = 'invoice_month'


class BillingTimeColumn(str, Enum):
    """List of billing time columns"""

    DAY = 'day'
    USAGE_START_TIME = 'usage_start_time'
    USAGE_END_TIME = 'usage_end_time'
    EXPORT_TIME = 'export_time'
