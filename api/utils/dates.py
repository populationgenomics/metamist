from datetime import datetime, date, timedelta

INVOICE_DAY_DIFF = 3


def parse_date_only_string(d: str | None) -> date | None:
    """Convert date string to date, allow for None"""
    if not d:
        return None

    try:
        return datetime.strptime(d, '%Y-%m-%d').date()
    except Exception as excep:
        raise ValueError(f'Date could not be converted: {d}') from excep


def get_invoice_month_range(convert_month: date) -> tuple[date, date]:
    """
    Get the start and end date of the invoice month for a given date

    Specifically it will return the start and end date of the invoice month passed in.
    This enables us to optimise queries where the main date filtering is by
    invoice_month. Of course the partiontioning is not by invoice_month, it's by day.

    So given an invoice_month, return an start and end day to filter on the partition
    to reduce query costs and time.
    """
    first_day = convert_month.replace(day=1)

    # Grab the first day of invoice month then subtract INVOICE_DAY_DIFF days
    start_day = first_day + timedelta(days=-INVOICE_DAY_DIFF)

    if convert_month.month == 12:
        next_month = first_day.replace(month=1, year=convert_month.year + 1)
    else:
        next_month = first_day.replace(month=convert_month.month + 1)

    # Grab the last day of invoice month then add INVOICE_DAY_DIFF days
    last_day = next_month + timedelta(days=-1) + timedelta(days=INVOICE_DAY_DIFF)

    return start_day, last_day
