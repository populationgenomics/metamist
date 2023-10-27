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
    first_day = convert_month.replace(day=1)

    # Grab the first day of invoice month then subtract INVOICE_DAY_DIFF days
    start_day = first_day + timedelta(days=-INVOICE_DAY_DIFF)

    # Grab the last day of invoice month then add INVOICE_DAY_DIFF days
    current_day = (
        first_day.replace(month=(convert_month.month + 1) % 12)
        + timedelta(days=-1)
        + timedelta(days=INVOICE_DAY_DIFF)
    )

    return start_day, current_day
