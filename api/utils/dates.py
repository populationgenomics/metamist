from datetime import date, datetime, timedelta

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
    Start and end date are used mostly for optimising BQ queries
    All our BQ tables/views are partitioned by day
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


def reformat_datetime(
    in_date: str | None, in_format: str, out_format: str
) -> str | None:
    """
    Reformat datetime as string to another string format
    This function take string as input and return string as output
    """
    if not in_date:
        return None

    try:
        result = datetime.strptime(in_date, in_format)
        return result.strftime(out_format)

    except Exception as excep:
        raise ValueError(f'Date could not be converted: {in_date}') from excep
