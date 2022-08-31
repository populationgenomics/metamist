from datetime import datetime, date


def strtodate(d: str | None) -> date | None:
    """Convert date string to date, allow for None"""
    if d:
        try:
            return datetime.strptime(d, '%Y-%m-%d').date()
        except Exception as excep:
            raise ValueError(f'Date could not be converted: {d}') from excep

    return None
