def luhn_is_valid(n: int, offset: int = 0):
    """
    Based on: https://stackoverflow.com/a/21079551

    >>> luhn_is_valid(4532015112830366)
    True

    >>> luhn_is_valid(6011514433546201)
    True

    >>> luhn_is_valid(6771549495586802)
    True
    """

    def digits_of(n):
        return [int(d) for d in str(n)]

    digits = digits_of(n)
    odd_digits = digits[-1::-2]
    even_digits = digits[-2::-2]
    checksum = sum(odd_digits) + sum(sum(digits_of(d * 2)) for d in even_digits)
    return checksum % 10 == offset


def luhn_compute(n: int, offset: int = 0):
    """
    Compute Luhn check digit of number given as string

    >>> luhn_compute(453201511283036)
    6

    >>> luhn_compute(601151443354620)
    1

    >>> luhn_compute(677154949558680)
    2
    """
    m = [int(d) for d in reversed(str(n))]
    result = sum(m) + sum(d + (d >= 5) for d in m[::2])
    checksum = ((-result % 10) + offset) % 10
    return checksum
