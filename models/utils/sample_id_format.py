import functools
from typing import Iterable

from api.settings import SAMPLE_CHECKSUM_OFFSET, SAMPLE_PREFIX
from models.utils.luhn import luhn_compute, luhn_is_valid


def sample_id_transform_to_raw_list(
    identifier: Iterable[int | str], strict=True
) -> list[int]:
    """
    Transform LIST of STRING sample identifier (CPGXXXH) to XXX by:
        - validating prefix
        - validating checksum
    """
    return [sample_id_transform_to_raw(s, strict=strict) for s in identifier]


def sample_id_transform_to_raw(identifier: int | str, strict=True) -> int:
    """
    Transform STRING sample identifier (CPGXXXH) to XXX by:
        - validating prefix
        - validating checksum
    """
    expected_type = str if strict else (str, int)
    if not isinstance(identifier, expected_type):  # type: ignore
        raise TypeError(
            f'Expected identifier type to be {expected_type!r}, received {type(identifier)!r}'
        )

    if isinstance(identifier, int):
        return identifier

    if not isinstance(identifier, str):
        raise ValueError('Programming error related to sample checks')

    if not identifier.startswith(SAMPLE_PREFIX):
        raise ValueError(
            f'Invalid prefix found for {SAMPLE_PREFIX} sample identifier {identifier!r}'
        )

    stripped_identifier = identifier.lstrip(SAMPLE_PREFIX)
    if not stripped_identifier.isdigit():
        raise ValueError(f'Invalid sample identifier {identifier!r}')

    sample_id_with_checksum = int(stripped_identifier)
    if not luhn_is_valid(sample_id_with_checksum, offset=SAMPLE_CHECKSUM_OFFSET):
        raise ValueError(f'The provided sample ID was not valid: {identifier!r}')

    return int(stripped_identifier[:-1])


def sample_id_format_list(sample_ids: Iterable[int | str]) -> list[str]:
    """
    Transform LIST of raw (int) sample identifier to format (CPGXXXH) where:
        - CPG is the prefix
        - XXX is the original identifier
        - H is the Luhn checksum
    """
    return [sample_id_format(s) for s in sample_ids]


# This potentially gets called a lot in loops when iterating over lots of samples,
# so maintain a cache.
@functools.lru_cache(maxsize=1024)
def sample_id_format(sample_id: int | str) -> str:
    """
    Transform raw (int) sample identifier to format (CPGXXXH) where:
        - XPG is the prefix
        - XXX is the original identifier
        - H is the Luhn checksum

    >>> sample_id_format(10)
    'XPGLCL109'

    >>> sample_id_format(12345)
    'XPGLCL123455'
    """

    if isinstance(sample_id, str) and not sample_id.isdigit():
        if sample_id.startswith(SAMPLE_PREFIX):
            return sample_id
        raise ValueError(f'Unexpected format for sample identifier {sample_id!r}')

    sample_id = int(sample_id)

    checksum = luhn_compute(sample_id, offset=SAMPLE_CHECKSUM_OFFSET)

    return f'{SAMPLE_PREFIX}{sample_id}{checksum}'
