from typing import Iterable

from api.settings import COHORT_CHECKSUM_OFFSET, COHORT_PREFIX
from models.utils.luhn import luhn_compute, luhn_is_valid


def cohort_id_format(cohort_id: int | str) -> str:
    """
    Transform raw (int) cohort identifier to format (COHXXX) where:
        - COH is the prefix
        - XXX is the original identifier
        - H is the Luhn checksum
    """
    # Validate input
    if isinstance(cohort_id, str) and not cohort_id.isdigit():
        if cohort_id.startswith(COHORT_PREFIX):
            return cohort_id
        raise ValueError(f'Unexpected format for cohort identifier {cohort_id!r}')

    cohort_id = int(cohort_id)

    checksum = luhn_compute(cohort_id, offset=COHORT_CHECKSUM_OFFSET)

    return f'{COHORT_PREFIX}{cohort_id}{checksum}'


def cohort_id_format_list(cohort_ids: Iterable[int | str]) -> list[str]:
    """
    Transform LIST of raw (int) cohort identifier to format (COHXXX) where:
        - COH is the prefix
        - XXX is the original identifier
        - H is the Luhn checksum
    """
    return [cohort_id_format(s) for s in cohort_ids]


def cohort_id_transform_to_raw(cohort_id: int | str, strict=True) -> int:
    """
    Transform STRING cohort identifier (COHXXXH) to XXX by:
        - validating prefix
        - validating checksum
    """
    expected_type = str if strict else (str, int)
    if not isinstance(cohort_id, expected_type):  # type: ignore
        raise TypeError(
            f'Expected identifier type to be {expected_type!r}, received {type(cohort_id)!r}'
        )

    if isinstance(cohort_id, int):
        return cohort_id

    if not cohort_id.startswith(COHORT_PREFIX):
        raise ValueError(
            f'Invalid prefix found for {COHORT_PREFIX} cohort identifier {cohort_id!r}'
        )

    stripped_identifier = cohort_id.lstrip(COHORT_PREFIX)
    if not stripped_identifier.isdigit():
        raise ValueError(f'Invalid cohort identifier {cohort_id!r}')

    cohort_id_with_checksum = int(stripped_identifier)
    if not luhn_is_valid(cohort_id_with_checksum, offset=COHORT_CHECKSUM_OFFSET):
        raise ValueError(f'The provided cohort ID was not valid: {cohort_id!r}')

    return int(stripped_identifier[:-1])


def cohort_id_transform_to_raw_list(
    identifier: Iterable[int | str], strict=True
) -> list[int]:
    """
    Transform LIST of STRING cohort identifier (COHXXXH) to XXX by:
        - validating prefix
        - validating checksum
    """
    return [cohort_id_transform_to_raw(s, strict=strict) for s in identifier]
