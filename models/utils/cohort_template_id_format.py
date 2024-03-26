from typing import Iterable

from api.settings import COHORT_TEMPLATE_CHECKSUM_OFFSET, COHORT_TEMPLATE_PREFIX
from models.utils.luhn import luhn_compute, luhn_is_valid


def cohort_template_id_format(cohort_template_id: int | str) -> str:
    """
    Transform raw (int) cohort template identifier to format (CTPLXXX) where:
        - CTPL is the prefix
        - XXX is the original identifier
        - H is the Luhn checksum
    """
    # Validate input
    if isinstance(cohort_template_id, str) and not cohort_template_id.isdigit():
        if cohort_template_id.startswith(COHORT_TEMPLATE_PREFIX):
            return cohort_template_id
        raise ValueError(f'Unexpected format for cohort template identifier {cohort_template_id!r}')

    cohort_template_id = int(cohort_template_id)

    checksum = luhn_compute(
        cohort_template_id, offset=COHORT_TEMPLATE_CHECKSUM_OFFSET
    )

    return f'{COHORT_TEMPLATE_PREFIX}{cohort_template_id}{checksum}'


def cohort_template_id_format_list(cohort_template_ids: Iterable[int | str]) -> list[str]:
    """
    Transform LIST of raw (int) cohort template identifier to format (CTPLXXX) where:
        - CTPL is the prefix
        - XXX is the original identifier
        - H is the Luhn checksum
    """
    return [cohort_template_id_format(s) for s in cohort_template_ids]


def cohort_template_id_transform_to_raw(cohort_template_id: int | str) -> int:
    """
    Transform STRING cohort template identifier (CTPLXXXH) to XXX by:
        - validating prefix
        - validating checksum
    """
    expected_type = str
    if not isinstance(cohort_template_id, expected_type):  # type: ignore
        raise TypeError(
            f'Expected identifier type to be {expected_type!r}, received {type(cohort_template_id)!r}'
        )

    if isinstance(cohort_template_id, int):
        return cohort_template_id

    if not isinstance(cohort_template_id, str):
        raise ValueError('Programming error related to cohort template checks')

    if not cohort_template_id.startswith(COHORT_TEMPLATE_PREFIX):
        raise ValueError(
            f'Invalid prefix found for {COHORT_TEMPLATE_PREFIX} cohort template identifier {cohort_template_id!r}'
        )

    stripped_identifier = cohort_template_id[len(COHORT_TEMPLATE_PREFIX):]

    if not stripped_identifier.isdigit():
        raise ValueError(f'Invalid identifier found for cohort template identifier {cohort_template_id!r}')

    cohort_template_id = int(stripped_identifier)

    if not luhn_is_valid(cohort_template_id, offset=COHORT_TEMPLATE_CHECKSUM_OFFSET):
        raise ValueError(f'Invalid checksum found for cohort template identifier {cohort_template_id!r}')

    return int(stripped_identifier[:-1])


def cohort_template_id_transform_list_to_raw(cohort_template_ids: Iterable[int | str]) -> list[int]:
    """
    Transform LIST of STRING cohort template identifier (CTPLXXXH) to XXX by:
        - validating prefix
        - validating checksum
    """
    return [cohort_template_id_transform_to_raw(s) for s in cohort_template_ids]
