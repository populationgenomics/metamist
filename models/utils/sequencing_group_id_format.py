import functools
from typing import Iterable

from api.settings import SEQUENCING_GROUP_CHECKSUM_OFFSET, SEQUENCING_GROUP_PREFIX
from models.utils.luhn import luhn_compute, luhn_is_valid


def sequencing_group_id_transform_to_raw_list(
    identifier: Iterable[int | str], strict=True
) -> list[int]:
    """
    Transform LIST of STRING Sequencing Group identifiers (CPGXXXH) to XXX by:
        - validating prefix
        - validating checksum
    """
    return [sequencing_group_id_transform_to_raw(s, strict=strict) for s in identifier]


def sequencing_group_id_transform_to_raw(identifier: int | str, strict=True) -> int:
    """
    Transform STRING Sequencing Group identifier (CPGXXXH) to XXX by:
        - validating prefix
        - validating checksum
    """
    expected_type = str if strict else (str, int)
    if not isinstance(identifier, expected_type):  # type: ignore
        raise TypeError(
            f'Expected sequencing-group identifier type to be {expected_type!r}, '
            f'received {type(identifier)!r}'
        )

    if isinstance(identifier, int):
        return identifier

    if not isinstance(identifier, str):
        raise ValueError('Programming error related to sequencing-group-id checks')

    if not identifier.startswith(SEQUENCING_GROUP_PREFIX):
        raise ValueError(
            f'Invalid prefix found for {SEQUENCING_GROUP_PREFIX} sequencing-group '
            f'identifier {identifier!r}'
        )

    stripped_identifier = identifier.lstrip(SEQUENCING_GROUP_PREFIX)
    if not stripped_identifier.isdigit():
        raise ValueError(f'Invalid sequencing-group identifier {identifier!r}')

    sequencing_group_id_with_checksum = int(stripped_identifier)
    if not luhn_is_valid(
        sequencing_group_id_with_checksum, offset=SEQUENCING_GROUP_CHECKSUM_OFFSET
    ):
        raise ValueError(
            f'The provided sequencing group ID was not valid: {identifier!r}'
        )

    return int(stripped_identifier[:-1])


def sequencing_group_id_format_list(sg_ids: Iterable[int | str]) -> list[str]:
    """
    Transform LIST of raw sequencing-group identifiers to format (CPGXXXH) where:
        - CPG is the prefix
        - XXX is the original identifier
        - H is the Luhn checksum
    """
    return [sequencing_group_id_format(s) for s in sg_ids]


# This potentially gets called a lot in loops when iterating over lots of sgs,
# so maintain a cache.
@functools.lru_cache(maxsize=1024)
def sequencing_group_id_format(sequencing_group_id: int | str) -> str:
    """
    Transform raw (int) sequencing-group identifier to format (CPGXXXH) where:
        - CPG is the prefix
        - XXX is the original identifier
        - H is the Luhn checksum

    >>> sequencing_group_id_format(10)
    'CPG109'

    >>> sequencing_group_id_format(12345)
    'CPG123455'
    """
    if not sequencing_group_id:
        raise ValueError('Cannot format empty sequencing group ID')

    if isinstance(sequencing_group_id, str) and not sequencing_group_id.isdigit():
        if sequencing_group_id.startswith(SEQUENCING_GROUP_PREFIX):
            return sequencing_group_id
        raise ValueError(
            f'Unexpected format for sequencing-group identifier {sequencing_group_id!r}'
        )

    sequencing_group_id = int(sequencing_group_id)

    checksum = luhn_compute(
        sequencing_group_id, offset=SEQUENCING_GROUP_CHECKSUM_OFFSET
    )

    return f'{SEQUENCING_GROUP_PREFIX}{sequencing_group_id}{checksum}'
