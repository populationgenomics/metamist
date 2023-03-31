import json
import os
from typing import Optional, Dict, Union, List, Sequence, Iterable

from models.base import SMBase
from models.enums.sample import SampleType
from models.models.sequence import SampleSequencing

SAMPLE_PREFIX = os.getenv('SM_SAMPLEPREFIX', 'CPGLCL').upper()
CHECKSUM_OFFSET = int(os.getenv('SM_SAMPLECHECKOFFSET', '2'))


class Sample(SMBase):
    """Model for a Sample"""

    id: Union[str, int]
    external_id: str
    participant_id: Optional[int] = None
    active: Optional[bool] = None
    meta: Optional[Dict] = None
    type: Optional[SampleType] = None
    project: Optional[int] = None
    author: Optional[str] = None

    @staticmethod
    def from_db(d: Dict):
        """
        Convert from db keys, mainly converting id to id_
        """
        _id = sample_id_format(d.pop('id', None))
        type_ = d.pop('type', None)
        meta = d.pop('meta', None)
        active = d.pop('active', None)
        if active is not None:
            active = bool(active)
        if meta:
            if isinstance(meta, bytes):
                meta = meta.decode()
            if isinstance(meta, str):
                meta = json.loads(meta)

        return Sample(id=_id, type=SampleType(type_), meta=meta, active=active, **d)


SampleIdRaw = Union[str, int]


class SampleSequences(Sample):
    """Model for a Sample with list of sequences"""

    sequences: Optional[List[SampleSequencing]] = []


def sample_id_transform_to_raw_list(
    identifier: Sequence[SampleIdRaw], strict=True
) -> List[int]:
    """
    Transform LIST of STRING sample identifier (CPGXXXH) to XXX by:
        - validating prefix
        - validating checksum
    """
    return [sample_id_transform_to_raw(s, strict=strict) for s in identifier]


def sample_id_transform_to_raw(identifier: SampleIdRaw, strict=True) -> int:
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
    if not luhn_is_valid(sample_id_with_checksum, offset=CHECKSUM_OFFSET):
        raise ValueError(f'The provided sample ID was not valid: {identifier!r}')

    return int(stripped_identifier[:-1])


def sample_id_format_list(sample_ids: Iterable[Union[int, str]]) -> List[str]:
    """
    Transform LIST of raw (int) sample identifier to format (CPGXXXH) where:
        - CPG is the prefix
        - XXX is the original identifier
        - H is the Luhn checksum
    """
    return [sample_id_format(s) for s in sample_ids]


def sample_id_format(sample_id: Union[int, str]) -> str:
    """
    Transform raw (int) sample identifier to format (CPGXXXH) where:
        - CPG is the prefix
        - XXX is the original identifier
        - H is the Luhn checksum

    >>> sample_id_format(10)
    'CPG109'

    >>> sample_id_format(12345)
    'CPG123455'
    """

    if isinstance(sample_id, str) and not sample_id.isdigit():
        if sample_id.startswith(SAMPLE_PREFIX):
            return sample_id
        raise ValueError(f'Unexpected format for sample identifier {sample_id!r}')

    sample_id = int(sample_id)

    checksum = luhn_compute(sample_id, offset=CHECKSUM_OFFSET)

    return f'{SAMPLE_PREFIX}{sample_id}{checksum}'


def luhn_is_valid(n, offset=0):
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


def luhn_compute(n, offset=0):
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
