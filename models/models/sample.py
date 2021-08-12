import json
from typing import Optional, Dict, Union, List

from pydantic import BaseModel

from models.enums.sample import SampleType


class Sample(BaseModel):
    """Model for a Sample"""

    id: Union[str] = None
    external_id: str = None
    participant_id: Optional[str] = None
    active: Optional[bool] = None
    meta: Optional[Dict] = None
    type: Optional[SampleType] = None

    @staticmethod
    def from_db(**kwargs):
        """
        Convert from db keys, mainly converting id to id_
        """
        _id = sample_id_format(kwargs.pop('id', None))
        type_ = kwargs.pop('type', None)
        meta = kwargs.pop('meta', None)
        active = kwargs.pop('active', None)
        if active is not None:
            active = bool(active)
        if meta:
            if isinstance(meta, bytes):
                meta = meta.decode()
            if isinstance(meta, str):
                meta = json.loads(meta)

        return Sample(
            id=_id, type=SampleType(type_), meta=meta, active=active, **kwargs
        )


def sample_id_transform_to_raw(
    identifier: Union[List[Union[str, int]], Union[str, int]], strict=True
) -> Union[int, List[int]]:
    """
    Transform STRING sample identifier (CPGXXXH) to XXX by:
        - validating prefix
        - validating checksum
    """
    if isinstance(identifier, list):
        return [sample_id_transform_to_raw(s) for s in identifier]

    if strict and not (isinstance(identifier, str) and identifier.startswith('CPG')):
        raise Exception(
            f'Invalid prefix found for CPG sample identifier "{identifier}"'
        )
    if isinstance(identifier, int):
        return identifier

    if not identifier.startswith('CPG'):
        raise Exception(
            f'Invalid prefix found for CPG sample identifier "{identifier}"'
        )

    stripped_identifier = identifier.lstrip('CPG')
    if not stripped_identifier.isdigit():
        raise ValueError(f'Invalid sample identifier "{identifier}"')

    sample_id_with_checksum = int(stripped_identifier)
    if not luhn_is_valid(sample_id_with_checksum):
        raise ValueError(f'The provided sample ID was not valid: "{identifier}"')

    return int(stripped_identifier[:-1])


def sample_id_format(sample_id: Union[int, List[int]]):
    """
    Transform raw (int) sample identifier to format (CPGXXXH) where:
        - CPG is the prefix
        - H is the Luhn checksum
        - XXX is the original identifier

    >>> sample_id_format(10)
    'CPG109'

    >>> sample_id_format(12345)
    'CPG123455'
    """

    if isinstance(sample_id, list):
        return [sample_id_format(s) for s in sample_id]

    if isinstance(sample_id, str) and not sample_id.isdigit():
        if sample_id.startswith('CPG'):
            return sample_id
        raise ValueError(f'Unexpected format for sample identifier "{sample_id}"')
    sample_id = int(sample_id)

    return f'CPG{sample_id}{luhn_compute(sample_id)}'


def luhn_is_valid(n):
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
    return checksum % 10 == 0


def luhn_compute(n):
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
    return -result % 10
