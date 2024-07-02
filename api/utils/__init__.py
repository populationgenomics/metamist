"""Importing GCP libraries"""

from collections import defaultdict
from typing import Callable, Iterable, TypeVar

T = TypeVar('T')
X = TypeVar('X')


def group_by(iterable: Iterable[T], selector: Callable[[T], X]) -> dict[X, list[T]]:
    """Simple group by implementation"""
    ret = defaultdict(list)
    for k in iterable:
        ret[selector(k)].append(k)

    return dict(ret)
