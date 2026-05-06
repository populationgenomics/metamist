"""Importing GCP libraries"""

from collections import defaultdict
from collections.abc import Callable, Iterable
from typing import TypeVar


T = TypeVar('T')
X = TypeVar('X')


def ensure_nonnone(val: T | None) -> T:
    """Return the argument which is now guaranteed not to be None"""
    assert val is not None
    return val


def group_by(iterable: Iterable[T], selector: Callable[[T], X]) -> dict[X, list[T]]:
    """Simple group by implementation"""
    ret = defaultdict(list)
    for k in iterable:
        ret[selector(k)].append(k)

    return dict(ret)
