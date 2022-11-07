"""Importing GCP libraries"""
from collections import defaultdict
from typing import TypeVar, Callable, Iterable
from .openapi import get_openapi_schema_func
from .db import (
    authenticate,
    get_project_readonly_connection,
    get_project_write_connection,
    get_projectless_db_connection,
)

T = TypeVar('T')
X = TypeVar('X')


def group_by(iterable: Iterable[T], selector: Callable[[T], X]) -> dict[X, list[T]]:
    """Simple group by implementation"""
    ret = defaultdict(list)
    for k in iterable:
        ret[selector(k)].append(k)

    return dict(ret)
