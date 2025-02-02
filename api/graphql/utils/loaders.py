import copy
from typing import Any

from api.utils import group_by
from db.python.connect import Connection
from db.python.filters.generic import get_hashable_value

loaders: dict[str, Any] = {}


def connected_data_loader(id_: str, cache: bool = True):
    """Provide connection to a data loader"""

    def connected_data_loader_caller(fn):
        def inner(connection: Connection):
            async def wrapped(*args, **kwargs):
                return await fn(*args, **kwargs, connection=connection)

            return wrapped

        loaders[id_] = inner
        return inner

    return connected_data_loader_caller


def _get_connected_data_loader_partial_key(kwargs) -> tuple:
    return get_hashable_value({k: v for k, v in kwargs.items() if k != 'id'})  # type: ignore


def connected_data_loader_with_params(id_: str, default_factory=None, copy_args=True):
    """
    DataLoader Decorator for allowing DB connection to be bound to a loader
    """

    def connected_data_loader_caller(fn):
        def inner(connection: Connection):
            async def wrapped(query: list[dict[str, Any]]) -> list[Any]:
                by_key: dict[tuple, Any] = {}

                if any('connection' in q for q in query):
                    raise ValueError('Cannot pass connection in query')
                if any('id' not in q for q in query):
                    raise ValueError('Must pass id in query')

                # group by all last fields (except the first which is always ID
                grouped = group_by(query, _get_connected_data_loader_partial_key)
                for extra_args, chunk in grouped.items():
                    # ie: matrix transform
                    ids = [row['id'] for row in chunk]
                    kwargs = {
                        k: copy.copy(v) if copy_args else v
                        for k, v in chunk[0].items()
                        if k != 'id'
                    }
                    value_map = await fn(connection=connection, ids=ids, **kwargs)
                    if not isinstance(value_map, dict):
                        raise ValueError(
                            f'Expected dict from {fn.__name__}, got {type(value_map)}'
                        )
                    for returned_id, value in value_map.items():
                        by_key[(returned_id, *extra_args)] = value

                return [
                    by_key.get(
                        (q['id'], *_get_connected_data_loader_partial_key(q)),
                        default_factory() if default_factory else None,
                    )
                    for q in query
                ]

            return wrapped

        loaders[id_] = inner
        return inner

    return connected_data_loader_caller
