import dataclasses
from collections.abc import Callable, Sequence
from enum import Enum
from string.templatelib import Template
from typing import Any, TypeVar

from psycopg import sql

from db.python.utils import escape_like_term
from models.base import SMBase


T = TypeVar('T')
X = TypeVar('X')


def get_hashable_value(value):  # noqa: PLR0911
    """Prepare a value that can be hashed, for use in a dict or set"""
    if value is None:
        return None
    if isinstance(value, (int, str, float, bool)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (tuple, list)):
        # let's see if later we need to prepare the values in the list
        return tuple(get_hashable_value(v) for v in value)
    if isinstance(value, dict):
        return tuple(
            sorted(
                ((k, get_hashable_value(v)) for k, v in value.items()),
                key=lambda x: x[0],
            )
        )
    if hasattr(value, 'get_hashable_value'):
        return value.get_hashable_value()

    return hash(value)


class GenericFilter[T](SMBase):
    """
    Generic filter for eq, in_ (in) and nin (not in)
    """

    eq: T | None = None
    neq: T | None = None
    in_: Sequence[T] | None = None
    nin: Sequence[T] | None = None
    gt: T | None = None
    gte: T | None = None
    lt: T | None = None
    lte: T | None = None
    contains: T | None = None
    icontains: T | None = None
    startswith: T | None = None
    isnull: bool | None = None

    def __repr__(self):
        keys = [
            'eq',
            'neq',
            'in_',
            'nin',
            'gt',
            'gte',
            'lt',
            'lte',
            'contains',
            'icontains',
            'startswith',
            'isnull',
        ]
        inner_values = ', '.join(
            f'{k}={getattr(self, k)!r}' for k in keys if getattr(self, k) is not None
        )
        return f'{self.__class__.__name__}({inner_values})'

    def get_hashable_value(self):
        """Get value that we could run hash on"""
        return get_hashable_value(
            (
                self.__class__.__name__,
                self.eq,
                self.neq,
                tuple(self.in_) if self.in_ is not None else None,
                tuple(self.nin) if self.nin is not None else None,
                self.gt,
                self.gte,
                self.lt,
                self.lte,
                self.contains,
                self.icontains,
                self.startswith,
                self.isnull,
            )
        )

    def __hash__(self):
        """Override to ensure we can hash this object"""
        return hash(self.get_hashable_value())

    def to_sql(
        self, column: str, column_expression: Template | None = None
    ) -> Template:
        """
        Convert to SQL, and avoid SQL injection

        Args:
            column (str): The expression, or column name that derives the values
            column_expression (Template, optional): A SQL expression as a str Template
                to be used for the column. This can be used if you want to use a sql
                function for the column

        Returns:
            Template
        """
        filters: list[Template] = []

        if not isinstance(column, str):
            raise ValueError(f'Column {column!r} must be a string')

        column_query = (
            column_expression
            if column_expression
            else t'{sql.Identifier(*column.split(".")):i}'
        )

        if self.eq is not None:
            filters.append(t'{column_query:q} = {self.eq}')
        if self.neq is not None:
            filters.append(t'{column_query:q} != {self.neq}')
        if self.in_ is not None:
            if not isinstance(self.in_, list):
                raise ValueError('IN filter must be a list')

            # in an empty list is always false
            if len(self.in_) == 0:
                return t'FALSE'

            filters.append(t'{column_query:q} = ANY({self.in_})')

        if self.nin is not None and len(self.nin) > 0:
            if not isinstance(self.nin, list):
                raise ValueError('NIN filter must be a list')

            # Include NULLs here as the user would expect to recieve nulls in the
            # results when they are trying to exclude certain values
            filters.append(
                t'({column_query:q} IS NULL OR NOT ({column_query:q} = ANY({self.nin})))'
            )
        if self.gt is not None:
            filters.append(t'{column_query:q} > {self.gt}')
        if self.gte is not None:
            filters.append(t'{column_query:q} >= {self.gte}')
        if self.lt is not None:
            filters.append(t'{column_query:q} < {self.lt}')
        if self.lte is not None:
            filters.append(t'{column_query:q} <= {self.lte}')
        if self.contains is not None:
            search_term = escape_like_term(str(self.contains))
            filters.append(t"{column_query:q} LIKE '%' || {search_term} || '%'")
        if self.icontains is not None:
            search_term = escape_like_term(str(self.icontains))
            filters.append(t"{column_query:q} ILIKE '%' || {search_term} || '%'")
        if self.startswith is not None:
            search_term = escape_like_term(str(self.startswith))
            filters.append(t"{column_query:q} LIKE {search_term} || '%'")
        if self.isnull is not None:
            if self.isnull:
                filters.append(t'{column_query:q} IS NULL')
            else:
                filters.append(t'{column_query:q} IS NOT NULL')

        if len(filters) == 0:
            return t''

        return sql.SQL(' AND ').join(filters)

    def transform(self, func: Callable[[T], X]) -> GenericFilter[X]:
        """
        Apply a function to each value in the filter
        """
        return GenericFilter(
            eq=func(self.eq) if self.eq else None,
            neq=func(self.neq) if self.neq else None,
            in_=list(map(func, self.in_)) if self.in_ else None,
            nin=list(map(func, self.nin)) if self.nin else None,
            gt=func(self.gt) if self.gt else None,
            gte=func(self.gte) if self.gte else None,
            lt=func(self.lt) if self.lt else None,
            lte=func(self.lte) if self.lte else None,
            contains=func(self.contains) if self.contains else None,
            icontains=func(self.icontains) if self.icontains else None,
            startswith=func(self.startswith) if self.startswith else None,
            isnull=self.isnull,
        )


GenericMetaFilter = dict[str, GenericFilter[Any]]


@dataclasses.dataclass(kw_only=True)
class GenericFilterModel:
    """
    Class that contains fields of GenericFilters that can be used to filter
    """

    def __hash__(self):
        """Hash the GenericFilterModel, this doesn't override well"""
        return hash(self.get_hashable_value())

    def get_hashable_value(self):
        """Get value that we could run hash on"""
        return get_hashable_value((self.__class__.__name__, *dataclasses.astuple(self)))

    def __post_init__(self):
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if value is None:
                continue

            if isinstance(value, tuple) and len(value) == 1 and value[0] is None:
                raise ValueError(
                    f'There is very likely a trailing comma on the end of '
                    f'{self.__class__.__name__}.{field.name}. If you actually want a '
                    f'tuple of length one with the value = (None,), then use '
                    f'dataclasses.field(default_factory=lambda: (None,))'
                )
            if isinstance(value, GenericFilter):
                continue

            if isinstance(value, GenericFilterModel):
                # allow nested GenericFilterModels
                continue

            if isinstance(value, dict):
                # make sure each field is a GenericFilter, or set it to be one,
                # in this case it's always 'eq', never automatically in_
                new_value = {
                    k: v if isinstance(v, GenericFilter) else GenericFilter(eq=v)
                    for k, v in value.items()
                }
                setattr(self, field.name, new_value)
                continue

            # lazily provided a value, which we'll correct
            if isinstance(value, list):
                setattr(self, field.name, GenericFilter(in_=value))
            else:
                setattr(self, field.name, GenericFilter(eq=value))

    def to_sql(
        self,
        field_overrides: dict[str, str] | None = None,
        only: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> Template | None:
        """Convert the model to SQL, and avoid SQL injection"""

        _foverrides = field_overrides or {}

        # check for bad field_overrides
        bad_field_overrides = set(_foverrides.keys()) - set(
            f.name for f in dataclasses.fields(self)
        )
        if bad_field_overrides:
            raise ValueError(
                f'Specified field overrides that were not used: {bad_field_overrides}'
            )

        fields = dataclasses.fields(self)
        filters: list[Template] = []
        for field in fields:
            if only and field.name not in only:
                continue
            if exclude and field.name in exclude:
                continue

            fcolumn = _foverrides.get(field.name, field.name)
            if filter_ := getattr(self, field.name):
                if isinstance(filter_, dict):
                    filters.append(
                        prepare_query_from_dict_field(
                            filter_=filter_, field_name=field.name, column_name=fcolumn
                        )
                    )
                elif isinstance(filter_, GenericFilter):
                    filters.append(filter_.to_sql(fcolumn))
                else:
                    raise ValueError(
                        f'Filter {field.name} must be a GenericFilter or dict[str, GenericFilter]'
                    )

        if len(filters) == 0:
            return None

        return sql.SQL(' AND ').join(filters)


def prepare_query_from_dict_field(
    filter_: dict[str, Any],  # noqa: ARG001
    field_name: str,  # noqa: ARG001
    column_name: str,  # noqa: ARG001
) -> Template:
    """
    Prepare a SQL query from a dict field, which is a dict of GenericFilters.
    Usually this is a JSON field in the database that we want to query on.

    """
    # @TODO implement this, it's a bit tricky as postgres is much more strict with JSON
    # types.
    raise NotImplementedError('Querying JSON keys is not implemented at the moment')
    return t'FALSE'
