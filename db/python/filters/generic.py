import dataclasses
import datetime
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

    def to_sql(self, column: str | Template) -> Template | None:
        """
        Convert to SQL, and avoid SQL injection.

        **Note**:
            If column contains an expression, it _must_ be of type Template.
            If column is a column name, it should be of type str.

        Args:
            column (str | Template): The expression, or column name that derives the values.

        Returns:
            Template
        """
        filters: list[Template | None] = []

        # MARIADB BACKWARDS COMPATIBILITY:
        # Goal: Make all string comparisions case insensitive
        # We are going to do a check on all of the filter fields
        # if their type is str, make sure that the value is set to
        # CASEFOLD(value) and the column query is also wrapped in CASEFOLD
        pg_type = self.infer_pg_type_from_filter()

        def wrap_val(val: T | str):
            if pg_type == 'text':
                return t'CASEFOLD({val})'
            return t'{val}'

        def wrap_val_list(val_list: list[T]):
            if pg_type == 'text':
                return t'(SELECT CASEFOLD(val) FROM unnest({val_list}::text[]) AS val)'
            return t'{val_list}'

        original_column_query = (
            column
            if isinstance(column, Template)
            else t'{sql.Identifier(*column.split(".")):i}'
        )

        # Add CASEFOLD to the column query if any of the fields are strings
        column_query = original_column_query
        if pg_type == 'text':
            column_query = t'CASEFOLD({original_column_query:q})'

        if self.eq is not None:
            filters.append(t'{column_query:q} = {wrap_val(self.eq):q}')
        if self.neq is not None:
            filters.append(t'{column_query:q} IS DISTINCT FROM {wrap_val(self.neq):q}')
        if self.in_ is not None:
            if not isinstance(self.in_, list):
                raise ValueError('IN filter must be a list')

            # in an empty list is always false
            if len(self.in_) == 0:
                return t'FALSE'

            filters.append(t'{column_query:q} = ANY({wrap_val_list(self.in_):q})')

        if self.nin is not None and len(self.nin) > 0:
            if not isinstance(self.nin, list):
                raise ValueError('NIN filter must be a list')

            # Include NULLs here as the user would expect to recieve nulls in the
            # results when they are trying to exclude certain values
            filters.append(
                t'({column_query:q} IS NULL OR NOT ({column_query:q} = ANY({wrap_val_list(self.nin):q})))'
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
            search_term = wrap_val(escape_like_term(str(self.contains)))
            filters.append(
                t"{original_column_query:q} ILIKE '%' || {search_term:q} || '%'"
            )
        if self.icontains is not None:
            search_term = wrap_val(escape_like_term(str(self.icontains)))
            filters.append(
                t"{original_column_query:q} ILIKE '%' || {search_term:q} || '%'"
            )
        if self.startswith is not None:
            search_term = wrap_val(escape_like_term(str(self.startswith)))
            filters.append(t"{original_column_query:q} ILIKE {search_term:q} || '%'")
        if self.isnull is not None:
            if self.isnull:
                filters.append(t'{column_query:q} IS NULL')
            else:
                filters.append(t'{column_query:q} IS NOT NULL')

        filters_rm_none: list[Template] = [f for f in filters if f is not None]
        if len(filters_rm_none) == 0:
            return None

        return sql.SQL(' AND ').join(filters_rm_none)

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

    def infer_pg_type_from_filter(self) -> str | None:
        """
        Infer the SQL cast type from actual values in the filter

        In order for these values to work you could just check the
        type of the first non-None value, but to be thorough we will enforce
        that all values in the filter (excluding isnull) are of the same type.

        This does require trust in the user to apply an appropriate filter
        for the column key value type e.g. for the json column "meta" and a row value {'favnum': int}
        We hope that a filter would be appropriately applied like
        meta={'favnum': GenericFilter(eq=5)} <- we'll infer the type as integer from the value 5
        and not
        meta={'favnum': GenericFilter(eq='five')} <- we'll infer the type as text

        If the casting of any value fails, the database will raise an error.

        If the type cannot be inferred, None is returned
        """
        # Collect all non-None comparison values
        sample_values: list[Any] = []
        for k, v in self.__dict__.items():
            if k == 'isnull' or v is None:
                continue
            if isinstance(v, list):
                sample_values.extend(v)
            else:
                sample_values.append(v)

        # Infer type non-None values
        cast_types: set[str] = set()
        for v in sample_values:
            if isinstance(v, bool):
                cast_types.add('bool')
            elif isinstance(v, (int, float)):
                cast_types.add('numeric')
            elif isinstance(v, Enum):
                cast_types.add('enum')
            elif isinstance(v, (datetime.datetime, datetime.date)):
                cast_types.add('timestamp')
            elif isinstance(v, str):
                cast_types.add('text')

        if len(cast_types) > 1:
            raise ValueError(
                f'Mixed types in filter values: {cast_types}. All values in a filter must be of the same type.'
            )

        return cast_types.pop() if cast_types else None


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
        filters: list[Template | None] = []
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
                            filter_=filter_, column_name=fcolumn
                        )
                    )
                elif isinstance(filter_, GenericFilter):
                    filters.append(filter_.to_sql(fcolumn))
                else:
                    raise ValueError(
                        f'Filter {field.name} must be a GenericFilter or dict[str, GenericFilter]'
                    )

        filters_rm_none: list[Template] = [f for f in filters if f is not None]

        if len(filters_rm_none) == 0:
            return None

        return sql.SQL(' AND ').join(filters_rm_none)


def prepare_query_from_dict_field(
    filter_: dict[str, Any],
    column_name: str | Template,
) -> Template | None:
    """
    Prepare a SQL query from a dict field, which is a dict of GenericFilters.
    Usually this is a JSON field in the database that we want to query on.

    """

    column_query = (
        column_name
        if isinstance(column_name, Template)
        else t'{sql.Identifier(*column_name.split(".")):i}'
    )

    conditionals: list[Template] = []
    for key, value in filter_.items():
        if not isinstance(value, GenericFilter):
            raise ValueError(f'Filter {column_name} must be a GenericFilter')

        # In the case where an 'isnull' filter is applied and the type
        # cannot be inferred, we will default to text
        pg_type = value.infer_pg_type_from_filter() or 'text'

        if pg_type == 'enum':
            raise ValueError(
                f'Unsupported type "enum" for dict field filter. '
                f'Enum types are not supported in dict field filters because the database cannot infer the correct enum type to cast to.'
            )

        # Validate that the pg_type is supported for dict field queries
        # The `returning {pg_type}` clause only works with numeric, bool, or text
        supported_types = {'numeric', 'bool', 'text'}
        if pg_type not in supported_types:
            raise ValueError(
                f'Unsupported type {pg_type!r} for dict field filter. '
                f'Supported types are: {", ".join(sorted(supported_types))}'
            )

        _inner_query = value.to_sql(t"json_value(a, '$' returning {pg_type:i})")

        if _inner_query:
            conditionals.append(t"""
            exists (
                select 1 FROM (SELECT 1) AS dummy_row -- need at least one row for negation comparisons to work
                LEFT JOIN LATERAL jsonb_path_query({column_query:q}, ('$.' || {key})::jsonpath) a ON TRUE
                where {_inner_query:q}
            )""")
    if not conditionals:
        return None

    return sql.SQL(' AND ').join(conditionals)
