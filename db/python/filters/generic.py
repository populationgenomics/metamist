import dataclasses
import re
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

        column_query = (
            column
            if isinstance(column, Template)
            else t'{sql.Identifier(*column.split(".")):i}'
        )

        if self.eq is not None:
            filters.append(t'{column_query:q} = {self.eq}')
        if self.neq is not None:
            filters.append(t'{column_query:q} IS DISTINCT FROM {self.neq}')
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

        filters_rm_none: list[Template] = [f for f in filters if f is not None]
        if len(filters_rm_none) == 0:
            return None

        # Join Template objects using the + operator
        if len(filters_rm_none) == 1:
            return filters_rm_none[0]

        result = filters_rm_none[0]
        for f in filters_rm_none[1:]:
            result = result + t' AND ' + f

        return result

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

    def to_jsonpath_exists(self, column_name: str, jsonb_expression: str) -> Template:
        r"""
        Convert a GenericFilter to a JSONPath exists query for JSONB columns with array filtering.

        This is used for filtering on nested JSON arrays, e.g., finding all assays where
        any element in the 'reads' array has a 'size' >= 10000.

        Args:
            column_name: The column name (e.g., 'a.meta' or 'meta')
            jsonb_expression: The JSON path expression (e.g., 'reads[*].basename')

        Returns:
            Template for jsonb_path_exists() query

        Example:
            GenericFilter(gte=10000).to_jsonpath_exists('a.meta', 'reads[*].size')
            # Returns: jsonb_path_exists(a.meta, '$.reads[*] ? (@.size >= 10000)')

            GenericFilter(contains='fastq.gz').to_jsonpath_exists('a.meta', 'reads[*].basename')
            # Returns: jsonb_path_exists(a.meta, '$.reads[*] ? (@.basename like_regex "fastq\.gz")')
        """

        # Parse the jsonb_expression to extract the final field and
        # json path expression.
        # e.g., 'reads[*].basename' -> array_path='reads[*]', field='basename'
        match = re.match(r'^(.+[\[*"\]]*)\.(.+)$', jsonb_expression)
        if not match:
            raise ValueError(f'Invalid JSONB expression: {jsonb_expression}. ')

        array_path = match.group(1)  # 'reads[*]'
        field = match.group(2)  # 'basename'

        # Build JSONPath predicates
        predicates = []
        inferred_type = infer_type_from_filter(self)

        # Quote values in JSONPath if they are strings
        q = '"' if inferred_type == 'string' else ''

        # String processer
        def escape_jsonpath_string(value: str) -> str:
            return re.escape(str(value)).replace('\\', '\\\\')

        if self.eq is not None:
            predicates.append(f'@.{field} == {q}{self.eq}{q}')

        if self.neq is not None:
            predicates.append(f'@.{field} != {q}{self.neq}{q}')

        if self.gt is not None:
            predicates.append(f'@.{field} > {self.gt}')

        if self.gte is not None:
            predicates.append(f'@.{field} >= {self.gte}')

        if self.lt is not None:
            predicates.append(f'@.{field} < {self.lt}')

        if self.lte is not None:
            predicates.append(f'@.{field} <= {self.lte}')

        if self.contains is not None:
            # Escape special regex characters for JSONPath like_regex
            # Double backslashes for PostgreSQL JSONPath parser
            escaped = escape_jsonpath_string(str(self.contains))
            predicates.append(f'@.{field} like_regex "{escaped}"')

        if self.icontains is not None:
            escaped = escape_jsonpath_string(str(self.icontains))
            predicates.append(f'@.{field} like_regex "{escaped}" flag "i"')

        if self.startswith is not None:
            escaped = escape_jsonpath_string(str(self.startswith))
            predicates.append(f'@.{field} like_regex "^{escaped}"')

        if self.in_ is not None and len(self.in_) > 0:
            # (@.field == val1 || @.field == val2 || ...)
            in_conditions = []
            for val in self.in_:
                in_conditions.append(f'@.{field} == {q}{val}{q}')
            predicates.append(f'({" || ".join(in_conditions)})')

        if self.nin is not None and len(self.nin) > 0:
            # !(@.field == val1 || @.field == val2 || ...)
            nin_conditions = []
            for val in self.nin:
                nin_conditions.append(f'@.{field} == {q}{val}{q}')
            predicates.append(f'!({" || ".join(nin_conditions)})')

        if self.isnull is not None:
            if self.isnull:
                # Match where field is null or doesn't exist
                predicates.append(f'(@.{field} == null || !exists(@.{field}))')
            else:
                # Match where field exists and is not null
                predicates.append(f'(exists(@.{field}) && @.{field} != null)')

        if not predicates:
            raise ValueError('No filter conditions specified')

        # Combine predicates with AND
        predicate_str = ' && '.join(predicates)

        # Build the JSONPath: $.array[*] ? (predicate)
        # Don't add quotes - psycopg will handle that when passing as parameter
        json_path_str = f'$.{array_path} ? ({predicate_str})'

        # Build the Template - pass JSONPath as a parameter
        column_parts = sql.Identifier(*column_name.split('.'))

        return t'jsonb_path_exists({column_parts:i}, {json_path_str})'


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


def infer_type_from_filter(value: GenericFilter) -> str:
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
    """
    # Collect all non-None comparison values
    sample_values: list[Any] = []
    for k, v in value.__dict__.items():
        if k == 'isnull' or v is None:
            continue
        if isinstance(v, list):
            sample_values.extend(v)
        else:
            sample_values.append(v)

    # Infer type non-None values
    cast_types = set()
    for v in sample_values:
        if isinstance(v, bool):
            cast_types.add('boolean')
        elif isinstance(v, int):
            cast_types.add('integer')
        elif isinstance(v, float):
            cast_types.add('numeric')
        else:
            cast_types.add('string')

    if len(cast_types) > 1:
        raise ValueError(
            f'Mixed types in filter values: {cast_types}. All values in a filter must be of the same type.'
        )

    return cast_types.pop() if cast_types else 'string'


def prepare_query_from_dict_field(
    filter_: dict[str, Any], column_name: str
) -> Template | None:
    """
    Prepare a SQL query from a dict field, which is a dict of GenericFilters.
    Usually this is a JSON field in the database that we want to query on.

    Supports two modes:
    1. Simple key-value: 'meta' column with key 'foo' -> meta ->> 'foo'
    2. Array filtering: 'meta' column with key 'reads[*].size' -> jsonb_path_exists(meta, '$.reads[*] ? (@.size >= 10000)')
    """
    wheres: list[Template | None] = []

    for key, field_filter in filter_.items():
        # Check if this is array filtering (contains '[*]')
        if any(sub in key for sub in ('"', '[*]', '.')):
            # Use JSONPath exists for array filtering
            where_filter = field_filter.to_jsonpath_exists(column_name, key)
        else:
            # Simple key-value extraction with type casting
            column_parts = sql.Identifier(*column_name.split('.'))
            cast_type: str = infer_type_from_filter(field_filter)

            json_expr = t'{column_parts:i}->>{key}'
            if cast_type != 'string':
                # Cast the JSON text extraction to the appropriate type
                # Use sql.SQL() to insert the type name as a literal, not a parameter
                json_expr = t'({json_expr:q})::{sql.SQL(cast_type):q}'

            # Construct sql where clause for this key and filter
            where_filter = field_filter.to_sql(column=json_expr)

            # If we are using an exclusion filter (nin, neq)
            # we also include rows where the key doesn't exist or the json
            # value is null. The exception is if isnull=False
            if field_filter.isnull is not False and (
                field_filter.nin or field_filter.neq
            ):
                where_filter = t'({where_filter:q} OR {json_expr:q} IS NULL)'

        wheres.append(where_filter)

    wheres_rm_none: list[Template] = [f for f in wheres if f is not None]
    if len(wheres_rm_none) == 0:
        return None

    # Join Template objects using the + operator
    return sql.SQL(' AND ').join(wheres_rm_none)
