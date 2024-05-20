import dataclasses
import json
import logging
import os
import re
from enum import Enum
from typing import Any, Generic, Sequence, TypeVar

T = TypeVar('T')

levels_map = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING}

LOGGING_LEVEL = levels_map[os.getenv('SM_LOGGING_LEVEL', 'INFO').upper()]
USE_GCP_LOGGING = os.getenv('SM_ENABLE_GCP_LOGGING', '0').lower() in ('y', 'true', '1')

RE_FILENAME_SPLITTER = re.compile('[,;]')
NONFIELD_CHARS_REGEX = re.compile(r'[^a-zA-Z0-9_]')

# pylint: disable=invalid-name
_logger = None


class NoOpAenter:
    """
    Sometimes it's useful to use `async with VARIABLE()`, and have
    either VARIABLE be a transaction, or noop (eg: when a transaction
    is already taking place). Use this in place.
    """

    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class Forbidden(Exception):
    """Forbidden action"""


class NotFoundError(Exception):
    """Custom error when you can't find something"""


class InternalError(Exception):
    """An internal programming error"""


class ProjectDoesNotExist(Forbidden):
    """Custom error for ProjectDoesNotExist"""

    def __init__(self, project_name, *args: object) -> None:
        super().__init__(
            f'Project with id {project_name!r} does not exist, '
            'or you do not have the appropriate permissions',
            *args,
        )


class NoProjectAccess(Forbidden):
    """Not allowed access to a project (or not allowed project-less access)"""

    def __init__(
        self,
        project_names: Sequence[str] | None,
        author: str,
        *args,
        readonly: bool = None,
    ):
        project_names_str = ', '.join(repr(p) for p in project_names)
        access_type = ''
        if readonly is False:
            access_type = 'write '

        super().__init__(
            f'{author} does not have {access_type}access to resources from the '
            f'following project(s), or they may not exist: {project_names_str}',
            *args,
        )


# pylint: disable=too-many-instance-attributes
class GenericFilter(Generic[T]):
    """
    Generic filter for eq, in_ (in) and nin (not in)
    """

    eq: T | None = None
    in_: Sequence[T] | None = None
    nin: Sequence[T] | None = None
    gt: T | None = None
    gte: T | None = None
    lt: T | None = None
    lte: T | None = None
    contains: T | None = None
    icontains: T | None = None

    def __init__(
        self,
        *,
        eq: T | None = None,
        in_: Sequence[T] | None = None,
        nin: Sequence[T] | None = None,
        gt: T | None = None,
        gte: T | None = None,
        lt: T | None = None,
        lte: T | None = None,
        contains: T | None = None,
        icontains: T | None = None,
    ):
        self.eq = eq
        self.in_ = in_
        self.nin = nin
        self.gt = gt
        self.gte = gte
        self.lt = lt
        self.lte = lte
        self.contains = contains
        self.icontains = icontains

    def __repr__(self):
        keys = ['eq', 'in_', 'nin', 'gt', 'gte', 'lt', 'lte', 'contains', 'icontains']
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
                tuple(self.in_) if self.in_ is not None else None,
                tuple(self.nin) if self.nin is not None else None,
                self.gt,
                self.gte,
                self.lt,
                self.lte,
                self.contains,
                self.icontains,
            )
        )

    def __hash__(self):
        """Override to ensure we can hash this object"""
        return hash(self.get_hashable_value())

    @staticmethod
    def generate_field_name(name):
        """
        Replace any non \\w characters with an underscore

        >>> GenericFilter.generate_field_name('foo')
        'foo'
        >>> GenericFilter.generate_field_name('foo.bar')
        'foo_bar'
        >>> GenericFilter.generate_field_name('$foo bar:>baz')
        '_foo_bar__baz'
        """
        return NONFIELD_CHARS_REGEX.sub('_', name)

    def is_false(self) -> bool:
        """
        The filter will resolve to False (usually because the in_ is an empty list)
        """
        return self.in_ is not None and len(self.in_) == 0

    def to_sql(
        self, column: str, column_name: str | None = None
    ) -> tuple[str, dict[str, T | list[T]]]:
        """Convert to SQL, and avoid SQL injection

        Args:
            column (str): The expression, or column name that derives the values
            column_name (str, optional): A column name to use in the field_override.
                We'll replace any non-alphanumeric characters with an _.
                (Defaults to None)

        Returns:
            tuple[str, dict[str, T | list[T]]]: (condition, prepared_values)
        """
        conditionals = []
        values: dict[str, T | list[T]] = {}
        _column_name = column_name or column

        if not isinstance(column, str):
            raise ValueError(f'Column {_column_name!r} must be a string')
        if self.eq is not None:
            k = self.generate_field_name(_column_name + '_eq')
            conditionals.append(f'{column} = :{k}')
            values[k] = self._sql_value_prep(self.eq)
        if self.in_ is not None:
            if len(self.in_) == 0:
                # in an empty list is always false
                return 'FALSE', {}
            if not isinstance(self.in_, list):
                raise ValueError('IN filter must be a list')
            if len(self.in_) == 1:
                k = self.generate_field_name(_column_name + '_in_eq')
                conditionals.append(f'{column} = :{k}')
                values[k] = self._sql_value_prep(self.in_[0])
            else:
                k = self.generate_field_name(_column_name + '_in')
                conditionals.append(f'{column} IN :{k}')
                values[k] = self._sql_value_prep(self.in_)
        if self.nin is not None and len(self.nin) > 0:
            # not in an empty list is always true
            if not isinstance(self.nin, list):
                raise ValueError('NIN filter must be a list')
            k = self.generate_field_name(column + '_nin')
            conditionals.append(f'{column} NOT IN :{k}')
            values[k] = self._sql_value_prep(self.nin)
        if self.gt is not None:
            k = self.generate_field_name(column + '_gt')
            conditionals.append(f'{column} > :{k}')
            values[k] = self._sql_value_prep(self.gt)
        if self.gte is not None:
            k = self.generate_field_name(column + '_gte')
            conditionals.append(f'{column} >= :{k}')
            values[k] = self._sql_value_prep(self.gte)
        if self.lt is not None:
            k = self.generate_field_name(column + '_lt')
            conditionals.append(f'{column} < :{k}')
            values[k] = self._sql_value_prep(self.lt)
        if self.lte is not None:
            k = self.generate_field_name(column + '_lte')
            conditionals.append(f'{column} <= :{k}')
            values[k] = self._sql_value_prep(self.lte)
        if self.contains is not None:
            search_term = escape_like_term(str(self.contains))
            k = self.generate_field_name(column + '_contains')
            conditionals.append(f'{column} LIKE :{k}')
            values[k] = self._sql_value_prep(f'%{search_term}%')
        if self.icontains is not None:
            search_term = escape_like_term(str(self.icontains))
            k = self.generate_field_name(column + '_icontains')
            conditionals.append(f'LOWER({column}) LIKE LOWER(:{k})')
            values[k] = self._sql_value_prep(f'%{search_term}%')

        return ' AND '.join(conditionals), values

    @staticmethod
    def _sql_value_prep(value):
        if isinstance(value, list):
            return [GenericFilter._sql_value_prep(v) for v in value]
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, dict):
            return {k: GenericFilter._sql_value_prep(v) for k, v in value.items()}

        # nothing else to do at this itme
        return value


def get_hashable_value(value):
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


# pylint: disable=missing-class-docstring
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

    def is_false(self) -> bool:
        """
        Returns False if any of the internal filters is FALSE
        """
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if isinstance(value, GenericFilter) and value.is_false():
                return True

            if isinstance(value, dict):
                if any(f.is_false() for f in value.values()):
                    return True

        return False

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
    ) -> tuple[str, dict[str, Any]]:
        """Convert the model to SQL, and avoid SQL injection"""
        if self.is_false():
            return 'FALSE', {}

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
        conditionals, values = [], {}
        for field in fields:
            if only and field.name not in only:
                continue
            if exclude and field.name in exclude:
                continue

            fcolumn = _foverrides.get(field.name, field.name)
            if filter_ := getattr(self, field.name):
                if isinstance(filter_, dict):
                    meta_conditionals, meta_values = prepare_query_from_dict_field(
                        filter_=filter_, field_name=field.name, column_name=fcolumn
                    )
                    conditionals.extend(meta_conditionals)
                    values.update(meta_values)
                elif isinstance(filter_, GenericFilter):
                    fconditionals, fvalues = filter_.to_sql(fcolumn)
                    conditionals.append(fconditionals)
                    values.update(fvalues)
                elif not isinstance(field.type, (GenericFilter, dict)):
                    values.update({fcolumn: filter_})
                    conditionals.append(f'{fcolumn} = :{fcolumn}')
                else:
                    raise ValueError(
                        f'Filter {field.name} must be a GenericFilter or dict[str, GenericFilter]'
                    )

        if not conditionals:
            return 'True', {}

        return ' AND '.join(filter(None, conditionals)), values


def prepare_query_from_dict_field(
    filter_, field_name, column_name
) -> tuple[list[str], dict[str, Any]]:
    """
    Prepare a SQL query from a dict field, which is a dict of GenericFilters.
    Usually this is a JSON field in the database that we want to query on.
    """
    conditionals: list[str] = []
    values: dict[str, Any] = {}
    for key, value in filter_.items():
        if not isinstance(value, GenericFilter):
            raise ValueError(f'Filter {field_name} must be a GenericFilter')
        if '"' in key:
            raise ValueError('Meta key contains " character, which is not allowed')
        fconditionals, fvalues = value.to_sql(
            f"JSON_EXTRACT({column_name}, '$.{key}')",
            column_name=f'{column_name}_{key}',
        )
        conditionals.append(fconditionals)
        values.update(fvalues)

    return conditionals, values


def get_logger():
    """
    Retrieves a Cloud Logging handler based on the environment
    you're running in and integrates the handler with the
    Python logging module. By default this captures all logs
    at INFO level and higher
    """
    # pylint: disable=invalid-name,global-statement
    global _logger
    if _logger:
        return _logger

    for lname in ('asyncio', 'urllib3', 'databases'):
        logging.getLogger(lname).setLevel(logging.WARNING)

    _logger = logging.getLogger('sample-metadata-api')
    _logger.setLevel(level=LOGGING_LEVEL)

    if USE_GCP_LOGGING:
        # pylint: disable=import-outside-toplevel,c-extension-no-member
        import google.cloud.logging

        client = google.cloud.logging.Client()  # pylint: disable=no-member
        client.get_default_handler()
        client.setup_logging()

    return _logger


def to_db_json(val):
    """Convert val to json for DB"""
    # return psycopg2.extras.Json(val)
    return json.dumps(val)


def split_generic_terms(string: str) -> list[str]:
    """
    Take a string and split on both [,;]
    """
    if not string:
        return []
    if isinstance(string, list):
        return sorted(set(r.strip() for f in string for r in split_generic_terms(f)))

    # strip, because sometimes collaborators use ', ' instead of ','
    filenames = [f.strip() for f in RE_FILENAME_SPLITTER.split(string)]
    filenames = [f for f in filenames if f]

    return filenames


def escape_like_term(query: str):
    """
    Escape meaningful keys when using LIKE with a user supplied input
    """

    return query.replace('%', '\\%').replace('_', '\\_')
