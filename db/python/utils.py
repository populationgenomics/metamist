import logging
import os
import re
import json
from typing import Sequence, Optional, List

ProjectId = int

levels_map = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING}

LOGGING_LEVEL = levels_map[os.getenv('SM_LOGGING_LEVEL', 'INFO').upper()]
USE_GCP_LOGGING = os.getenv('SM_ENABLE_GCP_LOGGING', '0').lower() in ('y', 'true', '1')

RE_FILENAME_SPLITTER = re.compile('[,;]')

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
        project_names: Sequence[Optional[str]],
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


from typing import TypeVar, Generic, Any
import dataclasses

T = TypeVar("T")


class GenericFilter(Generic[T]):
    eq: T | None = None
    in_: list[T] | None = None
    nin: list[T] | None = None

    def __init__(
        self,
        eq: T | None = None,
        in_: list[T] | None = None,
        nin: list[T] | None = None,
    ):
        self.eq = eq
        self.in_ = in_
        self.nin = nin

    def __hash__(self):
        return hash(
            (
                self.eq,
                tuple(self.in_) if self.in_ is not None else None,
                tuple(self.nin) if self.nin is not None else None,
            )
        )

    @staticmethod
    def generate_field_name(name):
        return name.replace('.', '_').replace(' ', '_').lower()

    def to_sql(self, column: str) -> tuple[str, dict[str, T]]:
        """Convert to SQL, and avoid SQL injection"""
        conditionals = []
        values = {}
        if self.eq is not None:
            k = self.generate_field_name(column + '_eq')
            conditionals.append(f"{column} = :{k}")
            values[k] = self.eq
        if self.in_ is not None:
            if not isinstance(self.in_, list):
                raise ValueError("IN filter must be a list")
            k = self.generate_field_name(column + '_in')
            conditionals.append(f"{column} IN :{k}")
            values[k] = self.in_
        if self.nin is not None:
            if not isinstance(self.nin, list):
                raise ValueError("NIN filter must be a list")
            k = self.generate_field_name(column + '_nin')
            conditionals.append(f"{column} NOT IN :{k}")
            values[k] = self.nin
        return " AND ".join(conditionals), values


GenericMetaFilter = dict[str, GenericFilter[Any]]


@dataclasses.dataclass(kw_only=True)
class GenericFilterModel:
    def __hash__(self):
        return hash(dataclasses.astuple(self))

    def __post_init__(self):

        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if value is None:
                continue

            if isinstance(value, (GenericFilter, dict)):
                continue

            # lazily provided a value, which we'll correct
            if isinstance(value, list):
                setattr(self, field.name, GenericFilter(in_=value))
            else:
                setattr(self, field.name, GenericFilter(eq=value))

    def to_sql(
        self, field_overrides: dict[str, str] = None
    ) -> tuple[str, dict[str, Any]]:
        _foverrieds = field_overrides or {}

        fields = dataclasses.fields(self)
        conditionals, values = [], {}
        for field in fields:
            fcolumn = _foverrieds.get(field.name, field.name)
            if filter_ := getattr(self, field.name):
                if isinstance(filter_, dict):
                    for key, value in filter_.items():
                        if not isinstance(value, GenericFilter):
                            raise ValueError(
                                f"Filter {field.name} must be a GenericFilter"
                            )
                        if '"' in key:
                            raise ValueError(
                                'Meta key contains " character, which is not allowed'
                            )
                        fconditionals, fvalues = value.to_sql(
                            f'JSON_EXTRACT({fcolumn}, "$.{key}")'
                        )
                        conditionals.append(fconditionals)
                        values.update(fvalues)
                elif isinstance(filter_, GenericFilter):
                    fconditionals, fvalues = filter_.to_sql(fcolumn)
                    conditionals.append(fconditionals)
                    values.update(fvalues)
                else:

                    raise ValueError(
                        f"Filter {field.name} must be a GenericFilter or dict[str, GenericFilter]"
                    )

        return " AND ".join(conditionals), values


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


def split_generic_terms(string: str) -> List[str]:
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
