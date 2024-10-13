import json
import logging
import os
import re
from typing import Any, TypeVar

T = TypeVar('T')
X = TypeVar('X')

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
        # Do nothing on enter
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Do nothing on exit
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
        project_names: list[str],
        author: str,
        allowed_roles: list[str],
        *args: tuple[Any, ...],
    ):
        project_names_str = (
            ', '.join(repr(p) for p in project_names) if project_names else ''
        )
        required_roles_str = ' or '.join(allowed_roles)

        super().__init__(
            f'{author} does not have {required_roles_str} access to resources from the '
            f'following project(s), or they may not exist: {project_names_str}',
            *args,
        )


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


def from_db_json(text):
    """Convert DB's JSON text to Python object"""
    return json.loads(text)


def to_db_json(val):
    """Convert val to json for DB"""
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
