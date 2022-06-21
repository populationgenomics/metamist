import logging
import os
import re
import json
from typing import Sequence, Optional, List

ProjectId = int

levels_map = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING}

LOGGING_LEVEL = levels_map[os.getenv('SM_LOGGING_LEVEL', 'DEBUG').upper()]
USE_GCP_LOGGING = os.getenv('SM_ENABLE_GCP_LOGGING', '0').lower() in ('y', 'true', '1')

RE_FILENAME_SPLITTER = re.compile('[,;]')

# pylint: disable=invalid-name
_logger = None


class Forbidden(Exception):
    """Forbidden action"""


class ProjectDoesNotExist(Forbidden):
    """Custom error for ProjectDoesNotExist"""

    def __init__(self, project_name, *args: object) -> None:
        super().__init__(
            f'Project with id "{project_name}" does not exist, '
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
        project_names_str = ', '.join(str(p) for p in project_names)
        access_type = ''
        if readonly is False:
            access_type = 'write '

        super().__init__(
            f'{author} does not have {access_type}access to resources from the '
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

    logging.basicConfig(level=LOGGING_LEVEL)
    _logger = logging.getLogger('sample-metadata-api')

    if USE_GCP_LOGGING:

        # pylint: disable=import-outside-toplevel,c-extension-no-member
        import google.cloud.logging

        client = google.cloud.logging.Client()
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
        return sorted(set(r for f in string for r in split_generic_terms(f)))

    filenames = [f.strip() for f in RE_FILENAME_SPLITTER.split(string)]
    filenames = [f for f in filenames if f]

    return filenames
