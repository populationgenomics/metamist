from typing import Iterable, Sequence, Optional

import os
import logging

ProjectId = int

levels_map = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING}

LOGGING_LEVEL = levels_map[os.getenv('SM_LOGGING_LEVEL', 'DEBUG').upper()]
USE_GCP_LOGGING = os.getenv('SM_ENABLE_GCP_LOGGING', '0').lower() in ('y', 'true', '1')

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
        self, project_names: Sequence[Optional[str]], author: str, *args, readonly: bool = None
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
