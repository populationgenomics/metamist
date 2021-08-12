"""Importing GCP libraries"""
from os import getenv

from .openapi import get_openapi_schema_func
from .db import (
    authenticate,
    get_project_db_connection,
    dependable_get_project_connection,
)

IS_PRODUCTION = getenv('SM_ENVIRONMENT') == 'PRODUCTION'
