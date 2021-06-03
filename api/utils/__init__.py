"""Importing GCP libraries"""
from os import getenv

from .openapi import get_openapi_schema_func
from .db import authenticate, get_db_connection, dependable_get_connection

IS_PRODUCTION = getenv('SM_ENVIRONMENT') == 'PRODUCTION'
