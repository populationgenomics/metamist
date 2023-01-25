# pylint: disable=global-statement
import os

TRUTH_SET = ('1', 'y', 't', 'true')

_ALLOW_FULL_ACCESS: bool = os.getenv('SM_ALLOWALLACCESS', 'n').lower() in TRUTH_SET
SKIP_DATABASE_CONNECTION = bool(os.getenv('SM_SKIP_DATABASE_CONNECTION'))
PROFILE_REQUESTS = os.getenv('SM_PROFILE_REQUESTS', 'false').lower() in TRUTH_SET
IGNORE_GCP_CREDENTIALS_ERROR = os.getenv('SM_IGNORE_GCP_CREDENTIALS_ERROR') in TRUTH_SET
MEMBERS_CACHE_LOCATION = os.getenv('SM_MEMBERS_CACHE_LOCATION')


def is_full_access() -> bool:
    """Does SM have full access"""
    return _ALLOW_FULL_ACCESS


def set_full_access(access: bool):
    """Set full_access for future use"""
    global _ALLOW_FULL_ACCESS
    _ALLOW_FULL_ACCESS = access
