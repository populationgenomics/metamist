# pylint: disable=global-statement
import os

TRUTH_SET = ('1', 'y', 't', 'true')

_ALLOW_ALL_ACCESS: bool = os.getenv('SM_ALLOWALLACCESS', 'n').lower() in TRUTH_SET
_DEFAULT_USER = os.getenv('SM_LOCALONLY_DEFAULTUSER')
SKIP_DATABASE_CONNECTION = bool(os.getenv('SM_SKIP_DATABASE_CONNECTION'))
PROFILE_REQUESTS = os.getenv('SM_PROFILE_REQUESTS', 'false').lower() in TRUTH_SET
IGNORE_GCP_CREDENTIALS_ERROR = os.getenv('SM_IGNORE_GCP_CREDENTIALS_ERROR') in TRUTH_SET
MEMBERS_CACHE_LOCATION = os.getenv('SM_MEMBERS_CACHE_LOCATION')


def get_default_user() -> str | None:
    """Determine if a default user is available"""
    if is_all_access() and _DEFAULT_USER:
        return _DEFAULT_USER
    return None


def is_all_access() -> bool:
    """Does SM have full access"""
    return _ALLOW_ALL_ACCESS


def set_all_access(access: bool):
    """Set full_access for future use"""
    global _ALLOW_ALL_ACCESS
    _ALLOW_ALL_ACCESS = access
