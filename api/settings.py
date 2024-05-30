# pylint: disable=global-statement
import os
from functools import lru_cache

from cpg_utils.cloud import read_secret

TRUTH_SET = ('1', 'y', 't', 'true')

LOG_DATABASE_QUERIES = (
    os.getenv('SM_LOG_DATABASE_QUERIES', 'false').lower() in TRUTH_SET
)
_ALLOW_ALL_ACCESS: bool = os.getenv('SM_ALLOWALLACCESS', 'n').lower() in TRUTH_SET
_DEFAULT_USER = os.getenv('SM_LOCALONLY_DEFAULTUSER')
SM_ENVIRONMENT = os.getenv('SM_ENVIRONMENT', 'local').lower()
SKIP_DATABASE_CONNECTION = bool(os.getenv('SM_SKIP_DATABASE_CONNECTION'))
PROFILE_REQUESTS = os.getenv('SM_PROFILE_REQUESTS', 'false').lower() in TRUTH_SET
IGNORE_GCP_CREDENTIALS_ERROR = os.getenv('SM_IGNORE_GCP_CREDENTIALS_ERROR') in TRUTH_SET
MEMBERS_CACHE_LOCATION = os.getenv('SM_MEMBERS_CACHE_LOCATION')

SEQR_URL = os.getenv('SM_SEQR_URL')
SEQR_AUDIENCE = os.getenv('SM_SEQR_AUDIENCE')
SEQR_MAP_LOCATION = os.getenv('SM_SEQR_MAP_LOCATION')

# you can set one of the following for pluggin in slack notifications
_SLACK_SECRET_PROJECT_ID = os.getenv('SM_SLACK_SECRET_PROJECT_ID')
_SLACK_SECRET_ID = os.getenv('SM_SLACK_SECRET_ID')

# or
_SLACK_TOKEN = os.getenv('SM_SLACK_TOKEN')

SEQR_SLACK_NOTIFICATION_CHANNEL = os.getenv('SM_SEQR_SLACK_NOTIFICATION_CHANNEL')

SAMPLE_PREFIX = os.getenv('SM_SAMPLEPREFIX', 'XPGLCL').upper()
SAMPLE_CHECKSUM_OFFSET = int(os.getenv('SM_SAMPLECHECKOFFSET', '2'))

SEQUENCING_GROUP_PREFIX = os.getenv('SM_SEQUENCINGGROUPPREFIX', 'CPGLCL').upper()
SEQUENCING_GROUP_CHECKSUM_OFFSET = int(os.getenv('SM_SEQUENCINGGROUPCHECKOFFSET', '9'))

COHORT_PREFIX = os.getenv('SM_COHORTPREFIX', 'COH').upper()
COHORT_CHECKSUM_OFFSET = int(os.getenv('SM_COHORTCHECKOFFSET', '5'))

COHORT_TEMPLATE_PREFIX = os.getenv('SM_COHORTTEMPLATEPREFIX', 'CTPL').upper()
COHORT_TEMPLATE_CHECKSUM_OFFSET = int(os.getenv('SM_COHORTTEMPLATECHECKOFFSET', '3'))

# billing settings
BQ_AGGREG_VIEW = os.getenv('SM_GCP_BQ_AGGREG_VIEW')
BQ_AGGREG_RAW = os.getenv('SM_GCP_BQ_AGGREG_RAW')
BQ_AGGREG_EXT_VIEW = os.getenv('SM_GCP_BQ_AGGREG_EXT_VIEW')
BQ_BUDGET_VIEW = os.getenv('SM_GCP_BQ_BUDGET_VIEW')
BQ_GCP_BILLING_VIEW = os.getenv('SM_GCP_BQ_BILLING_VIEW')
BQ_BATCHES_VIEW = os.getenv('SM_GCP_BQ_BATCHES_VIEW')

# BQ cost per 1 TB, used to calculate cost of BQ queries
BQ_COST_PER_TB = 6.25

# This is to optimise BQ queries, DEV table has data only for Mar 2023
BQ_DAYS_BACK_OPTIMAL = 30  # Look back 30 days for optimal query
BILLING_CACHE_RESPONSE_TTL = 3600  # 1 Hour


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


@lru_cache
def get_slack_token(allow_empty=False):
    """Get slack token"""
    if _SLACK_TOKEN:
        return _SLACK_TOKEN

    if _SLACK_SECRET_ID and _SLACK_SECRET_PROJECT_ID:
        return read_secret(
            project_id=_SLACK_SECRET_PROJECT_ID,
            secret_name=_SLACK_SECRET_ID,
            fail_gracefully=allow_empty,
        )
    if allow_empty:
        return None

    raise ValueError('No slack token found')
