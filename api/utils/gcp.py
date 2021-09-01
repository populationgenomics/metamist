# pylint: disable=unused-import
import os

from google.auth.exceptions import DefaultCredentialsError


IGNORE_GCP_CREDENTIALS_ERROR = os.getenv('SM_IGNORE_GCP_CREDENTIALS_ERROR') in (
    'y',
    'true',
    '1',
)

# 2021-06-02 mfranklin:
#   Sometimes it's useful to start the server without a GCP context,
#   and we don't want to run the import everytime we make a request.
#   So if the import and receive the DefaultCredentialsError, we'll
#   alias the 'email_from_id_token' function, and return the error then.
try:
    # pylint: disable=import-outside-toplevel
    from cpg_utils.cloud import email_from_id_token

except DefaultCredentialsError as e:
    if IGNORE_GCP_CREDENTIALS_ERROR:
        exception_args = e.args

        def email_from_id_token(*args, **kwargs):
            """Raises DefaultCredentialsError at runtime"""
            raise DefaultCredentialsError(*exception_args)

    else:
        raise e
