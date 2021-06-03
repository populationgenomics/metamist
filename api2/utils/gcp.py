# pylint: disable=unused-import
import os

from google.auth.exceptions import DefaultCredentialsError

IGNORE_GCP_CREDENTIALS_ERROR = bool(os.getenv('SM_IGNORE_GCP_CREDENTIALS_ERROR'))


def setup_gcp_logging(
    is_production,
    ignore_gcp_credentials_error=IGNORE_GCP_CREDENTIALS_ERROR,
):
    """
    Retrieves a Cloud Logging handler based on the environment
    you're running in and integrates the handler with the
    Python logging module. By default this captures all logs
    at INFO level and higher
    """
    if is_production:
        try:
            # pylint: disable=import-outside-toplevel,c-extension-no-member
            import google.cloud.logging

            client = google.cloud.logging.Client()
            client.get_default_handler()
            client.setup_logging()

        except DefaultCredentialsError as exp:
            if not ignore_gcp_credentials_error:
                raise exp


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
