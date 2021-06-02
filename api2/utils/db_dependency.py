from typing import Optional
from os import getenv

from fastapi import (
    HTTPException,
    Request,
    Depends,
)
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from databases import Database
from google.auth.exceptions import DefaultCredentialsError

from db.python.connect import SMConnections, Connection


# 2021-06-02 mfranklin:
#   Sometimes it's useful to start the server without a GCP context,
#   and we don't want to run the import everytime we make a request.
#   So if the import and receive the DefaultCredentialsError, we'll
#   alias the 'email_from_id_token' function, and return the error then.
try:
    # pylint: disable=import-outside-toplevel
    from cpg_utils.cloud import email_from_id_token

except DefaultCredentialsError as e:
    if bool(getenv('SM_IGNORE_GCP_CREDENTIALS_ERROR')):
        exception_args = e.args

        def email_from_id_token(*args, **kwargs):
            """Raises DefaultCredentialsError at runtime"""
            raise DefaultCredentialsError(*exception_args)

    else:
        raise e


auth = HTTPBearer()


def authenticate(
    token: Optional[HTTPAuthorizationCredentials] = Depends(auth),
) -> Optional[str]:
    """If a token is provided, return the email, else return None"""
    if token:
        return email_from_id_token(token.credentials)
    return None


def get_connection(project: str, author: str = Depends(authenticate)) -> Connection:
    """FastAPI handler for getting connection"""
    return SMConnections.get_connection_for_project(project, author)
