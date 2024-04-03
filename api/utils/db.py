import json
import logging
from os import getenv

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from google.auth.transport import requests
from google.oauth2 import id_token

from api.settings import get_default_user
from api.utils.gcp import email_from_id_token
from db.python.connect import Connection, SMConnections
from db.python.gcp_connect import BqConnection, PubSubConnection
from db.python.tables.project import ProjectPermissionsTable

EXPECTED_AUDIENCE = getenv('SM_OAUTHAUDIENCE')


def get_jwt_from_request(request: Request) -> str | None:
    """
    Get google JWT value, capture it like this instead of using
        x_goog_iap_jwt_assertion = Header(None)
    so it doesn't show up in the swagger parameters section
    """
    return request.headers.get('x-goog-iap-jwt-assertion')


def get_ar_guid(request: Request) -> str | None:
    """Get sm-ar-guid from the headers to provide with requests"""
    return request.headers.get('sm-ar-guid')


def get_extra_audit_log_values(request: Request) -> dict | None:
    """Get a JSON encoded dictionary from the 'sm-extra-values' header if it exists"""
    headers = request.headers.get('sm-extra-values')
    if not headers:
        return None

    try:
        return json.loads(headers)
    except json.JSONDecodeError:
        logging.error(f'Could not parse sm-extra-values: {headers}')
        return None


def get_on_behalf_of(request: Request) -> str | None:
    """
    Get sm-on-behalf-of if there are requests that were performed on behalf of
    someone else (some automated process)
    """
    return request.headers.get('sm-on-behalf-of')


def authenticate(
    token: HTTPAuthorizationCredentials | None = Depends(HTTPBearer(auto_error=False)),
    x_goog_iap_jwt_assertion: str | None = Depends(get_jwt_from_request),
) -> str:
    """
    If a token (OR Google IAP auth jwt) is provided,
    return the email, else raise an Exception
    """
    if x_goog_iap_jwt_assertion:
        # We have to PREFER the IAP's identity, otherwise you could have a case where
        # the JWT is forged, but IAP lets it through and authenticates, but then we take
        # the identity then without checking.
        return validate_iap_jwt_and_get_email(
            x_goog_iap_jwt_assertion, audience=EXPECTED_AUDIENCE
        )

    if token:
        return email_from_id_token(token.credentials)

    if default_user := get_default_user():
        # this should only happen in LOCAL environments
        logging.info(f'Using {default_user} as authenticated user')
        return default_user

    raise HTTPException(status_code=401, detail='Not authenticated :(')


async def dependable_get_write_project_connection(
    project: str,
    request: Request,
    author: str = Depends(authenticate),
    ar_guid: str = Depends(get_ar_guid),
    extra_values: dict | None = Depends(get_extra_audit_log_values),
    on_behalf_of: str | None = Depends(get_on_behalf_of),
) -> Connection:
    """FastAPI handler for getting connection WITH project"""
    meta = {'path': request.url.path}
    if request.client:
        meta['ip'] = request.client.host
    if extra_values:
        meta.update(extra_values)

    return await ProjectPermissionsTable.get_project_connection(
        project_name=project,
        author=author,
        readonly=False,
        ar_guid=ar_guid,
        on_behalf_of=on_behalf_of,
        meta=meta,
    )


async def dependable_get_readonly_project_connection(
    project: str,
    author: str = Depends(authenticate),
    ar_guid: str = Depends(get_ar_guid),
    extra_values: dict | None = Depends(get_extra_audit_log_values),
) -> Connection:
    """FastAPI handler for getting connection WITH project"""
    meta = {}
    if extra_values:
        meta.update(extra_values)

    return await ProjectPermissionsTable.get_project_connection(
        project_name=project,
        author=author,
        readonly=True,
        on_behalf_of=None,
        ar_guid=ar_guid,
        meta=meta,
    )


async def dependable_get_connection(
    request: Request,
    author: str = Depends(authenticate),
    ar_guid: str = Depends(get_ar_guid),
    extra_values: dict | None = Depends(get_extra_audit_log_values),
):
    """FastAPI handler for getting connection withOUT project"""
    meta = {'path': request.url.path}
    if request.client:
        meta['ip'] = request.client.host

    if extra_values:
        meta.update(extra_values)

    return await SMConnections.get_connection_no_project(
        author, ar_guid=ar_guid, meta=meta
    )


async def dependable_get_bq_connection(author: str = Depends(authenticate)):
    """FastAPI handler for getting connection withOUT project"""
    return await BqConnection.get_connection_no_project(author)


async def dependable_get_pubsub_connection(
    author: str = Depends(authenticate), topic: str = None
):
    """FastAPI handler for getting connection withOUT project"""
    return await PubSubConnection.get_connection_no_project(author, topic)


def validate_iap_jwt_and_get_email(iap_jwt, audience):
    """
    Validate an IAP JWT and return email
    Source: https://cloud.google.com/iap/docs/signed-headers-howto

    :param iap_jwt: The contents of the X-Goog-IAP-JWT-Assertion header.
    :param audience: The audience to validate against
    """

    try:
        decoded_jwt = id_token.verify_token(
            iap_jwt,
            requests.Request(),
            audience=audience,
            certs_url='https://www.gstatic.com/iap/verify/public_key',
        )
        return decoded_jwt['email']
    except Exception as e:
        logging.error(f'JWT validation error {e}')
        raise e


get_author = Depends(authenticate)
get_project_readonly_connection = Depends(dependable_get_readonly_project_connection)
get_project_write_connection = Depends(dependable_get_write_project_connection)
get_projectless_db_connection = Depends(dependable_get_connection)
get_projectless_bq_connection = Depends(dependable_get_bq_connection)
get_projectless_pubsub_connection = Depends(dependable_get_pubsub_connection)
