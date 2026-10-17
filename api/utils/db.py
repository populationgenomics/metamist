import json
import logging
import threading
import time
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from http import HTTPStatus
from os import getenv
from typing import Any

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from google.auth.exceptions import MalformedError
from google.auth.transport import requests
from google.oauth2 import id_token
from requests.adapters import HTTPAdapter, Retry
from requests.sessions import Session

from api.settings import get_default_user
from api.utils.gcp import email_from_id_token
from db.python.connect import Connection, SMConnections
from db.python.gcp_connect import BqConnection
from models.models.project import Project, ProjectId, ProjectMemberRole


EXPECTED_AUDIENCE = getenv('SM_OAUTHAUDIENCE')
SM_LEGACY_PROXY_SA = getenv('SM_LEGACY_PROXY_SA')

IAP_CERTS_URL = 'https://www.gstatic.com/iap/verify/public_key'
# How long to cache IAP certs for, these change very infrequently and a failure to
# verify will trigger a refetch, so we can have a pretty high TTL here
IAP_CERTS_CACHE_SECONDS = 60 * 60
# The minimum amount of time between refresh requests caused by malformed tokens. We
# do want a malformed token to cause an attempt to refresh certs incase the malformed
# response is due to out of date cached certs, but don't want to do it too often as
# the JWT might legitimately be malformed.
IAP_CERTS_REFRESH_SECONDS = 60
# default is 120 seconds, which is far too long for just fetching a cert, and timeout
# will be retried.
IAP_CERTS_TIMEOUT_SECONDS = 10


def get_jwt_from_request(request: Request) -> str | None:
    """
    Get google JWT value, capture it like this instead of using
        x_goog_iap_jwt_assertion = Header(None)
    so it doesn't show up in the swagger parameters section
    """
    return request.headers.get('x-goog-iap-jwt-assertion')


def get_sm_legacy_proxy_author(request: Request) -> str | None:
    """
    If this is a request proxied through the old cloud run instance, the author
    will be passed as a header. This should only be trusted if the request is
    authenticated with the known correct service account.
    """
    return request.headers.get('sm-legacy-proxy-author')


def get_ar_guid(request: Request) -> str | None:
    """Get sm-ar-guid from the headers to provide with requests"""
    return request.headers.get('sm-ar-guid')


def get_extra_audit_log_values(request: Request) -> dict[str, Any] | None:
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
    sm_legacy_proxy_author: str | None = Depends(get_sm_legacy_proxy_author),
) -> str:
    """
    If a token (OR Google IAP auth jwt) is provided,
    return the email, else raise an Exception
    """
    author: str | None = None

    if x_goog_iap_jwt_assertion:
        # We have to PREFER the IAP's identity, otherwise you could have a case where
        # the JWT is forged, but IAP lets it through and authenticates, but then we take
        # the identity then without checking.
        assert EXPECTED_AUDIENCE is not None
        author = validate_iap_jwt_and_get_email(
            x_goog_iap_jwt_assertion, audience=EXPECTED_AUDIENCE
        )

    elif token:
        author = email_from_id_token(token.credentials)

    elif default_user := get_default_user():
        # this should only happen in LOCAL environments
        logging.info(f'Using {default_user} as authenticated user')
        author = default_user

    # If this request has come from the old sample metadata cloud run service, which
    # now just proxies requests to this new service, then we want to act as the user
    # that the proxy is sending us.
    if SM_LEGACY_PROXY_SA is not None and author == SM_LEGACY_PROXY_SA:
        if not sm_legacy_proxy_author:
            raise HTTPException(
                status_code=401, detail='Proxy requests must provide author'
            )
        author = sm_legacy_proxy_author

    if author:
        return author

    raise HTTPException(status_code=401, detail='Not authenticated :(')


def dependable_get_project_db_connection(allowed_roles: set[ProjectMemberRole]):
    """Return a partially applied dependable db connection with allowed roles applied"""

    async def dependable_project_db_connection(
        project: str,
        request: Request,
        author: str = Depends(authenticate),
        ar_guid: str = Depends(get_ar_guid),
        extra_values: dict[str, Any] | None = Depends(get_extra_audit_log_values),
        on_behalf_of: str | None = Depends(get_on_behalf_of),
    ) -> AsyncGenerator[Connection]:
        """FastAPI handler for getting connection WITH project"""
        meta = {'path': request.url.path}
        if request.client:
            meta['ip'] = request.client.host

        if extra_values:
            meta.update(extra_values)

        pool = SMConnections.get_postgres_pool()

        async with pool.connection() as connection:
            conn = await SMConnections.get_connection_with_project(
                pg_connection=connection,
                project_name=project,
                author=author,
                allowed_roles=allowed_roles,
                on_behalf_of=on_behalf_of,
                ar_guid=ar_guid,
                meta=meta,
            )

            yield conn

    return dependable_project_db_connection


async def dependable_get_connection(
    request: Request,
    author: str = Depends(authenticate),
    ar_guid: str = Depends(get_ar_guid),
    extra_values: dict[str, Any] | None = Depends(get_extra_audit_log_values),
    on_behalf_of: str | None = Depends(get_on_behalf_of),
):
    """FastAPI handler for getting connection withOUT project"""
    meta = {'path': request.url.path}
    if request.client:
        meta['ip'] = request.client.host

    if extra_values:
        meta.update(extra_values)

    pool = SMConnections.get_postgres_pool()

    async with pool.connection() as connection:
        yield await SMConnections.get_connection_no_project(
            connection, author, ar_guid=ar_guid, meta=meta, on_behalf_of=on_behalf_of
        )


GetConnection = Callable[[], AbstractAsyncContextManager[Connection]]


async def dependable_get_connection_getter(
    request: Request,
    author: str = Depends(authenticate),
    ar_guid: str = Depends(get_ar_guid),
    extra_values: dict[str, Any] | None = Depends(get_extra_audit_log_values),
    on_behalf_of: str | None = Depends(get_on_behalf_of),
) -> GetConnection:
    """FastAPI handler for getting connection getter for connection withOUT project"""
    meta = {'path': request.url.path}
    if request.client:
        meta['ip'] = request.client.host

    if extra_values:
        meta.update(extra_values)

    pool = SMConnections.get_postgres_pool()
    project_id_map: dict[ProjectId, Project] | None = None
    project_name_map: dict[str, Project] | None = None

    @asynccontextmanager
    async def get_connection():
        nonlocal project_id_map, project_name_map
        async with pool.connection() as connection:
            sm_connection = await SMConnections.get_connection_no_project(
                connection,
                author,
                ar_guid=ar_guid,
                meta=meta,
                on_behalf_of=on_behalf_of,
                project_id_map=project_id_map,
                project_name_map=project_name_map,
            )

            # Cache these maps for the next time this get_connection() is called
            if project_id_map is None:
                project_id_map = sm_connection.project_id_map
            if project_name_map is None:
                project_name_map = sm_connection.project_name_map

            yield sm_connection

    return get_connection


async def dependable_get_bq_connection(author: str = Depends(authenticate)):
    """FastAPI handler for getting connection withOUT project"""
    return await BqConnection.get_connection_no_project(author)


class CachingCertsRequest(requests.Request):
    """
    The google auth verify_token request accepts a instance of the requests.Request
    transport. This is a minimal implementation of a transport that caches keys as it
    fetches, with retries enabled for the session.

    This avoids making the http request on _every_ request that hits metamist.
    """

    def __init__(self):
        session = Session()
        session.mount(
            'https://',
            HTTPAdapter(
                max_retries=Retry(
                    total=3,
                    backoff_factor=0.2,
                    status_forcelist=(429, 500, 502, 503, 504),
                )
            ),
        )
        super().__init__(session=session)
        # url -> (time fetched, response)
        self._cache: dict[str, tuple[float, Any]] = {}
        # authenticate() is a sync dependency, so this is shared across the threadpool.
        # having a lock here avoids a bunch of requests being made if nothing is cached
        # yet.
        self._lock = threading.Lock()

    def __call__(
        self, url, *args, timeout=IAP_CERTS_TIMEOUT_SECONDS, max_age=None, **kwargs
    ):
        if max_age is None:
            max_age = IAP_CERTS_CACHE_SECONDS

        with self._lock:
            fetched_at, cached = self._cache.get(url, (0.0, None))
            if cached is not None and time.monotonic() - fetched_at <= max_age:
                return cached

            response = super().__call__(url, *args, timeout=timeout, **kwargs)
            # Don't cache non-ok statuses, statuses in the forcelist above will be
            # retried and won't make it to here, but there's still plenty of possible
            # statuses that we don't want to cache.
            if response.status == HTTPStatus.OK:
                self._cache[url] = (time.monotonic(), response)

            return response

    def refresh(self, url):
        """Refresh certs"""
        self(url, max_age=IAP_CERTS_REFRESH_SECONDS)


iap_certs_request = CachingCertsRequest()


def _verify_iap_jwt(iap_jwt: str, audience: str):
    return id_token.verify_token(
        iap_jwt,
        iap_certs_request,
        audience=audience,
        certs_url=IAP_CERTS_URL,
    )


def validate_iap_jwt_and_get_email(iap_jwt: str, audience: str):
    """
    Validate an IAP JWT and return email
    Source: https://cloud.google.com/iap/docs/signed-headers-howto

    :param iap_jwt: The contents of the X-Goog-IAP-JWT-Assertion header.
    :param audience: The audience to validate against
    """

    try:
        try:
            decoded_jwt = _verify_iap_jwt(iap_jwt, audience)
        except MalformedError:
            # If we get a malformed error, quickly do a refresh of the iap certs just
            # in case the ones we have cached are out of date and that is causing
            # the malformed error.
            iap_certs_request.refresh(IAP_CERTS_URL)
            decoded_jwt = _verify_iap_jwt(iap_jwt, audience)

        return decoded_jwt['email']
    except Exception as e:
        logging.error(f'JWT validation error {e}')
        raise e


get_author = Depends(authenticate)


def get_project_db_connection(allowed_roles: set[ProjectMemberRole]):
    """Get a project db connection with allowed roles applied"""
    return Depends(dependable_get_project_db_connection(allowed_roles))


get_projectless_db_connection = Depends(dependable_get_connection)
get_projectless_db_connection_getter = Depends(dependable_get_connection_getter)
get_projectless_bq_connection = Depends(dependable_get_bq_connection)
