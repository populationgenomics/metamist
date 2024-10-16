# pylint: disable=global-statement,import-self
"""
GraphQL utilities for Metamist, allows you to:
    - construct queries using the `gql` function (which validates graphql syntax)
    - validate queries with metamist schema (by fetching the schema)
"""

import os
from json.decoder import JSONDecodeError
from typing import Any, Dict

import backoff
from gql import Client
from gql import gql as gql_constructor
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.aiohttp import log as aiohttp_logger
from gql.transport.exceptions import TransportServerError
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.requests import log as requests_logger

# this does not import itself, it imports the module
from graphql import DocumentNode  # type: ignore
from requests.exceptions import HTTPError

from cpg_utils.cloud import get_google_identity_token

import metamist.configuration

_sync_client: Client | None = None
_async_client: Client | None = None


def get_local_schema() -> str:
    """Get the local schema"""
    with open(os.path.join(os.path.dirname(__file__), 'schema.graphql')) as f:
        contents = f.read()

    return contents


def get_sm_url():
    """Get the URL for the Metamist GraphQL API"""
    return os.path.join(metamist.configuration.sm_url, 'graphql')


def configure_sync_client(
    url: str = None,
    schema: str = None,
    auth_token: str = None,
    force_recreate=False,
    use_local_schema: bool = False,
):
    """Get sync gql Client"""
    global _sync_client

    if use_local_schema:
        # for local schema, we basically don't allow requests
        schema = get_local_schema()
        return Client(schema=schema)

    if _sync_client and not force_recreate:
        return _sync_client

    env = os.getenv('SM_ENVIRONMENT', 'PRODUCTION').lower()
    if env == 'local':
        transport = RequestsHTTPTransport(url=url or get_sm_url())
    else:
        token = auth_token or get_google_identity_token(
            target_audience=metamist.configuration.sm_url
        )
        transport = RequestsHTTPTransport(
            url=url or get_sm_url(),
            headers={'Authorization': f'Bearer {token}'},
        )

    _sync_client = Client(
        transport=transport, schema=schema, fetch_schema_from_transport=schema is None
    )
    # _sync_client.connect_sync()

    return _sync_client


async def configure_async_client(
    url: str = None, schema: str = None, auth_token: str = None, force_recreate=False
) -> Client:
    """Configure an async client for use with the Metamist GraphQL API"""
    global _async_client

    if _async_client and not force_recreate:
        return _async_client

    env = os.getenv('SM_ENVIRONMENT', 'PRODUCTION').lower()
    if env == 'local':
        transport = AIOHTTPTransport(url=url or get_sm_url())
    else:
        token = auth_token or get_google_identity_token(
            target_audience=metamist.configuration.sm_url
        )
        transport = AIOHTTPTransport(
            url=url or get_sm_url(),
            headers={'Authorization': f'Bearer {token}'},
        )

    _async_client = Client(
        transport=transport, schema=schema, fetch_schema_from_transport=schema is None
    )
    # await _async_client.connect_async()
    return _async_client


async def get_async_client():
    """Get async gql Client"""
    return await configure_async_client()


def gql(
    request_string: str, should_validate=False, use_local_schema=False
) -> DocumentNode:
    """
    Given a String containing a GraphQL request, parse it into a Document.
    Optionally validate by fetching schema from metamist at SM_URL / SM_ENVIRONMENT
    """
    doc = gql_constructor(request_string)
    if should_validate:
        validate(doc, use_local_schema=use_local_schema)
    return doc


def validate(doc: DocumentNode, client=None, use_local_schema=False):
    """
    Validate a GraphQL document against the schema.
    Fetch schema from SM_URL / SM_ENVIRONMENT
    """
    if client is not None and use_local_schema:
        raise ValueError('Cannot provide local_schema and client to validate')
    if isinstance(doc, str):
        doc = gql(doc)

    (client or configure_sync_client(use_local_schema=use_local_schema)).validate(doc)
    return True


# use older style typing to broaden supported Python versions
@backoff.on_exception(
    backoff.fibo,
    exception=(HTTPError, JSONDecodeError, TransportServerError),
    max_time=55,
    max_value=55,
)
def query(
    _query: str | DocumentNode,
    variables: Dict | None = None,
    client: Client | None = None,
    log_response: bool = False,
) -> Dict[str, Any]:
    """Query the metamist GraphQL API"""
    if variables is None:
        variables = {}

    # disable logging for gql
    current_level = aiohttp_logger.level
    if not log_response:
        requests_logger.setLevel('WARNING')

    response = (client or configure_sync_client()).execute_sync(
        _query if isinstance(_query, DocumentNode) else gql(_query),
        variable_values=variables,
    )

    if not log_response:
        requests_logger.setLevel(current_level)
    return response


@backoff.on_exception(
    backoff.expo,
    exception=(HTTPError, JSONDecodeError, TransportServerError),
    max_time=10,
)
async def query_async(
    _query: str | DocumentNode,
    variables: Dict | None = None,
    client: Client | None = None,
    log_response: bool = False,
) -> Dict[str, Any]:
    """Asynchronously query the Metamist GraphQL API"""
    if variables is None:
        variables = {}

    # disable logging for gql
    current_level = aiohttp_logger.level
    if log_response:
        aiohttp_logger.setLevel('WARNING')

    if not client:
        client = await configure_async_client()

    response = await client.execute_async(
        _query if isinstance(_query, DocumentNode) else gql(_query),
        variable_values=variables,
    )

    if log_response:
        aiohttp_logger.setLevel(current_level)

    return response
