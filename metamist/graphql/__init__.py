import asyncio
import os
from typing import Dict

import aiohttp
from cpg_utils.cloud import get_google_identity_token
from metamist.configuration import sm_url


# use older style typing to broaden supported Python versions
def query(_query: str, variables: Dict = None, auth_token: str = None):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(query_async(
        _query=_query,
    variables=variables,
    auth_token=auth_token,
    ))

async def query_async(_query: str, variables: Dict = None, auth_token: str = None):
    """Query the Metamist GraphQL API"""
    if variables is None:
        variables = {}

    token = auth_token or get_google_identity_token(target_audience=sm_url)

    async with aiohttp.ClientSession() as session:
        request = await session.post(
            os.path.join(sm_url, 'graphql'),
            json={'query': _query, 'variables': variables},
            headers={'Authorization': f'Bearer {token}'},
            timeout=60,
        )

        if not request.ok:
            raise ValueError(
                f'Metamist GraphQL query failed with code {request.status}: {await request.text()}'
            )

        data = await request.json()
    if 'errors' in data:
        raise ValueError(f'Metamist GraphQL query failed: {data["errors"]}')
    return data['data']

