import os
import time
from typing import Annotated

import httpx
from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.params import Depends
from starlette.background import BackgroundTask
from starlette.responses import RedirectResponse, StreamingResponse

from cpg_utils.cloud import get_google_identity_token

from api.settings import (
    SM_ENVIRONMENT,
)
from api.utils.db import authenticate
from db.python.utils import get_logger


# This tag is automatically updated by bump-my-version
_VERSION = '7.14.0'


logger = get_logger()


app = FastAPI()


if SM_ENVIRONMENT == 'local':
    app.add_middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    )


@app.middleware('http')
async def add_process_time_header(request: Request, call_next):
    """Add X-Process-Time to all requests for logging"""
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers['X-Process-Time'] = f'{round(process_time * 1000, 1)}ms'
    return response


client = httpx.AsyncClient(timeout=None)
TARGET_URL = os.getenv('METAMIST_PROXY_TARGET_URL')
TARGET_AUDIENCE = os.getenv('METAMIST_PROXY_TARGET_AUDIENCE')

HEADERS_TO_PASS = [
    'sm-ar-guid',
    'sm-extra-values',
    'sm-on-behalf-of',
    'content-type',
    'accept',
]


@app.api_route(
    '/{path:path}', methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'HEAD', 'PATCH']
)
async def proxy(
    request: Request,
    path: str,
    author: Annotated[str, Depends(authenticate)],
):
    """Proxy to forward requests to new metamist server"""
    assert TARGET_AUDIENCE
    assert TARGET_URL
    """Proxy all requests to the target URL"""
    url = f'{TARGET_URL.rstrip("/")}/{path}'
    if request.url.query:
        url += f'?{request.url.query}'

    # If this isn't an API request, then return a redirect to the new server
    if not (path in {'graphql', 'api/v1'} or path.startswith(('graphql/', 'api/v1/'))):
        return RedirectResponse(url=url, status_code=302)

    headers: dict[str, str] = {}

    for header in HEADERS_TO_PASS:
        value = request.headers.get(header)
        if value:
            headers[header] = value

    # Add Google identity token if available
    token = await run_in_threadpool(
        get_google_identity_token,
        target_audience=TARGET_AUDIENCE,
    )

    if token:
        headers['Authorization'] = f'Bearer {token}'

    headers['sm-legacy-proxy-author'] = author

    req = client.build_request(
        request.method, url, headers=headers, content=request.stream()
    )

    r = await client.send(req, stream=True)
    return StreamingResponse(
        r.aiter_raw(),
        status_code=r.status_code,
        headers=r.headers,
        background=BackgroundTask(r.aclose),
    )


if __name__ == '__main__':
    import logging

    import uvicorn

    logging.getLogger('watchfiles').setLevel(logging.WARNING)
    logging.getLogger('watchfiles.main').setLevel(logging.WARNING)

    uvicorn.run(
        'api.server:app',
        host='0.0.0.0',
        port=int(os.getenv('PORT', '8000')),
        reload=True,
    )
