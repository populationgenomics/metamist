import asyncio
import datetime
import os
import time
import traceback
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import ValidationError
from starlette.responses import FileResponse, RedirectResponse, Response

from api import routes
from api.graphql.schema import MetamistGraphQLRouter  # type: ignore
from api.settings import (
    PROFILE_REQUESTS,
    PROFILE_REQUESTS_OUTPUT,
    SKIP_DATABASE_CONNECTION,
    SM_ENVIRONMENT,
)
from api.utils.exceptions import determine_code_from_error
from api.utils.mirror import Comparison, mirror
from api.utils.openapi import get_openapi_schema_func
from db.python.connect import SMConnections
from db.python.utils import get_logger


# This tag is automatically updated by bump-my-version
_VERSION = '7.14.3'


logger = get_logger()

STATIC_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'public')  # noqa: PTH118, PTH120

static_dir_exists = os.path.exists(STATIC_DIR)  # noqa: PTH110


@asynccontextmanager
async def app_lifespan(_: FastAPI):
    """
    Context manager for the app lifecycle. This is useful for running
    code before and after the app is started. This is used by the
    `run` command.
    """
    try:
        if not SKIP_DATABASE_CONNECTION:
            await SMConnections.connect()
        await mirror.open()
        yield
    finally:
        await mirror.close()
        if not SKIP_DATABASE_CONNECTION:
            await SMConnections.disconnect()


app = FastAPI(lifespan=app_lifespan)


if PROFILE_REQUESTS:
    from pyinstrument import Profiler
    from pyinstrument.renderers.speedscope import SpeedscopeRenderer

    @app.middleware('http')
    async def profile_request(request: Request, call_next):
        """optional profiling for http requests"""
        profiler = Profiler(async_mode='enabled')
        profiler.start()
        resp = await call_next(request)
        profiler.stop()

        if 'text' in PROFILE_REQUESTS_OUTPUT:
            text_output = profiler.output_text()
            print(text_output)

        timestamp = (
            datetime.datetime.now().replace(microsecond=0).isoformat().replace(':', '-')
        )

        if 'json' in PROFILE_REQUESTS_OUTPUT:
            os.makedirs('profiles', exist_ok=True)  # noqa: PTH103
            json = profiler.output(renderer=SpeedscopeRenderer())

            with open(f'profiles/{timestamp}.json', 'w') as file:  # noqa: PTH123
                file.write(json)
                file.close()

        if 'html' in PROFILE_REQUESTS_OUTPUT:
            os.makedirs('profiles', exist_ok=True)  # noqa: PTH103
            html = profiler.output_html()
            with open(f'profiles/{timestamp}.html', 'w') as file:  # noqa: PTH123
                file.write(html)
                file.close()

        return resp


if SM_ENVIRONMENT == 'local':
    app.add_middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    )


class SPAStaticFiles(StaticFiles):
    """
    https://stackoverflow.com/a/68363904
    """

    async def get_response(self, path: str, scope):
        """
        Overide get response to server index.html if file isn't found
        (to make single-page-app work correctly)
        """
        response = await super().get_response(path, scope)
        if response.status_code == 404 and not path.startswith('api'):  # noqa: PLR2004
            # server index.html if can't find existing resource
            response = await super().get_response('index.html', scope)
        return response


@app.middleware('http')
async def add_process_time_header(request: Request, call_next):
    """Add X-Process-Time to all requests for logging"""
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers['X-Process-Time'] = f'{round(process_time * 1000, 1)}ms'
    return response


async def _buffer_response_body(response) -> bytes:
    """
    Drain a response body into bytes so it can be both compared and re-served.

    A response returned by `call_next` is typically a streaming response exposing only
    `body_iterator`; the exception handler produces a `JSONResponse` with a `.body`.
    """
    if hasattr(response, 'body_iterator'):
        chunks = [chunk async for chunk in response.body_iterator]
        return b''.join(
            c if isinstance(c, bytes) else c.encode(response.charset) for c in chunks
        )
    return getattr(response, 'body', b'') or b''


def _rebuild_response(response, body: bytes) -> Response:
    """Rebuild a served-able Response from a drained response + its buffered body."""
    rebuilt = Response(
        content=body,
        status_code=response.status_code,
        headers=dict(response.headers),
        media_type=response.media_type,
    )
    rebuilt.background = response.background
    return rebuilt


# Registered LAST so it is the OUTERMOST middleware: it sees the final client bytes and,
# because exception handlers run below all middleware, `call_next` always yields a response
# and never raises. The mirror to the new server happens in a fire-and-forget background
# task (serve-from-old) so it can never add latency to or fail the client response.
@app.middleware('http')
async def mirror_and_compare(request: Request, call_next):
    """Serve the request locally and mirror it to the new server for comparison."""
    path = request.scope['path']

    # When serving from the new server, redirect non-API requests (the frontend and its
    # static assets) to the new server, as the basic proxy did - the whole app should live
    # on the new server in that mode. API/GraphQL requests continue to be served + compared.
    if mirror.enabled and mirror.serve_from_new and not mirror.is_api_path(path):
        return RedirectResponse(
            url=mirror.target_url(path, request.url.query), status_code=302
        )

    if not (mirror.enabled and mirror.should_mirror(path)):
        return await call_next(request)

    # Snapshot everything the background task needs BEFORE the request object goes away.
    # NB: reading the body here is safe - Starlette's BaseHTTPMiddleware caches and replays
    # it to the downstream routes (behaviour is coupled to the pinned Starlette version).
    comparison = Comparison(
        method=request.method,
        path=path,
        query=request.url.query,
        req_headers=dict(request.headers),
        req_body=await request.body(),
    )

    if mirror.serve_from_new:
        # New server is primary. Start its request concurrently with the old route so the
        # new (primary) response isn't serialized behind the old one - the old route still
        # runs as a synchronous backup (its latency may affect the client here, which is
        # acceptable in this mode), and is used for the comparison and as a fallback.
        new_task = asyncio.create_task(mirror.call_new_server(comparison))
        old_response = await call_next(request)
        old_body = await _buffer_response_body(old_response)
        comparison.old_status = old_response.status_code
        comparison.old_body = old_body
        new_resp = await new_task

        if new_resp is None:
            # New server unreachable: fall back to serving the old response, and let the
            # background task retry + record the transport error.
            mirror.schedule_comparison(comparison, new_prefetched=False)
            return _rebuild_response(old_response, old_body)

        comparison.new_status = new_resp.status_code
        comparison.new_body = new_resp.content
        mirror.schedule_comparison(comparison, new_prefetched=True)
        served = Response(
            content=new_resp.content,
            status_code=new_resp.status_code,
            media_type=new_resp.headers.get('content-type'),
        )
        served.background = old_response.background
        return served

    # Default: old server is primary. Serve the OLD response as soon as it is ready; the
    # new-server call and comparison happen entirely in a fire-and-forget background task,
    # so the new server's latency (or errors) can never affect the old server's response.
    old_response = await call_next(request)
    old_body = await _buffer_response_body(old_response)
    comparison.old_status = old_response.status_code
    comparison.old_body = old_body
    mirror.schedule_comparison(comparison, new_prefetched=False)
    return _rebuild_response(old_response, old_body)


@app.exception_handler(404)
async def not_found(request, exc):
    """
    New version of FastAPI not fires this method for 404 errors
    """
    if static_dir_exists:
        return FileResponse(STATIC_DIR + '/index.html')

    return request, exc


@app.exception_handler(Exception)
async def exception_handler(request: Request, e: Exception):
    """Generic exception handler"""
    add_stacktrace = True
    description: str

    if isinstance(e, HTTPException):
        code = e.status_code
        name = e.detail
        description = str(e)
    elif isinstance(e, ValidationError):
        # for whatever reason, calling str(e) here fails
        code = 500
        name = 'ValidationError'
        description = str(e.args)
    else:
        code = determine_code_from_error(e)
        name = str(type(e).__name__)
        description = str(e)

    base_params = {'name': name, 'description': description}

    if add_stacktrace:
        st = traceback.format_exc()
        base_params['stacktrace'] = st

    response = JSONResponse(
        status_code=code,
        content=base_params,
    )

    # https://github.com/tiangolo/fastapi/issues/457#issuecomment-851547205
    # FastAPI doesn't run middleware on exception, but if we make a non-GET/INFO
    # request, then we lose CORS and hence lose the exception in the body of the
    # response. Grab it manually, and explicitly allow origin if so.
    middlewares = [
        m
        for m in app.user_middleware
        if isinstance(m, CORSMiddleware) or m.cls == CORSMiddleware
    ]
    if middlewares:
        cors_middleware = middlewares[0]

        request_origin = request.headers.get('origin', '')
        if cors_middleware and '*' in cors_middleware.kwargs['allow_origins']:  # type: ignore
            response.headers['Access-Control-Allow-Origin'] = '*'
        elif (
            cors_middleware
            and request_origin in cors_middleware.kwargs['allow_origins']  # type: ignore
        ):
            response.headers['Access-Control-Allow-Origin'] = request_origin

    return response


# graphql
app.include_router(MetamistGraphQLRouter, prefix='/graphql', include_in_schema=False)

for route in routes.__dict__.values():
    if not isinstance(route, APIRouter):
        continue
    app.include_router(route, prefix='/api/v1')


if static_dir_exists:
    # only allow static files if the static files are available
    app.mount('/', SPAStaticFiles(directory=STATIC_DIR, html=True), name='static')

app.openapi = get_openapi_schema_func(app, _VERSION)  # type: ignore[assignment]


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
