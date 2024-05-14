import os
import time
import traceback

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import ValidationError
from starlette.responses import FileResponse

from api import routes
from api.graphql.schema import MetamistGraphQLRouter  # type: ignore
from api.settings import PROFILE_REQUESTS, SKIP_DATABASE_CONNECTION
from api.utils import get_openapi_schema_func
from api.utils.exceptions import determine_code_from_error
from db.python.connect import SMConnections
from db.python.tables.project import is_all_access
from db.python.utils import get_logger

# This tag is automatically updated by bump2version
_VERSION = '6.10.2'


logger = get_logger()

STATIC_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'public')

static_dir_exists = os.path.exists(STATIC_DIR)

app = FastAPI()

if PROFILE_REQUESTS:
    from fastapi_profiler.profiler import PyInstrumentProfilerMiddleware

    app.add_middleware(PyInstrumentProfilerMiddleware)

if is_all_access():
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
        if response.status_code == 404 and not path.startswith('api'):
            # server index.html if can't find existing resource
            response = await super().get_response('index.html', scope)
        return response


@app.on_event('startup')
async def startup():
    """Server is starting up, connect dbs"""
    if not SKIP_DATABASE_CONNECTION:
        await SMConnections.connect()


@app.on_event('shutdown')
async def shutdown():
    """Shutdown server, disconnect dbs"""
    if not SKIP_DATABASE_CONNECTION:
        await SMConnections.disconnect()


@app.middleware('http')
async def add_process_time_header(request: Request, call_next):
    """Add X-Process-Time to all requests for logging"""
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers['X-Process-Time'] = f'{round(process_time * 1000, 1)}ms'
    return response


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
        if cors_middleware and '*' in cors_middleware.options['allow_origins']:  # type: ignore
            response.headers['Access-Control-Allow-Origin'] = '*'
        elif (
            cors_middleware
            and request_origin in cors_middleware.options['allow_origins']  # type: ignore
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
        # debug=True,
        reload=True,
    )
