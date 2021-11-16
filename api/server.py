import os
import time
import traceback

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from db.python.connect import SMConnections
from db.python.utils import get_logger

from api.routes import (
    sample_router,
    import_router,
    analysis_router,
    sequence_router,
    participant_router,
    family_router,
    project_router,
)
from api.utils import get_openapi_schema_func
from api.utils.exceptions import determine_code_from_error

# This tag is automatically updated by bump2version
_VERSION = '3.5.1'

logger = get_logger()

SKIP_DATABASE_CONNECTION = bool(os.getenv('SM_SKIP_DATABASE_CONNECTION'))
app = FastAPI()


class SPAStaticFiles(StaticFiles):
    """
    https://stackoverflow.com/a/68363904
    """

    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)
        if response.status_code == 404 and not path.startswith('api'):
            # server index.html if can't find existing resource
            response = await super().get_response('.', scope)
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


@app.exception_handler(Exception)
async def exception_handler(_: Request, e: Exception):
    """Generic exception handler"""
    add_stacktrace = True

    if isinstance(e, HTTPException):
        code = e.status_code
        name = e.detail

    else:
        code = determine_code_from_error(e)
        name = str(type(e).__name__)

    base_params = {'name': name, 'description': str(e)}

    if add_stacktrace:
        st = traceback.format_exc()
        base_params['stacktrace'] = st

    return JSONResponse(
        status_code=code,
        content=base_params,
    )


app.include_router(sample_router, prefix='/api/v1')
app.include_router(import_router, prefix='/api/v1')
app.include_router(analysis_router, prefix='/api/v1')
app.include_router(sequence_router, prefix='/api/v1')
app.include_router(participant_router, prefix='/api/v1')
app.include_router(family_router, prefix='/api/v1')
app.include_router(project_router, prefix='/api/v1')

static_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'public')
if os.path.exists(static_dir):
    # only allow static files if the static files are available
    app.mount('/', SPAStaticFiles(directory=static_dir, html=True), name='static')

app.openapi = get_openapi_schema_func(app, _VERSION)


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=int(os.getenv('PORT', '8000')), debug=True)
