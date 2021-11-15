from os import getenv
import time
import traceback

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

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

SKIP_DATABASE_CONNECTION = bool(getenv('SM_SKIP_DATABASE_CONNECTION'))
app = FastAPI()


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

app.openapi = get_openapi_schema_func(app, _VERSION)


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=int(getenv('PORT', '8000')), debug=True)
