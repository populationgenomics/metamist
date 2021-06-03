import time

from fastapi import FastAPI, Request

from db.python.connect import SMConnections

from api2.routes.sample import router as sample_router
from api2.routes.imports import router as import_router
from api2.utils import IS_PRODUCTION, get_openapi_schema_func
from api2.utils.gcp import setup_gcp_logging

# This tag is automatically updated by bump2version
_VERSION = '1.0.0'

setup_gcp_logging(IS_PRODUCTION)


app = FastAPI()


@app.on_event('startup')
async def startup():
    """Server is starting up, connect dbs"""
    await SMConnections.connect()


@app.on_event('shutdown')
async def shutdown():
    """Shutdown server, disconnect dbs"""
    await SMConnections.disconnect()


@app.middleware('http')
async def add_process_time_header(request: Request, call_next):
    """Add X-Process-Time to all requests for logging"""
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers['X-Process-Time'] = str(round(process_time * 1000, 1))
    return response


app.include_router(sample_router, prefix='/api/v1/{project}')
app.include_router(import_router, prefix='/api/v1/{project}')

app.openapi = get_openapi_schema_func(app, _VERSION, is_production=IS_PRODUCTION)


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=8000)
