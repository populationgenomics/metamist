from typing import Optional
import time

from fastapi import (
    FastAPI,
    HTTPException,
    Request,
    Depends,
)
from databases import Database

from api2.routes.sample import router as sample_router
from db.python.connect import SMConnections

from .utils.db_dependency import authenticate


app = FastAPI(dependencies=[Depends(authenticate)])


@app.on_event('startup')
async def startup():
    """Server is starting up, connect dbs"""
    await SMConnections.connect()


@app.on_event('shutdown')
async def shutdown():
    """Shutdown server, disconnect dbs"""
    await SMConnections.disconnect()


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """Add X-Process-Time to all requests for logging"""
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


# @app.middleware("http")
# async def set_connection(request: Request, call_next):
#     print(request)
#     project = request.path_params.get('project')
#     if 'Authorization' in request.headers:
#         token = HTTPAuthorizationCredentials(
#             scheme="Bearer",
#             credentials=request.headers['Authorization'][7:],
#         )
#         author = authenticate(token)

#     if project is not None and author is not None:
#         request.state.author = author
#         request.state.db = SMConnections.get_connection_for_project(project, author)

#     response = await call_next(request)
#     return response


@app.get("/")
async def root():

    return {"message": "Hello World"}


app.include_router(sample_router, prefix="/api/v1/{project}")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
