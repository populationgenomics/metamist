import os
from functools import wraps
from io import BytesIO
from typing import Awaitable, Callable, ParamSpec

from fastapi import APIRouter, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel


class TableError(BaseModel):
    """Simple error model for table routes"""

    error: str


P = ParamSpec('P')


def parquet_table_route(router: APIRouter, path: str, operation_id: str):
    """
    Decorator for routes that return a parquet table, this is intended to reduce
    a bunch of boilerplate around setting the response headers, handling range requests
    and handling 404s when there's no data for a table
    """

    def decorator(func: Callable[P, Awaitable[BytesIO | None]]):
        @router.api_route(
            path,
            methods=['GET'],
            operation_id=operation_id,
            responses={
                200: {'content': {'application/vnd.apache.parquet': {}}},
                404: {'model': TableError},
            },
            response_class=Response,
        )
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> Response:
            table_bytes = await func(*args, **kwargs)

            if table_bytes is None:
                return JSONResponse(
                    status_code=404,
                    content=TableError(error='Table empty or not found'),
                )

            filename = os.path.basename(path)

            headers = {'Content-Disposition': f'attachment; filename="{filename}"'}

            table_bytes.seek(0)
            result = table_bytes.getvalue()

            return Response(
                content=result,
                media_type='application/vnd.apache.parquet',
                headers=headers,
            )

        return wrapper

    return decorator
