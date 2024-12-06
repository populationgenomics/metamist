from functools import wraps
from io import BytesIO
from typing import Awaitable, Callable, ParamSpec, Tuple

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from api.utils.range_request_handler import handle_range_request


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

    def decorator(func: Callable[P, Awaitable[Tuple[Request, BytesIO | None]]]):
        @router.api_route(
            path,
            methods=['GET', 'HEAD'],
            operation_id=operation_id,
            responses={
                200: {'content': {'application/vnd.apache.parquet': {}}},
                404: {'model': TableError},
            },
            response_class=Response,
        )
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> Response:
            (request, table_bytes) = await func(*args, **kwargs)

            if table_bytes is None:
                return JSONResponse(
                    status_code=404,
                    content={'error': 'Table empty or not found'},
                )

            headers = {
                'Content-Disposition': 'attachment; filename="participant_table.parquet"'
            }

            range_header = request.headers.get('range', None)
            result = handle_range_request(range_header, table_bytes)

            return Response(
                content=result,
                media_type='application/vnd.apache.parquet',
                headers=headers,
            )

        return wrapper

    return decorator
