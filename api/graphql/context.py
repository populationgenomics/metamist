from typing import Any

from fastapi import Request
from strawberry.dataloader import DataLoader
from strawberry.fastapi import BaseContext

from api.graphql.utils.loaders import loaders
from api.utils.db import get_projectless_db_connection
from db.python.connect import Connection


class GraphQLContext(BaseContext):
    """Basic dict type for GraphQL context to be passed to resolvers"""

    def __init__(self, connection: Connection | None, loaders: dict[str, Any]):
        super().__init__()
        self.connection: Connection | None = connection
        self.loaders: dict[str, Any] = {
            k: DataLoader(v, cache=False) for k, v in loaders.items()
        }


async def get_context(
    request: Request,  # pylint: disable=unused-argument
    connection: Connection = get_projectless_db_connection,
) -> GraphQLContext:
    """Get loaders / cache context for strawberyy GraphQL"""
    mapped_loaders = {k: fn(connection) for k, fn in loaders.items()}
    return GraphQLContext(
        connection=connection,
        loaders=mapped_loaders,
    )
