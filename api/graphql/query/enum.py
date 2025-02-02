from inspect import isclass

import strawberry

from api.graphql.context import GraphQLContext
from db.python import enum_tables

enum_methods = {}
for enum in enum_tables.__dict__.values():
    if not isclass(enum):
        continue

    def create_function(_enum):
        async def m(info: strawberry.Info[GraphQLContext, None]) -> list[str]:
            return await _enum(info.context.connection).get()

        m.__name__ = _enum.get_enum_name()
        # m.__annotations__ = {'return': list[str]}
        return m

    enum_methods[enum.get_enum_name()] = strawberry.field(
        create_function(enum),
    )


GraphQLEnum = strawberry.type(type('GraphQLEnum', (object,), enum_methods))
