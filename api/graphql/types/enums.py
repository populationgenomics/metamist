from enum import EnumType
from inspect import isclass
from typing import TYPE_CHECKING

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from db.python import enum_tables
from models.enums import AnalysisStatus

if TYPE_CHECKING:
    from api.graphql.schema import Query


# Create the class first, without fields
@strawberry.type
class GraphQLEnum:
    """Dynamically populated enum fields will be added to this class."""

    # Example field to demonstrate the structure
    example: str = 'example'


# Dynamically add strawberry fields to the GraphQLEnum class
for enum in enum_tables.__dict__.values():
    if not isclass(enum):
        continue

    def create_function(_enum):
        """Create a function that retrieves the enum values."""

        async def m(info: Info[GraphQLContext, 'Query']) -> list[str]:
            return await _enum(info.context['connection']).get()

        m.__name__ = _enum.get_enum_name()
        return m

    field_name = enum.get_enum_name()
    resolver = create_function(enum)
    setattr(GraphQLEnum, field_name, strawberry.field(resolver))


GraphQLAnalysisStatus: EnumType = strawberry.enum(AnalysisStatus)
