from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from db.python.layers.family import FamilyLayer

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment


@strawberry.type
class FamilyMutations:
    """Family mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: int,
        info: Info[GraphQLContext, 'FamilyMutations'],
    ) -> Annotated['GraphQLComment', strawberry.lazy('api.graphql.schema')]:
        """Add a comment to a family"""
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
        family_layer = FamilyLayer(connection)
        result = await family_layer.add_comment_to_family(content=content, family_id=id)
        return GraphQLComment.from_internal(result)
