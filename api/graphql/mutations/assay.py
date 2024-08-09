from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from db.python.layers.assay import AssayLayer

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment


@strawberry.type
class AssayMutations:
    """Assay mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: int,
        info: Info[GraphQLContext, 'AssayMutations'],
    ) -> Annotated['GraphQLComment', strawberry.lazy('api.graphql.schema')]:
        """Add a comment to a assay"""
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
        assay_layer = AssayLayer(connection)
        result = await assay_layer.add_comment_to_assay(content=content, assay_id=id)
        return GraphQLComment.from_internal(result)
