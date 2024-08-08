import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from db.python.layers.assay import AssayLayer


@strawberry.type
class AssayMutations:
    """Assay mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: int,
        info: Info[GraphQLContext, 'AssayMutations'],
    ) -> int:
        """Add a comment to a assay"""
        connection = info.context['connection']
        assay_layer = AssayLayer(connection)
        result = await assay_layer.add_comment_to_assay(content=content, assay_id=id)
        return result
