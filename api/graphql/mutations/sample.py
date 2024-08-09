from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from db.python.layers.sample import SampleLayer
from models.utils.sample_id_format import sample_id_transform_to_raw

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment


@strawberry.type
class SampleMutations:
    """Sample mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: str,
        info: Info[GraphQLContext, 'SampleMutations'],
    ) -> Annotated['GraphQLComment', strawberry.lazy('api.graphql.schema')]:
        """Add a comment to a sample"""
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
        sample_layer = SampleLayer(connection)
        result = await sample_layer.add_comment_to_sample(
            content=content, sample_id=sample_id_transform_to_raw(id)
        )
        return GraphQLComment.from_internal(result)
