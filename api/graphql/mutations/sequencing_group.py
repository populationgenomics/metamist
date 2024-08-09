from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from db.python.layers.sequencing_group import SequencingGroupLayer
from models.utils.sequencing_group_id_format import sequencing_group_id_transform_to_raw

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment


@strawberry.type
class SequencingGroupMutations:
    """Sequencing Group Mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: str,
        info: Info[GraphQLContext, 'SequencingGroupMutations'],
    ) -> Annotated['GraphQLComment', strawberry.lazy('api.graphql.schema')]:
        """Add a comment to a sequencing group"""
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
        sequencing_group_layer = SequencingGroupLayer(connection)
        result = await sequencing_group_layer.add_comment_to_sequencing_group(
            content=content,
            sequencing_group_id=sequencing_group_id_transform_to_raw(id),
        )
        return GraphQLComment.from_internal(result)
