import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from db.python.layers.sequencing_group import SequencingGroupLayer
from models.utils.sequencing_group_id_format import sequencing_group_id_transform_to_raw


@strawberry.type
class SequencingGroupMutations:
    """Sequencing Group Mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: str,
        info: Info[GraphQLContext, 'SequencingGroupMutations'],
    ) -> int:
        """Add a comment to a sequencing group"""
        connection = info.context['connection']
        sequencing_group_layer = SequencingGroupLayer(connection)
        result = await sequencing_group_layer.add_comment_to_sequencing_group(
            content=content,
            sequencing_group_id=sequencing_group_id_transform_to_raw(id),
        )
        return result
