from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from db.python.layers.participant import ParticipantLayer

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment


@strawberry.type
class ParticipantMutations:
    """Participant mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: int,
        info: Info[GraphQLContext, 'ParticipantMutations'],
    ) -> Annotated['GraphQLComment', strawberry.lazy('api.graphql.schema')]:
        """Add a comment to a participant"""
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
        participant_layer = ParticipantLayer(connection)
        result = await participant_layer.add_comment_to_participant(
            content=content, participant_id=id
        )
        return GraphQLComment.from_internal(result)
