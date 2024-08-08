import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from db.python.layers.participant import ParticipantLayer


@strawberry.type
class ParticipantMutations:
    """Participant mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: int,
        info: Info[GraphQLContext, 'ParticipantMutations'],
    ) -> int:
        """Add a comment to a participant"""
        connection = info.context['connection']
        participant_layer = ParticipantLayer(connection)
        result = await participant_layer.add_comment_to_participant(
            content=content, participant_id=id
        )
        return result
