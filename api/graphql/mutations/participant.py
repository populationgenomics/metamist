# pylint: disable=redefined-builtin, import-outside-toplevel

from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from api.graphql.mutations.sample import SampleUpsertInput
from db.python.connect import Connection
from db.python.layers.comment import CommentLayer
from db.python.layers.participant import ParticipantLayer
from models.models.comment import CommentEntityType
from models.models.participant import ParticipantUpsert
from models.models.project import FullWriteAccessRoles

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment, GraphQLParticipant


@strawberry.type
class UpdateParticipantFamilyType:
    """Update participant family type"""

    family_id: int
    participant_id: int

    @staticmethod
    def from_tuple(t: tuple[int, int]) -> 'UpdateParticipantFamilyType':
        """Returns graphql model from tuple"""
        return UpdateParticipantFamilyType(family_id=t[0], participant_id=t[1])


@strawberry.input
class ParticipantUpsertInput:
    """Participant upsert input"""

    id: int | None = None
    external_ids: strawberry.scalars.JSON | None = None
    reported_sex: int | None = None
    reported_gender: str | None = None
    karyotype: str | None = None
    meta: strawberry.scalars.JSON | None = None

    samples: list[SampleUpsertInput] | None = None


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
        # Import needed here to avoid circular import
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
        cl = CommentLayer(connection)
        result = await cl.add_comment_to_entity(
            entity=CommentEntityType.participant, entity_id=id, content=content
        )
        return GraphQLComment.from_internal(result)

    @strawberry.mutation
    async def update_participant(
        self,
        participant_id: int,
        participant: ParticipantUpsertInput,
        info: Info,
    ) -> Annotated['GraphQLParticipant', strawberry.lazy('api.graphql.schema')]:
        """Update Participant Data"""
        from api.graphql.schema import GraphQLParticipant

        connection = info.context['connection']
        player = ParticipantLayer(connection)

        participant.id = participant_id

        upserted = await player.upsert_participant(
            ParticipantUpsert.from_dict(strawberry.asdict(participant)).to_internal()
        )
        updated_participant = (await player.get_participants_by_ids([upserted.id]))[0]  # type: ignore [attr-defined]

        return GraphQLParticipant.from_internal(updated_participant)

    @strawberry.mutation
    async def upsert_participants(
        self,
        participants: list[ParticipantUpsertInput],
        info: Info,
    ) -> list[Annotated['GraphQLParticipant', strawberry.lazy('api.graphql.schema')]]:
        """
        Upserts a list of participants with samples and sequences
        Returns the list of internal sample IDs
        """
        from api.graphql.schema import GraphQLParticipant

        connection: Connection = info.context['connection']
        connection.check_access(FullWriteAccessRoles)

        pt = ParticipantLayer(connection)
        results = await pt.upsert_participants(
            [
                ParticipantUpsert.from_dict(strawberry.asdict(p)).to_internal()
                for p in participants
            ]
        )
        updated_participants = await pt.get_participants_by_ids(
            [p.id for p in results]  # type: ignore [arg-type]
        )
        return [GraphQLParticipant.from_internal(p) for p in updated_participants]

    @strawberry.mutation
    async def update_participant_family(
        self,
        participant_id: int,
        old_family_id: int,
        new_family_id: int,
        info: Info,
    ) -> UpdateParticipantFamilyType:
        """
        Change a participants family from old_family_id
        to new_family_id, maintaining all other fields.
        The new_family_id must already exist.
        """
        connection = info.context['connection']
        player = ParticipantLayer(connection)

        return UpdateParticipantFamilyType.from_tuple(
            await player.update_participant_family(
                participant_id=participant_id,
                old_family_id=old_family_id,
                new_family_id=new_family_id,
            )
        )
