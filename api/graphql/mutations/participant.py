import strawberry
from strawberry.types import Info

from api.graphql.types import (
    CustomJSON,
    ParticipantUpsertInput,
    ParticipantUpsertType,
    UpdateParticipantFamilyType,
)
from db.python.connect import Connection
from db.python.layers.participant import ParticipantLayer
from models.models.participant import ParticipantUpsertInternal
from models.models.project import FullWriteAccessRoles


@strawberry.type
class ParticipantMutations:
    """Participant mutations"""

    @strawberry.mutation
    async def fill_in_missing_participants(
        self,
        info: Info,
    ) -> CustomJSON:
        """
        Create a corresponding participant (if required)
        for each sample within a project, useful for then importing a pedigree
        """
        # TODO: Reconfigure connection permissions as per `routes``
        connection: Connection = info.context['connection']
        connection.check_access(FullWriteAccessRoles)
        participant_layer = ParticipantLayer(connection)

        return CustomJSON(
            {'success': await participant_layer.fill_in_missing_participants()}
        )

    @strawberry.mutation
    async def update_many_participant_external_ids(
        self,
        internal_to_external_id: strawberry.scalars.JSON,
        info: Info,
    ) -> bool:
        """Update external_ids of participants by providing an update map"""
        connection = info.context['connection']
        player = ParticipantLayer(connection)
        return await player.update_many_participant_external_ids(
            internal_to_external_id
        )

    @strawberry.mutation
    async def update_participant(
        self,
        participant_id: int,
        participant: ParticipantUpsertInput,
        info: Info,
    ) -> ParticipantUpsertType:
        """Update Participant Data"""
        connection = info.context['connection']
        participant_layer = ParticipantLayer(connection)

        participant.id = participant_id

        updated_participant = await participant_layer.upsert_participant(
            ParticipantUpsertInternal.from_dict(strawberry.asdict(participant))
        )

        return ParticipantUpsertType.from_upsert_internal(updated_participant)

    @strawberry.mutation
    async def upsert_participants(
        self,
        participants: list[ParticipantUpsertInput],
        info: Info,
    ) -> list[ParticipantUpsertType]:
        """
        Upserts a list of participants with samples and sequences
        Returns the list of internal sample IDs
        """
        # TODO Reconfigure connection permissions as per `routes`
        connection: Connection = info.context['connection']
        connection.check_access(FullWriteAccessRoles)
        pt = ParticipantLayer(connection)
        results = await pt.upsert_participants(
            [
                ParticipantUpsertInternal.from_dict(strawberry.asdict(p))
                for p in participants
            ]
        )
        return [ParticipantUpsertType.from_upsert_internal(p) for p in results]

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
