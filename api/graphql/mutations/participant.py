from typing import TYPE_CHECKING

import strawberry
from strawberry.types import Info
from typing_extensions import Annotated

from api.graphql.types import (
    CustomJSON,
    ParticipantUpsertInput,
    ParticipantUpsertType,
    QueryParticipantCriteria,
    UpdateParticipantFamilyType,
)
from db.python.layers.participant import ParticipantLayer
from models.models.participant import ParticipantUpsertInternal

if TYPE_CHECKING:
    from ..schema import GraphQLParticipant


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
        connection = info.context['connection']
        participant_layer = ParticipantLayer(connection)

        return CustomJSON(
            {'success': await participant_layer.fill_in_missing_participants()}
        )

    @strawberry.mutation
    async def get_id_map_by_external_ids(
        self,
        info: Info,
        external_participant_ids: list[str],
        allow_missing: bool = False,
    ) -> CustomJSON:
        """Get ID map of participants, by external_id"""
        # TODO: Reconfigure connection permissions as per `routes``
        connection = info.context['connection']
        player = ParticipantLayer(connection)
        return CustomJSON(
            await player.get_id_map_by_external_ids(
                external_participant_ids,
                allow_missing=allow_missing,
                project=connection.project,
            )
        )

    @strawberry.mutation
    async def update_many_participant_external_ids(
        self,
        internal_to_external_id: strawberry.scalars.JSON,
        info: Info,
    ) -> bool:
        """Update external_ids of participants by providing an update map"""
        # TODO: Reconfigure connection permissions as per `routes``
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
        # TODO: Reconfigure connection permissions as per `routes``
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
        # TODO: Reconfigure connection permissions as per `routes`
        connection = info.context['connection']
        pt = ParticipantLayer(connection)
        results = await pt.upsert_participants(
            [
                ParticipantUpsertInternal.from_dict(strawberry.asdict(p))
                for p in participants
            ]
        )
        return [ParticipantUpsertType.from_upsert_internal(p) for p in results]

    @strawberry.mutation
    async def get_participants(
        self,
        criteria: QueryParticipantCriteria,
        info: Info,
    ) -> list[Annotated['GraphQLParticipant', strawberry.lazy('..schema')]]:
        """Get participants, default ALL participants in project"""
        # TODO: Reconfigure connection permissions as per `routes`
        connection = info.context['connection']
        player = ParticipantLayer(connection)
        participants = await player.get_participants(
            project=connection.project,
            external_participant_ids=(
                criteria.external_participant_ids if criteria else None
            ),
            internal_participant_ids=(
                criteria.internal_participant_ids if criteria else None
            ),
        )
        #  pylint: disable=no-member
        return [Annotated['GraphQLParticipant', strawberry.lazy('..schema')].from_internal(p) for p in participants]  # type: ignore[attr-defined]

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
        # TODO: Reconfigure connection permissions as per `routes`
        connection = info.context['connection']
        player = ParticipantLayer(connection)

        return UpdateParticipantFamilyType.from_tuple(
            await player.update_participant_family(
                participant_id=participant_id,
                old_family_id=old_family_id,
                new_family_id=new_family_id,
            )
        )
