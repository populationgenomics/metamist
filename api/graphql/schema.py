import strawberry
from strawberry.types import Info

from api.utils import get_projectless_db_connection
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequence import SampleSequenceLayer
from models.enums import SampleType
from models.models.sample import sample_id_transform_to_raw


async def get_context(connection=get_projectless_db_connection):
    return {'connection': connection}

@strawberry.type
class Participant:
    id: int
    external_id: str
    meta: strawberry.scalars.JSON

    @strawberry.field
    async def samples(self, info: Info, root) -> list['Sample']:
        connection = info.context['connection']
        return await SampleLayer(connection).get_samples_by_participant(root.id)


@strawberry.type
class Sample:
    id: str
    external_id: str
    active: bool
    meta: strawberry.scalars.JSON
    type: strawberry.enum(SampleType)
    author: str | None

    @strawberry.field
    async def participant(self, info: Info, root) -> Participant:
        connection = info.context['connection']
        player = ParticipantLayer(connection)
        return (await player.get_participants_by_ids([root.participant_id]))[0]

    @strawberry.field
    async def sequences(self, info: Info, root) -> list['SampleSequencing']:
        connection = info.context['connection']
        seqlayer = SampleSequenceLayer(connection)

        return await seqlayer.get_sequences_for_sample_ids(
            [sample_id_transform_to_raw(root.id)]
        )

@strawberry.type
class SampleSequencing:
    id: int
    meta: strawberry.scalars.JSON
    external_ids: strawberry.scalars.JSON


@strawberry.type
class Query:
    @strawberry.field
    async def sample(self, info: Info, id: str) -> Sample:
        connection = info.context['connection']
        slayer = SampleLayer(connection)
        return await slayer.get_sample_by_id(sample_id_transform_to_raw(id))

    @strawberry.field
    async def sample_sequence(self, info: Info, id: int) -> SampleSequencing:
        connection = info.context['connection']
        slayer = SampleSequenceLayer(connection)
        return await slayer.get_sequence_by_id(id)

    @strawberry.field
    async def participant(self, info: Info, id: int) -> Participant:
        connection = info.context['connection']
        player = ParticipantLayer(connection)
        ps = await player.get_participants_by_ids([id])
        return ps[0]


schema = strawberry.Schema(query=Query, mutation=None)