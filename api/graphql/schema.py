import strawberry
from strawberry.types import Info

from api.utils import get_projectless_db_connection
from db.python.layers.analysis import AnalysisLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequence import SampleSequenceLayer
from models.enums import SampleType, SequenceType, AnalysisType, AnalysisStatus
from models.models.sample import sample_id_transform_to_raw


async def get_context(connection=get_projectless_db_connection):
    return {'connection': connection}


@strawberry.type
class Analysis:
    id: int
    type: strawberry.enum(AnalysisType)
    status: strawberry.enum(AnalysisStatus)
    output: str | None
    timestamp_completed: str | str = None
    active: bool
    meta: strawberry.scalars.JSON
    author: str

    project: int

    @strawberry.field
    async def samples(self, info: Info, root) -> list['Sample']:
        connection = info.context['connection']
        return await SampleLayer(connection).get_samples_by_analysis_id(root.id)


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

    @strawberry.field
    async def analyses_for_type(self, info: Info, root, analysis_type: strawberry.enum(AnalysisType)) -> list[Analysis]:
        connection = info.context['connection']
        alayer = AnalysisLayer(connection)
        return await alayer.get_latest_complete_analysis_for_samples_and_type(
            sample_ids=[sample_id_transform_to_raw(root.id)], analysis_type=analysis_type
        )


@strawberry.type
class SampleSequencing:
    id: int
    type: strawberry.enum(SequenceType)
    meta: strawberry.scalars.JSON
    external_ids: strawberry.scalars.JSON

    @strawberry.field
    async def sample(self, info: Info, root) -> 'Sample':
        connection = info.context['connection']
        return await SampleLayer(connection).get_sample_by_id(root.sample_id)


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

    @strawberry.field
    async def get_analyses_for_sample(
        self, info: Info, sample_id: str
    ) -> list[Analysis]:
        connection = info.context['connection']
        alayer = AnalysisLayer(connection)
        return await alayer.get_analysis_for_sample(
            sample_id_transform_to_raw(sample_id), map_sample_ids=False
        )


schema = strawberry.Schema(query=Query, mutation=None)
