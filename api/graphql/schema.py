# pylint: disable=redefined-builtin
import strawberry
from strawberry.types import Info

from api.utils import get_projectless_db_connection
from db.python.layers.analysis import AnalysisLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequence import SampleSequenceLayer

from models.enums import (
    SampleType,
    SequenceType,
    AnalysisType,
    AnalysisStatus,
)
from models.models.sample import sample_id_transform_to_raw


async def get_context(connection=get_projectless_db_connection):
    """Connection context"""
    return {'connection': connection}


@strawberry.type
class Analysis:
    """Graphql Analysis Data Type"""

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
        """Get related samples from this analysis"""
        connection = info.context['connection']
        return await SampleLayer(connection).get_samples_by_analysis_id(root.id)


@strawberry.type
class Participant:
    """Graphql Participant Data Type"""

    id: int
    external_id: str
    meta: strawberry.scalars.JSON

    @strawberry.field
    async def samples(self, info: Info, root) -> list['Sample']:
        """Get related samples from this participant"""
        connection = info.context['connection']
        return await SampleLayer(connection).get_samples_by_participant(root.id)


@strawberry.type
class Sample:
    """Graphql Sample Data Type"""

    id: str
    external_id: str
    active: bool
    meta: strawberry.scalars.JSON
    type: SampleType
    author: str | None

    @strawberry.field
    async def participant(self, info: Info, root) -> Participant:
        """Get related participatn from this sample"""
        connection = info.context['connection']
        player = ParticipantLayer(connection)
        return (await player.get_participants_by_ids([root.participant_id]))[0]

    @strawberry.field
    async def sequences(self, info: Info, root) -> list['SampleSequencing']:
        """Get related sequences from this sample"""
        connection = info.context['connection']
        seqlayer = SampleSequenceLayer(connection)

        return await seqlayer.get_sequences_for_sample_ids(
            [sample_id_transform_to_raw(root.id)]
        )

    @strawberry.field
    async def analyses_for_type(
        self, info: Info, root, analysis_type: strawberry.enum(AnalysisType)
    ) -> list[Analysis]:
        """Get related analyses by type from this sample"""
        connection = info.context['connection']
        alayer = AnalysisLayer(connection)
        return await alayer.get_latest_complete_analysis_for_samples_and_type(
            sample_ids=[sample_id_transform_to_raw(root.id)],
            analysis_type=analysis_type,
        )


@strawberry.type
class SampleSequencing:
    """Graphql SampleSequencing Data Type"""

    id: int
    type: strawberry.enum(SequenceType)
    meta: strawberry.scalars.JSON
    external_ids: strawberry.scalars.JSON

    @strawberry.field
    async def sample(self, info: Info, root) -> 'Sample':
        """Get related samples from this sequence"""
        connection = info.context['connection']
        return await SampleLayer(connection).get_sample_by_id(root.sample_id)


@strawberry.type
class Query:
    """Graphql Query Model"""

    @strawberry.field
    async def sample(self, info: Info, id: str) -> Sample:
        """Get sample"""
        connection = info.context['connection']
        slayer = SampleLayer(connection)
        return await slayer.get_sample_by_id(sample_id_transform_to_raw(id))

    @strawberry.field
    async def sample_sequence(self, info: Info, id: int) -> SampleSequencing:
        """Get sequence"""
        connection = info.context['connection']
        slayer = SampleSequenceLayer(connection)
        return await slayer.get_sequence_by_id(id)

    @strawberry.field
    async def participant(self, info: Info, id: int) -> Participant:
        """Get participant"""
        connection = info.context['connection']
        player = ParticipantLayer(connection)
        ps = await player.get_participants_by_ids([id])
        return ps[0]

    @strawberry.field
    async def get_analyses_for_sample(
        self, info: Info, sample_id: str
    ) -> list[Analysis]:
        """Get analyses for a sample"""
        connection = info.context['connection']
        alayer = AnalysisLayer(connection)
        return await alayer.get_analysis_for_sample(
            sample_id_transform_to_raw(sample_id), map_sample_ids=False
        )


schema = strawberry.Schema(query=Query, mutation=None)
