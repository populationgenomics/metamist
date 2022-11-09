# pylint: disable=no-value-for-parameter,redefined-builtin,missing-function-docstring
# type: ignore
"""
Schema for GraphQL.

Note, we silence a lot of linting here because GraphQL looks at type annotations
and defaults to decide the GraphQL schema, so it might not necessarily look correct.
"""

import strawberry
from strawberry.dataloader import DataLoader
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info

from api.utils import get_projectless_db_connection, group_by
from db.python.layers.analysis import AnalysisLayer
from db.python.layers.family import FamilyLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequence import SampleSequenceLayer
from db.python.tables.project import ProjectPermissionsTable
from models.enums import SampleType, SequenceType, AnalysisType, AnalysisStatus
from models.models.sample import sample_id_transform_to_raw


def connected_data_loader(fn):
    """
    DataLoader Decorator for allowing DB connection to be bound to a loader
    """

    def inner(connection):
        async def wrapped(*args, **kwargs):
            return await fn(*args, **kwargs, connection=connection)

        return wrapped

    return inner


@connected_data_loader
async def load_sequences_for_samples(
    sample_ids: list[int], connection
) -> list[list['SampleSequencing']]:
    """
    DataLoader: get_sequences_for_sample_ids
    """
    seqlayer = SampleSequenceLayer(connection)
    sequences = await seqlayer.get_sequences_for_sample_ids(sample_ids=sample_ids)
    seq_map: dict[int, list['SampleSequencing']] = group_by(
        sequences, lambda sequence: int(sequence.sample_id)
    )

    return [seq_map.get(sid, []) for sid in sample_ids]


@connected_data_loader
async def load_samples_for_participant_ids(
    participant_ids: list[int], connection
) -> list[list['Sample']]:
    """
    DataLoader: get_samples_for_participant_ids
    """
    sample_map = await SampleLayer(connection).get_samples_by_participants(
        participant_ids
    )

    return [sample_map.get(pid, []) for pid in participant_ids]


@connected_data_loader
async def load_samples_for_ids(sample_ids: list[int], connection) -> list['Sample']:
    """
    DataLoader: get_samples_for_ids
    """
    samples = await SampleLayer(connection).get_samples_by(sample_ids=sample_ids)
    return samples


@connected_data_loader
async def load_participants_for_ids(
    participant_ids: list[int], connection
) -> list['Participant']:
    """
    DataLoader: get_participants_by_ids
    """
    player = ParticipantLayer(connection)
    return await player.get_participants_by_ids(participant_ids)


@connected_data_loader
async def load_samples_for_analysis_ids(
    analysis_ids: list[int], connection
) -> list['Sample']:
    """
    DataLoader: get_samples_for_analysis_ids
    """
    slayer = SampleLayer(connection)
    analysis_sample_map = await slayer.get_samples_by_analysis_ids(analysis_ids)

    return [analysis_sample_map.get(aid, []) for aid in analysis_ids]


@connected_data_loader
async def load_projects_for_ids(project_ids: list[int], connection) -> list['Project']:
    pttable = ProjectPermissionsTable(connection.connection)
    return await pttable.get_projects_by_ids(project_ids)


async def get_context(connection=get_projectless_db_connection):
    return {
        'connection': connection,
        'loader_samples_for_participants': DataLoader(
            load_samples_for_participant_ids(connection)
        ),
        'loader_samples_for_ids': DataLoader(load_samples_for_ids(connection)),
        'loader_sequences_for_samples': DataLoader(
            load_sequences_for_samples(connection)
        ),
        'loader_samples_for_analysis_ids': DataLoader(
            load_samples_for_analysis_ids(connection)
        ),
        'loader_participants_for_ids': DataLoader(
            load_participants_for_ids(connection)
        ),
        'loader_projects_for_ids': DataLoader(load_projects_for_ids(connection)),
    }


@strawberry.type
class Project:
    """Project GraphQL model"""

    id: int
    name: str
    dataset: str
    meta: strawberry.scalars.JSON
    read_secret_name: str | None = None
    write_secret_name: str | None = None

    @strawberry.field()
    async def pedigree(
        self,
        info: Info,
        root: 'Project',
        internal_family_ids: list[int] | None = None,
        replace_with_participant_external_ids: bool = True,
        replace_with_family_external_ids: bool = True,
        include_participants_not_in_families: bool = False,
        empty_participant_value: str | None = None,
    ) -> list[strawberry.scalars.JSON]:
        connection = info.context['connection']
        family_layer = FamilyLayer(connection)

        pedigree_dicts = await family_layer.get_pedigree(
            project=root.id,
            family_ids=internal_family_ids,
            replace_with_participant_external_ids=replace_with_participant_external_ids,
            replace_with_family_external_ids=replace_with_family_external_ids,
            empty_participant_value=empty_participant_value,
            include_participants_not_in_families=include_participants_not_in_families,
        )

        return pedigree_dicts

    @strawberry.field()
    async def participants(self, info: Info, root: 'Project') -> list['Participant']:
        connection = info.context['connection']
        participants = await ParticipantLayer(connection).get_participants(
            project=root.id
        )

        return participants


@strawberry.type
class Analysis:
    """Analysis GraphQL model"""

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
        loader = info.context['loader_samples_for_analysis_ids']
        return await loader.load(root.id)


@strawberry.type
class Participant:
    """Participant GraphQL model"""

    id: int
    external_id: str
    meta: strawberry.scalars.JSON

    @strawberry.field
    async def samples(self, info: Info, root) -> list['Sample']:
        return await info.context['loader_samples_for_participants'].load(root.id)


@strawberry.type
class Sample:
    """Sample GraphQL model"""

    id: str
    external_id: str
    active: bool
    meta: strawberry.scalars.JSON
    type: strawberry.enum(SampleType)
    author: str | None

    @strawberry.field
    async def participant(self, info: Info, root: 'Sample') -> Participant:
        loader_participants_for_ids = info.context['loader_participants_for_ids']
        return await loader_participants_for_ids.load(root.participant_id)

    @strawberry.field
    async def sequences(self, info: Info, root) -> list['SampleSequencing']:
        loader_sequences_for_samples = info.context['loader_sequences_for_samples']
        return await loader_sequences_for_samples.load(
            sample_id_transform_to_raw(root.id)
        )

    @strawberry.field
    async def project(self, info: Info, root) -> Project:
        return await info.context['loader_projects_for_ids'].load(root.project)

    @strawberry.field
    async def analyses(
        self,
        info: Info,
        root: 'Sample',
        type: strawberry.enum(AnalysisType) | None = None,
        status: strawberry.enum(AnalysisStatus) | None = None,
    ) -> list[Analysis]:
        connection = info.context['connection']
        alayer = AnalysisLayer(connection)
        analyses = await alayer.get_analysis_for_sample(
            sample_id_transform_to_raw(root.id),
            analysis_type=type,
            status=status,
            map_sample_ids=False,
        )

        return analyses


@strawberry.type
class SampleSequencing:
    """SampleSequencing GraphQL model"""

    id: int
    type: strawberry.enum(SequenceType)
    meta: strawberry.scalars.JSON
    external_ids: strawberry.scalars.JSON

    @strawberry.field
    async def sample(self, info: Info, root) -> 'Sample':
        loader = info.context['loader_samples_for_ids']
        return await loader.load(root.sample_id)


@strawberry.type
class Query:
    """GraphQL Queries"""

    @strawberry.field
    async def sample(self, info: Info, id: str) -> Sample:
        loader = info.context['loader_samples_for_ids']
        return await loader.load(sample_id_transform_to_raw(id))

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

    @strawberry.field()
    async def project(self, info: Info, name: str) -> Project:
        connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection.connection)
        project_id = await ptable.get_project_id_from_name_and_user(
            user=connection.author, project_name=name, readonly=True
        )
        presponse = await ptable.get_projects_by_ids([project_id])
        return presponse[0]

    @strawberry.field
    async def my_projects(self, info: Info) -> list[Project]:
        connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection.connection)
        project_map = await ptable.get_projects_accessible_by_user(
            connection.author, readonly=True
        )
        return await ptable.get_projects_by_ids(list(project_map.keys()))


schema = strawberry.Schema(query=Query, mutation=None)
MetamistGraphQLRouter = GraphQLRouter(schema, graphiql=True, context_getter=get_context)
