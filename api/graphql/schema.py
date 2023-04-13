# type: ignore
# flake8: noqa
# pylint: disable=no-value-for-parameter,redefined-builtin,missing-function-docstring,unused-argument
"""
Schema for GraphQL.

Note, we silence a lot of linting here because GraphQL looks at type annotations
and defaults to decide the GraphQL schema, so it might not necessarily look correct.
"""

import strawberry
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info

from db.python.layers.family import FamilyLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.assay import AssayLayer
from db.python.tables.project import ProjectPermissionsTable
from models.enums import AnalysisStatus
from models.models.sample import sample_id_transform_to_raw
from models.utils.sequencing_group_id_format import sequencing_group_id_transform_to_raw
from api.graphql.loaders import (
    get_context,
LoaderKeys,
)



@strawberry.type
class GraphQLProject:
    """Project GraphQL model"""

    id: int
    name: str
    dataset: str
    meta: strawberry.scalars.JSON
    read_group_name: str | None = None
    write_group_name: str | None = None

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
    async def family(
        self, info: Info, root: 'Project', external_id: str
    ) -> 'GraphQLFamily':
        connection = info.context['connection']
        connection.project = root.id
        return await FamilyLayer(connection).get_family_by_external_id(external_id)

    @strawberry.field()
    async def families(self, info: Info, root: 'Project') -> list['GraphQLFamily']:
        connection = info.context['connection']
        participants = await FamilyLayer(connection).get_families(project=root.id)
        return participants

    @strawberry.field()
    async def participants(
        self, info: Info, root: 'Project'
    ) -> list['GraphQLParticipant']:
        connection = info.context['connection']
        participants = await ParticipantLayer(connection).get_participants(
            project=root.id
        )

        return participants

    @strawberry.field()
    async def sequencing_groups(self, info: Info, root: 'Project') -> list['GraphQLSequencingGroup']:
        loader = info.context[LoaderKeys.SEQUENCING_GROUPS_FOR_PROJECTS]
        return await loader.load(root.id)

    # @strawberry.field()
    # async def samples(self, info: Info, root: 'Project') -> list['GraphQLSample']:
    #     return []


@strawberry.type
class GraphQLAnalysis:
    """Analysis GraphQL model"""

    id: int
    type: str
    status: strawberry.enum(AnalysisStatus)
    output: str | None
    timestamp_completed: str | None = None
    active: bool
    meta: strawberry.scalars.JSON
    author: str

    @strawberry.field
    async def sequencing_groups(self, info: Info, root) -> list['GraphQLSample']:
        loader = info.context[LoaderKeys.SEQUENCING_GROUPS_FOR_ANALYSIS]
        return await loader.load(root.id)

    @strawberry.field
    async def project(self, info: Info, root: 'Analysis') -> GraphQLProject:
        loader = info.context[LoaderKeys.PROJECTS_FOR_IDS]
        return await loader.load(root.project)


@strawberry.type
class GraphQLFamily:
    """GraphQL Family"""

    id: int
    external_id: str

    description: str | None = None
    coded_phenotype: str | None = None

    @strawberry.field
    async def project(self, info: Info, root: 'Family') -> GraphQLProject:
        loader = info.context[LoaderKeys.PROJECTS_FOR_IDS]
        return await loader.load(root.project)

    @strawberry.field
    async def participants(
        self, info: Info, root: 'Family'
    ) -> list['GraphQLParticipant']:
        return await info.context[LoaderKeys.PARTICIPANTS_FOR_FAMILIES].load(root.id)


@strawberry.type
class GraphQLParticipant:
    """Participant GraphQL model"""

    id: int
    external_id: str
    meta: strawberry.scalars.JSON

    @strawberry.field
    async def samples(self, info: Info, root) -> list['GraphQLSample']:
        return await info.context[LoaderKeys.SAMPLES_FOR_PARTICIPANTS].load(root.id)

    @strawberry.field
    async def families(self, info: Info, root) -> list[GraphQLFamily]:
        fams = await info.context[LoaderKeys.FAMILIES_FOR_PARTICIPANTS].load(root.id)
        return fams

    @strawberry.field
    async def project(self, info: Info, root: 'Participant') -> GraphQLProject:
        loader = info.context[LoaderKeys.PROJECTS_FOR_IDS]
        return await loader.load(root.project)


@strawberry.type
class GraphQLSample:
    """Sample GraphQL model"""

    id: str
    external_id: str
    active: bool
    meta: strawberry.scalars.JSON
    type: str
    author: str | None
    participant_id: int

    @strawberry.field
    async def participant(
        self, info: Info, root: 'Sample'
    ) -> GraphQLParticipant | None:
        loader_participants_for_ids = info.context[LoaderKeys.PARTICIPANTS_FOR_IDS]
        if root.participant_id is None:
            return None
        return await loader_participants_for_ids.load(root.participant_id)

    @strawberry.field
    async def assays(self, info: Info, root) -> list['GraphQLAssay']:
        loader_assays_for_sample_ids = info.context[LoaderKeys.ASSAYS_FOR_SAMPLES]
        return await loader_assays_for_sample_ids.load(
            sample_id_transform_to_raw(root.id)
        )

    @strawberry.field
    async def project(self, info: Info, root) -> GraphQLProject:
        return await info.context[LoaderKeys.PROJECTS_FOR_IDS].load(root.project)

    @strawberry.field
    async def sequencing_groups(self, info: Info, root) -> list['GraphQLSequencingGroup']:
        loader = info.context[LoaderKeys.SEQUENCING_GROUPS_FOR_SAMPLES]
        return await loader.load(root.id)



@strawberry.type
class GraphQLSequencingGroup:
    """SequencingGroup GraphQL model"""

    id: str
    type: str
    technology: str
    platform: str
    meta: strawberry.scalars.JSON
    external_ids: strawberry.scalars.JSON

    @strawberry.field
    async def sample(self, info: Info, root) -> GraphQLSample:
        loader = info.context[LoaderKeys.SAMPLES_FOR_IDS]
        return await loader.load(int(root.sample_id))

    @strawberry.field
    async def analysis(self, info: Info, root) -> GraphQLAnalysis:
        loader = info.context[LoaderKeys.ANALYSES_FOR_SEQUENCING_GROUP_IDS]
        return await loader.load(int(root.analysis_id))

    @strawberry.field
    async def assays(self, info: Info, root) -> list['GraphQLAssay']:
        loader = info.context[LoaderKeys.ASSAYS_FOR_SEQUENCING_GROUP_IDS]
        return await loader.load(int(root.id))


@strawberry.type
class GraphQLAssay:
    """Assay GraphQL model"""

    id: int
    type: str
    meta: strawberry.scalars.JSON
    external_ids: strawberry.scalars.JSON

    @strawberry.field
    async def sample(self, info: Info, root) -> GraphQLSample:
        loader = info.context[LoaderKeys.SAMPLES_FOR_IDS]
        return await loader.load(int(root.sample_id))


@strawberry.type
class Query:
    """GraphQL Queries"""

    @strawberry.field()
    async def project(self, info: Info, name: str) -> GraphQLProject:
        connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection.connection)
        project_id = await ptable.get_project_id_from_name_and_user(
            user=connection.author, project_name=name, readonly=True
        )
        presponse = await ptable.get_projects_by_ids([project_id])
        return presponse[0]

    @strawberry.field
    async def sample(self, info: Info, id: str) -> GraphQLSample:
        loader = info.context[LoaderKeys.SAMPLES_FOR_IDS]
        return await loader.load(sample_id_transform_to_raw(id))

    @strawberry.field
    async def sequencing_group(self, info: Info, id: str) -> GraphQLSequencingGroup:
        loader = info.context[LoaderKeys.SEQUENCING_GROUPS_FOR_IDS]
        return await loader.load(sequencing_group_id_transform_to_raw(id))

    @strawberry.field
    async def assay(self, info: Info, id: int) -> GraphQLAssay:
        connection = info.context['connection']
        slayer = AssayLayer(connection)
        return await slayer.get_assay_by_id(id)

    @strawberry.field
    async def participant(self, info: Info, id: int) -> GraphQLParticipant:
        connection = info.context['connection']
        player = ParticipantLayer(connection)
        ps = await player.get_participants_by_ids([id])
        return ps[0]

    @strawberry.field()
    async def family(self, info: Info, family_id: int) -> GraphQLFamily:
        connection = info.context['connection']
        return await FamilyLayer(connection).get_family_by_internal_id(family_id)

    @strawberry.field
    async def my_projects(self, info: Info) -> list[GraphQLProject]:
        connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection.connection)
        project_map = await ptable.get_projects_accessible_by_user(
            connection.author, readonly=True
        )
        return await ptable.get_projects_by_ids(list(project_map.keys()))


schema = strawberry.Schema(query=Query, mutation=None)
MetamistGraphQLRouter = GraphQLRouter(schema, graphiql=True, context_getter=get_context)
