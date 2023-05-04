# type: ignore
# flake8: noqa
# pylint: disable=no-value-for-parameter,redefined-builtin,missing-function-docstring,unused-argument
"""
Schema for GraphQL.

Note, we silence a lot of linting here because GraphQL looks at type annotations
and defaults to decide the GraphQL schema, so it might not necessarily look correct.
"""
from inspect import isclass

import strawberry
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info

from db.python import enum_tables
from db.python.layers import AnalysisLayer
from db.python.layers.family import FamilyLayer
from db.python.layers.assay import AssayLayer
from db.python.tables.project import ProjectPermissionsTable
from models.enums import AnalysisStatus
from models.models import (
    SampleInternal,
    ParticipantInternal,
    Project,
    AnalysisInternal,
    FamilyInternal,
    SequencingGroupInternal,
    AssayInternal,
)
from models.models.sample import sample_id_transform_to_raw
from models.utils.sample_id_format import (
    sample_id_format,
    sample_id_transform_to_raw_list,
)
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_transform_to_raw,
    sequencing_group_id_format,
)
from api.graphql.loaders import (
    get_context,
    LoaderKeys,
)


# @strawberry.type
# class GraphQLEnum:
#     @strawberry.field()
#     async def sequencing_types(self, info: Info) -> list[str]:
#         return await SequencingTypeTable(info.context['connection']).get()
enum_methods = {}
for enum in enum_tables.__dict__.values():
    if not isclass(enum):
        continue

    def create_function(_enum):
        async def m(info: Info) -> list[str]:
            return await _enum(info.context['connection']).get()

        m.__name__ = _enum.get_enum_name()
        # m.__annotations__ = {'return': list[str]}
        return m

    enum_methods[enum.get_enum_name()] = strawberry.field(
        create_function(enum),
    )


GraphQLEnum = strawberry.type(type('GraphQLEnum', (object,), enum_methods))


@strawberry.type
class GraphQLProject:
    """Project GraphQL model"""

    id: int
    name: str
    dataset: str
    meta: strawberry.scalars.JSON
    read_group_name: str | None = None
    write_group_name: str | None = None

    @staticmethod
    def from_internal(internal: Project) -> 'GraphQLProject':
        return GraphQLProject(
            id=internal.id,
            name=internal.name,
            dataset=internal.dataset,
            meta=internal.meta,
            read_group_name=internal.read_group_name,
            write_group_name=internal.write_group_name,
        )

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
        families = await FamilyLayer(connection).get_families(project=root.id)
        return families

    @strawberry.field()
    async def participants(
        self, info: Info, root: 'Project'
    ) -> list['GraphQLParticipant']:
        loader = info.context[LoaderKeys.PARTICIPANTS_FOR_PROJECTS]
        participants = await loader.load(root.id)
        return [GraphQLParticipant.from_internal(p) for p in participants]

    @strawberry.field()
    async def samples(
        self,
        info: Info,
        root: 'Project',
        internal_ids: list[str] | None = None,
        # external_ids: list[str] | None = None,
    ) -> list['GraphQLSample']:
        loader = info.context[LoaderKeys.SAMPLES_FOR_PROJECTS]
        samples = await loader.load(
            {
                'id': root.id,
                'internal_sample_ids': sample_id_transform_to_raw_list(internal_ids)
                if internal_ids
                else None,
                # 'external_sample_ids': external_ids,
            }
        )
        return [GraphQLSample.from_internal(p) for p in samples]

    @strawberry.field()
    async def sequencing_groups(
        self, info: Info, root: 'GraphQLProject'
    ) -> list['GraphQLSequencingGroup']:
        loader = info.context[LoaderKeys.SEQUENCING_GROUPS_FOR_PROJECTS]
        sequencing_groups = await loader.load(root.id)
        return [GraphQLSequencingGroup.from_internal(sg) for sg in sequencing_groups]

    @strawberry.field()
    async def analyses(
        self,
        info: Info,
        root: 'Project',
        type: str | None = None,
        active: bool | None = None,
    ) -> list['GraphQLAnalysis']:
        connection = info.context['connection']
        connection.project = root.id
        internal_analysis = await AnalysisLayer(connection).query_analysis(
            project_ids=[root.id], analysis_type=type, active=active
        )
        return [GraphQLAnalysis.from_internal(a) for a in internal_analysis]


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

    @staticmethod
    def from_internal(internal: AnalysisInternal) -> 'GraphQLAnalysis':
        return GraphQLAnalysis(
            id=internal.id,
            type=internal.type,
            status=internal.status,
            output=internal.output,
            timestamp_completed=internal.timestamp_completed,
            active=internal.active,
            meta=internal.meta,
            author=internal.author,
        )

    @strawberry.field
    async def sequencing_groups(
        self, info: Info, root: 'GraphQLAnalysis'
    ) -> list['GraphQLSequencingGroup']:
        loader = info.context[LoaderKeys.SEQUENCING_GROUPS_FOR_ANALYSIS]
        sgs = await loader.load(root.id)
        return [GraphQLSequencingGroup.from_internal(sg) for sg in sgs]

    @strawberry.field
    async def project(self, info: Info, root: 'GraphQLAnalysis') -> GraphQLProject:
        loader = info.context[LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project)
        return GraphQLProject.from_internal(project)


@strawberry.type
class GraphQLFamily:
    """GraphQL Family"""

    id: int
    external_id: str

    description: str | None
    coded_phenotype: str | None

    # internal
    project_id: strawberry.Private[int]

    @staticmethod
    def from_internal(internal: FamilyInternal) -> 'GraphQLFamily':
        return GraphQLFamily(
            id=internal.id,
            external_id=internal.external_id,
            description=internal.description,
            coded_phenotype=internal.coded_phenotype,
            project_id=internal.project,
        )

    @strawberry.field
    async def project(self, info: Info, root: 'GraphQLFamily') -> GraphQLProject:
        loader = info.context[LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)

    @strawberry.field
    async def participants(
        self, info: Info, root: 'GraphQLFamily'
    ) -> list['GraphQLParticipant']:
        participants = await info.context[LoaderKeys.PARTICIPANTS_FOR_FAMILIES].load(
            root.id
        )
        return [GraphQLParticipant.from_internal(p) for p in participants]


@strawberry.type
class GraphQLParticipant:
    """Participant GraphQL model"""

    id: int
    external_id: str
    meta: strawberry.scalars.JSON

    project_id: strawberry.Private[int]

    @staticmethod
    def from_internal(internal: ParticipantInternal) -> 'GraphQLParticipant':
        return GraphQLParticipant(
            id=internal.id,
            external_id=internal.external_id,
            meta=internal.meta,
            project_id=internal.project,
        )

    @strawberry.field
    async def samples(
        self, info: Info, root: 'GraphQLParticipant'
    ) -> list['GraphQLSample']:
        samples = await info.context[LoaderKeys.SAMPLES_FOR_PARTICIPANTS].load(root.id)
        return [GraphQLSample.from_internal(s) for s in samples]

    @strawberry.field
    async def families(
        self, info: Info, root: 'GraphQLParticipant'
    ) -> list[GraphQLFamily]:
        fams = await info.context[LoaderKeys.FAMILIES_FOR_PARTICIPANTS].load(root.id)
        return [GraphQLFamily.from_internal(f) for f in fams]

    @strawberry.field
    async def project(self, info: Info, root: 'GraphQLParticipant') -> GraphQLProject:
        loader = info.context[LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)


@strawberry.type
class GraphQLSample:
    """Sample GraphQL model"""

    id: str
    external_id: str
    active: bool
    meta: strawberry.scalars.JSON
    type: str
    author: str | None

    # keep as integers, because they're useful to reference in the fields below
    internal_id: strawberry.Private[int]
    participant_id: strawberry.Private[int]
    project_id: strawberry.Private[int]

    @staticmethod
    def from_internal(sample: SampleInternal):
        return GraphQLSample(
            id=sample_id_format(sample.id),
            external_id=sample.external_id,
            active=sample.active,
            meta=sample.meta,
            type=sample.type,
            author=sample.author,
            # internals
            internal_id=sample.id,
            participant_id=sample.participant_id,
            project_id=sample.project,
        )

    @strawberry.field
    async def participant(
        self, info: Info, root: 'GraphQLSample'
    ) -> GraphQLParticipant | None:
        if root.participant_id is None:
            return None
        loader_participants_for_ids = info.context[LoaderKeys.PARTICIPANTS_FOR_IDS]
        participant = await loader_participants_for_ids.load(root.participant_id)
        return GraphQLParticipant.from_internal(participant)

    @strawberry.field
    async def assays(
        self, info: Info, root: 'GraphQLSample', type: str | None = None
    ) -> list['GraphQLAssay']:
        loader_assays_for_sample_ids = info.context[LoaderKeys.ASSAYS_FOR_SAMPLES]
        assays = await loader_assays_for_sample_ids.load(
            {'id': root.internal_id, 'assay_type': type}
        )
        return [GraphQLAssay.from_internal(assay) for assay in assays]

    @strawberry.field
    async def project(self, info: Info, root: 'GraphQLSample') -> GraphQLProject:
        project = await info.context[LoaderKeys.PROJECTS_FOR_IDS].load(root.project_id)
        return GraphQLProject.from_internal(project)

    @strawberry.field
    async def sequencing_groups(
        self,
        info: Info,
        root: 'GraphQLSample',
        sequencing_type: str | None = None,
        active_only: bool = True,
    ) -> list['GraphQLSequencingGroup']:
        loader = info.context[LoaderKeys.SEQUENCING_GROUPS_FOR_SAMPLES]
        sequencing_groups = await loader.load(
            {
                'id': root.internal_id,
                'sequencing_type': sequencing_type,
                'active_only': active_only,
            }
        )

        return [GraphQLSequencingGroup.from_internal(sg) for sg in sequencing_groups]


@strawberry.type
class GraphQLSequencingGroup:
    """SequencingGroup GraphQL model"""

    id: str
    type: str
    technology: str
    platform: str
    meta: strawberry.scalars.JSON
    external_ids: strawberry.scalars.JSON

    internal_id: strawberry.Private[int]
    sample_id: strawberry.Private[int]

    @staticmethod
    def from_internal(internal: SequencingGroupInternal) -> 'GraphQLSequencingGroup':
        return GraphQLSequencingGroup(
            id=sequencing_group_id_format(internal.id),
            type=internal.type,
            technology=internal.technology,
            platform=internal.platform,
            meta=internal.meta,
            external_ids=internal.external_ids,
            # internal
            internal_id=internal.id,
            sample_id=internal.sample_id,
        )

    @strawberry.field
    async def sample(self, info: Info, root: 'GraphQLSequencingGroup') -> GraphQLSample:
        loader = info.context[LoaderKeys.SAMPLES_FOR_IDS]
        sample = await loader.load(root.sample_id)
        return GraphQLSample.from_internal(sample)

    @strawberry.field
    async def analyses(
        self,
        info: Info,
        root: 'GraphQLSequencingGroup',
        analysis_status: AnalysisStatus = AnalysisStatus.COMPLETED,
        analysis_type: str | None = None,
    ) -> list[GraphQLAnalysis]:
        loader = info.context[LoaderKeys.ANALYSES_FOR_SEQUENCING_GROUPS]
        analyses = await loader.load(
            {
                'id': root.internal_id,
                'status': analysis_status,
                'analysis_type': analysis_type,
            }
        )
        return [GraphQLAnalysis.from_internal(a) for a in analyses]

    @strawberry.field
    async def assays(
        self, info: Info, root: 'GraphQLSequencingGroup'
    ) -> list['GraphQLAssay']:
        loader = info.context[LoaderKeys.ASSAYS_FOR_SEQUENCING_GROUPS]
        return await loader.load(root.internal_id)


@strawberry.type
class GraphQLAssay:
    """Assay GraphQL model"""

    id: int
    type: str
    meta: strawberry.scalars.JSON
    external_ids: strawberry.scalars.JSON

    sample_id: strawberry.Private[int]

    @staticmethod
    def from_internal(internal: AssayInternal) -> 'GraphQLAssay':
        return GraphQLAssay(
            id=internal.id,
            type=internal.type,
            meta=internal.meta,
            external_ids=internal.external_ids,
            # internal
            sample_id=internal.sample_id,
        )

    @strawberry.field
    async def sample(self, info: Info, root: 'GraphQLAssay') -> GraphQLSample:
        loader = info.context[LoaderKeys.SAMPLES_FOR_IDS]
        sample = await loader.load(root.sample_id)
        return GraphQLSample.from_internal(sample)


@strawberry.type
class Query:
    """GraphQL Queries"""

    @strawberry.field()
    def enum(self, info: Info) -> GraphQLEnum:
        return GraphQLEnum()

    @strawberry.field()
    async def project(self, info: Info, name: str) -> GraphQLProject:
        connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection.connection)
        project_id = await ptable.get_project_id_from_name_and_user(
            user=connection.author, project_name=name, readonly=True
        )
        presponse = await ptable.get_projects_by_ids([project_id])
        return GraphQLProject.from_internal(presponse[0])

    @strawberry.field
    async def sample(self, info: Info, id: str) -> GraphQLSample:
        loader = info.context[LoaderKeys.SAMPLES_FOR_IDS]
        sample = await loader.load(sample_id_transform_to_raw(id))
        return GraphQLSample.from_internal(sample)

    @strawberry.field
    async def sequencing_group(self, info: Info, id: str) -> GraphQLSequencingGroup:
        loader = info.context[LoaderKeys.SEQUENCING_GROUPS_FOR_IDS]
        sg = await loader.load(sequencing_group_id_transform_to_raw(id))
        return GraphQLSequencingGroup.from_internal(sg)

    @strawberry.field
    async def assay(self, info: Info, id: int) -> GraphQLAssay:
        connection = info.context['connection']
        slayer = AssayLayer(connection)
        assay = await slayer.get_assay_by_id(id)
        return GraphQLAssay.from_internal(assay)

    @strawberry.field
    async def participant(self, info: Info, id: int) -> GraphQLParticipant:
        loader = info.context[LoaderKeys.PARTICIPANTS_FOR_IDS]
        return GraphQLParticipant.from_internal(await loader.load(id))

    @strawberry.field()
    async def family(self, info: Info, family_id: int) -> GraphQLFamily:
        connection = info.context['connection']
        family = await FamilyLayer(connection).get_family_by_internal_id(family_id)
        return GraphQLFamily.from_internal(family)

    @strawberry.field
    async def my_projects(self, info: Info) -> list[GraphQLProject]:
        connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection.connection)
        project_map = await ptable.get_projects_accessible_by_user(
            connection.author, readonly=True
        )
        projects = await ptable.get_projects_by_ids(list(project_map.keys()))
        return [GraphQLProject.from_internal(p) for p in projects]


schema = strawberry.Schema(query=Query, mutation=None)
MetamistGraphQLRouter = GraphQLRouter(schema, graphiql=True, context_getter=get_context)
