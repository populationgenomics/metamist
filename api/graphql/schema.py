# pylint: disable=redefined-builtin,too-many-arguments
"""
Schema for GraphQL.

Note, we silence a lot of linting here because GraphQL looks at type annotations
and defaults to decide the GraphQL schema, so it might not necessarily look correct.
"""

import datetime

import strawberry
from strawberry.extensions import QueryDepthLimiter
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info

from api.graphql.filters import (
    GraphQLFilter,
    GraphQLMetaFilter,
    graphql_meta_filter_to_internal_filter,
)
from api.graphql.loaders import GraphQLContext, LoaderKeys, get_context
from api.graphql.mutations import Mutation
from api.graphql.types import (
    GraphQLAnalysis,
    GraphQLAnalysisRunner,
    GraphQLAssay,
    GraphQLCohort,
    GraphQLCohortTemplate,
    GraphQLEnum,
    GraphQLFamily,
    GraphQLParticipant,
    GraphQLProject,
    GraphQLProjectGroup,
    GraphQLSample,
    GraphQLSequencingGroup,
    GraphQLUser,
)
from db.python.filters import GenericFilter
from db.python.layers import (
    AnalysisLayer,
    AnalysisRunnerLayer,
    AssayLayer,
    CohortLayer,
    FamilyLayer,
    SampleLayer,
    SequencingGroupLayer,
)
from db.python.layers.project_groups import ProjectGroupsLayer
from db.python.layers.user import UserLayer
from db.python.tables.analysis import AnalysisFilter
from db.python.tables.analysis_runner import AnalysisRunnerFilter
from db.python.tables.cohort import CohortFilter, CohortTemplateFilter
from db.python.tables.sample import SampleFilter
from db.python.tables.sequencing_group import SequencingGroupFilter
from models.models.project import (
    FullWriteAccessRoles,
    ProjectId,
    ReadAccessRoles,
)
from models.utils.cohort_id_format import cohort_id_transform_to_raw
from models.utils.cohort_template_id_format import cohort_template_id_transform_to_raw
from models.utils.sample_id_format import sample_id_transform_to_raw
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_transform_to_raw,
)


@strawberry.type
class GraphQLViewer:
    """
    The "Viewer" construct is the current user and contains information about the
    current user and their permissions
    """

    username: str
    projects: list[GraphQLProject]


@strawberry.type
class Query:  # entry point to graphql.
    """GraphQL Queries"""

    @strawberry.field()
    def enum(self, _info: Info[GraphQLContext, 'Query']) -> GraphQLEnum:
        """Show all the available enums for the API"""
        return GraphQLEnum()

    @strawberry.field()
    async def viewer(
        self,
        info: Info[GraphQLContext, 'Query'],
    ) -> GraphQLViewer:
        """Get the current user and their projects"""
        connection = info.context['connection']
        return GraphQLViewer(
            username=connection.author,
            projects=[
                GraphQLProject.from_internal(p) for p in connection.all_projects()
            ],
        )

    @strawberry.field()
    async def cohort_templates(
        self,
        info: Info[GraphQLContext, 'Query'],
        id: GraphQLFilter[str] | None = None,
        project: GraphQLFilter[str] | None = None,
    ) -> list[GraphQLCohortTemplate]:
        """Cohort Templates query"""
        connection = info.context['connection']
        cohort_layer = CohortLayer(connection)

        project_name_map: dict[str, int] = {}
        project_filter = None
        if project:
            project_names = project.all_values()
            projects = connection.get_and_check_access_to_projects_for_names(
                project_names=project_names, allowed_roles=ReadAccessRoles
            )
            project_name_map = {p.name: p.id for p in projects}
            project_filter = project.to_internal_filter_mapped(
                lambda pname: project_name_map[pname]
            )

        filter_ = CohortTemplateFilter(
            id=(
                id.to_internal_filter_mapped(cohort_id_transform_to_raw) if id else None
            ),
            project=project_filter,
        )

        cohort_templates = await cohort_layer.query_cohort_templates(filter_)

        external_templates = []

        for template in cohort_templates:
            template_projects = connection.get_and_check_access_to_projects_for_ids(
                project_ids=template.criteria.projects or [],
                allowed_roles=ReadAccessRoles,
            )
            template_project_names = [p.name for p in template_projects if p.name]
            external_templates.append(
                GraphQLCohortTemplate.from_internal(template, template_project_names)
            )

        return external_templates

    @strawberry.field()
    async def cohorts(
        self,
        info: Info[GraphQLContext, 'Query'],
        id: GraphQLFilter[str] | None = None,
        project: GraphQLFilter[str] | None = None,
        name: GraphQLFilter[str] | None = None,
        author: GraphQLFilter[str] | None = None,
        template_id: GraphQLFilter[str] | None = None,
    ) -> list[GraphQLCohort]:
        """Cohorts query"""
        connection = info.context['connection']
        cohort_layer = CohortLayer(connection)

        project_name_map: dict[str, int] = {}
        project_filter = None
        if project:
            project_names = project.all_values()
            projects = connection.get_and_check_access_to_projects_for_names(
                project_names=project_names, allowed_roles=ReadAccessRoles
            )
            project_name_map = {p.name: p.id for p in projects}
            project_filter = project.to_internal_filter_mapped(
                lambda pname: project_name_map[pname]
            )

        filter_ = CohortFilter(
            id=id.to_internal_filter_mapped(cohort_id_transform_to_raw) if id else None,
            name=name.to_internal_filter() if name else None,
            project=project_filter,
            author=author.to_internal_filter() if author else None,
            template_id=(
                template_id.to_internal_filter_mapped(
                    cohort_template_id_transform_to_raw
                )
                if template_id
                else None
            ),
        )

        cohorts = await cohort_layer.query(filter_)
        return [GraphQLCohort.from_internal(cohort) for cohort in cohorts]

    @strawberry.field()
    async def project(
        self, info: Info[GraphQLContext, 'Query'], name: str
    ) -> GraphQLProject:
        """Project Query"""
        connection = info.context['connection']
        projects = connection.get_and_check_access_to_projects_for_names(
            project_names=[name], allowed_roles=ReadAccessRoles
        )
        return GraphQLProject.from_internal(next(p for p in projects))

    @strawberry.field
    async def sample(
        self,
        info: Info[GraphQLContext, 'Query'],
        id: GraphQLFilter[str] | None = None,
        project: GraphQLFilter[str] | None = None,
        type: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
        external_id: GraphQLFilter[str] | None = None,
        participant_id: GraphQLFilter[int] | None = None,
        active: GraphQLFilter[bool] | None = None,
        parent_id: GraphQLFilter[str] | None = None,
        root_id: GraphQLFilter[str] | None = None,
    ) -> list[GraphQLSample]:
        """Sample Query"""
        connection = info.context['connection']
        slayer = SampleLayer(connection)

        if not id and not project:
            raise ValueError('Must provide either id or project')

        if external_id and not project:
            raise ValueError('Must provide project when using external_id filter')

        project_name_map: dict[str, int] = {}
        if project:
            project_names = project.all_values()
            projects = connection.get_and_check_access_to_projects_for_names(
                project_names=project_names,
                allowed_roles=ReadAccessRoles,
            )
            project_name_map = {p.name: p.id for p in projects if p.name and p.id}

        filter_ = SampleFilter(
            id=id.to_internal_filter_mapped(sample_id_transform_to_raw) if id else None,
            type=type.to_internal_filter() if type else None,
            meta=graphql_meta_filter_to_internal_filter(meta),
            external_id=external_id.to_internal_filter() if external_id else None,
            participant_id=(
                participant_id.to_internal_filter() if participant_id else None
            ),
            project=(
                project.to_internal_filter_mapped(lambda pname: project_name_map[pname])
                if project
                else None
            ),
            active=active.to_internal_filter() if active else GenericFilter(eq=True),
            sample_root_id=(
                root_id.to_internal_filter_mapped(sample_id_transform_to_raw)
                if root_id
                else None
            ),
            sample_parent_id=(
                parent_id.to_internal_filter_mapped(sample_id_transform_to_raw)
                if parent_id
                else None
            ),
        )

        samples = await slayer.query(filter_)
        return [GraphQLSample.from_internal(sample) for sample in samples]

    # pylint: disable=too-many-arguments
    @strawberry.field
    async def sequencing_groups(
        self,
        info: Info[GraphQLContext, 'Query'],
        id: GraphQLFilter[str] | None = None,
        project: GraphQLFilter[str] | None = None,
        sample_id: GraphQLFilter[str] | None = None,
        type: GraphQLFilter[str] | None = None,
        technology: GraphQLFilter[str] | None = None,
        platform: GraphQLFilter[str] | None = None,
        active_only: GraphQLFilter[bool] | None = None,
        created_on: GraphQLFilter[datetime.date] | None = None,
        assay_meta: GraphQLMetaFilter | None = None,
        has_cram: bool | None = None,
        has_gvcf: bool | None = None,
    ) -> list[GraphQLSequencingGroup]:
        """Sequencing Groups Query"""

        connection = info.context['connection']
        sglayer = SequencingGroupLayer(connection)
        if not (project or sample_id or id):
            raise ValueError('Must filter by project, sample or id')

        # we list project names, but internally we want project ids
        _project_filter: GenericFilter[ProjectId] | None = None

        if project:
            project_names = project.all_values()
            projects = connection.get_and_check_access_to_projects_for_names(
                project_names=project_names,
                allowed_roles=ReadAccessRoles,
            )
            project_id_map = {p.name: p.id for p in projects if p.name and p.id}
            _project_filter = project.to_internal_filter_mapped(
                lambda p: project_id_map[p]
            )

        filter_ = SequencingGroupFilter(
            project=_project_filter,
            sample=(
                SequencingGroupFilter.SequencingGroupSampleFilter(
                    id=sample_id.to_internal_filter_mapped(sample_id_transform_to_raw)
                )
                if sample_id
                else None
            ),
            id=(
                id.to_internal_filter_mapped(sequencing_group_id_transform_to_raw)
                if id
                else None
            ),
            type=type.to_internal_filter() if type else None,
            technology=technology.to_internal_filter() if technology else None,
            platform=platform.to_internal_filter() if platform else None,
            active_only=(
                active_only.to_internal_filter()
                if active_only
                else GenericFilter(eq=True)
            ),
            created_on=created_on.to_internal_filter() if created_on else None,
            assay=(
                SequencingGroupFilter.SequencingGroupAssayFilter(
                    meta=graphql_meta_filter_to_internal_filter(assay_meta),
                )
            ),
            has_cram=has_cram,
            has_gvcf=has_gvcf,
        )
        sgs = await sglayer.query(filter_)
        return [GraphQLSequencingGroup.from_internal(sg) for sg in sgs]

    @strawberry.field
    async def assay(self, info: Info[GraphQLContext, 'Query'], id: int) -> GraphQLAssay:
        """Assay Query"""
        connection = info.context['connection']
        slayer = AssayLayer(connection)
        assay = await slayer.get_assay_by_id(id)
        return GraphQLAssay.from_internal(assay)

    @strawberry.field
    async def participant(
        self, info: Info[GraphQLContext, 'Query'], id: int
    ) -> GraphQLParticipant:
        """Participant Query"""
        loader = info.context['loaders'][LoaderKeys.PARTICIPANTS_FOR_IDS]
        return GraphQLParticipant.from_internal(await loader.load(id))

    @strawberry.field()
    async def family(
        self, info: Info[GraphQLContext, 'Query'], family_id: int
    ) -> GraphQLFamily:
        """Family Query"""
        connection = info.context['connection']
        family = await FamilyLayer(connection).get_family_by_internal_id(family_id)
        return GraphQLFamily.from_internal(family)

    @strawberry.field
    async def my_projects(
        self, info: Info[GraphQLContext, 'Query']
    ) -> list[GraphQLProject]:
        """Get projects the current user has access to"""
        connection = info.context['connection']
        projects = connection.projects_with_role(
            ReadAccessRoles.union(FullWriteAccessRoles)
        )
        return [GraphQLProject.from_internal(p) for p in projects]

    @strawberry.field
    async def analysis_runner(
        self,
        info: Info[GraphQLContext, 'Query'],
        ar_guid: str,
    ) -> GraphQLAnalysisRunner:
        """Get a specific analysis runner by its GUID"""
        if not ar_guid:
            raise ValueError('Must provide ar_guid')
        connection = info.context['connection']
        alayer = AnalysisRunnerLayer(connection)
        filter_ = AnalysisRunnerFilter(ar_guid=GenericFilter(eq=ar_guid))
        analysis_runners = await alayer.query(filter_)
        if len(analysis_runners) != 1:
            raise ValueError(
                f'Expected exactly one analysis runner expected, found {len(analysis_runners)}'
            )
        return GraphQLAnalysisRunner.from_internal(analysis_runners[0])

    @strawberry.field
    async def analyses(
        self,
        info: Info[GraphQLContext, 'Query'],
        id: GraphQLFilter[int],
    ) -> list[GraphQLAnalysis]:
        """Get analyses by ID"""
        connection = info.context['connection']
        analyses = await AnalysisLayer(connection).query(
            AnalysisFilter(id=id.to_internal_filter())
        )
        return [GraphQLAnalysis.from_internal(a) for a in analyses]

    @strawberry.field
    async def user(self, info: Info, id: int) -> GraphQLUser | None:
        """Get a user by ID"""
        connection = info.context['connection']
        user = await UserLayer(connection).get_user_by_id(id)
        return GraphQLUser.from_internal(user) if user else None

    @strawberry.field
    async def users(self, info: Info) -> list[GraphQLUser]:
        """Get all users"""
        connection = info.context['connection']
        # You may want to implement a method to fetch all users
        users = await UserLayer(connection).get_all_users()
        return [GraphQLUser.from_internal(u) for u in users]

    @strawberry.field()
    async def project_groups(self, info: Info) -> list['GraphQLProjectGroup']:
        """Get all project groups"""
        connection = info.context['connection']
        groups = await ProjectGroupsLayer(connection).get_all()
        return [GraphQLProjectGroup.from_internal(g) for g in groups]

    @strawberry.field()
    async def project_group(self, info: Info, id: int) -> 'GraphQLProjectGroup | None':
        """Get a project group by ID"""
        connection = info.context['connection']
        group = await ProjectGroupsLayer(connection).get_by_id(id)
        return GraphQLProjectGroup.from_internal(group) if group else None


schema = strawberry.Schema(
    query=Query, mutation=Mutation, extensions=[QueryDepthLimiter(max_depth=10)]
)
MetamistGraphQLRouter: GraphQLRouter = GraphQLRouter(
    schema, graphiql=True, context_getter=get_context
)
