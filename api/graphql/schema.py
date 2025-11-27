# pylint: disable=no-value-for-parameter,redefined-builtin,missing-function-docstring,unused-argument,too-many-lines,too-many-arguments
"""
Schema for GraphQL.

Note, we silence a lot of linting here because GraphQL looks at type annotations
and defaults to decide the GraphQL schema, so it might not necessarily look correct.
"""

import datetime
from collections import defaultdict
from inspect import isclass
from typing import (  # pylint: disable=unused-import; Union is used, pylint just doesn't know about it
    Annotated,
    Union,
)

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
from api.settings import COHORT_PREFIX, SAMPLE_PREFIX, SEQUENCING_GROUP_PREFIX
from db.python import enum_tables
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
from db.python.tables.analysis import AnalysisFilter
from db.python.tables.analysis_runner import AnalysisRunnerFilter
from db.python.tables.assay import AssayFilter
from db.python.tables.cohort import CohortFilter, CohortTemplateFilter
from db.python.tables.family import FamilyFilter
from db.python.tables.participant import ParticipantFilter
from db.python.tables.sample import SampleFilter
from db.python.tables.sequencing_group import SequencingGroupFilter
from models.enums import AnalysisStatus
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AnalysisInternal,
    AssayInternal,
    AuditLogInternal,
    CohortInternal,
    CohortTemplateInternal,
    FamilyInternal,
    ParticipantInternal,
    Project,
    SampleInternal,
    SequencingGroupInternal,
)
from models.models.analysis_runner import AnalysisRunnerInternal
from models.models.comment import (
    CommentEntityType,
    CommentInternal,
    CommentStatus,
    CommentVersionInternal,
    DiscussionInternal,
)
from models.models.family import PedRowInternal
from models.models.project import (
    FullWriteAccessRoles,
    ProjectId,
    ProjectMemberRole,
    ReadAccessRoles,
)
from models.models.sample import sample_id_transform_to_raw
from models.utils.cohort_id_format import cohort_id_format, cohort_id_transform_to_raw
from models.utils.cohort_template_id_format import (
    cohort_template_id_format,
    cohort_template_id_transform_to_raw,
)
from models.utils.sample_id_format import sample_id_format
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_format,
    sequencing_group_id_transform_to_raw,
)

enum_methods = {}
for enum in enum_tables.__dict__.values():
    if not isclass(enum):
        continue

    def create_function(_enum):
        async def m(info: Info[GraphQLContext, 'Query']) -> list[str]:
            return await _enum(info.context['connection']).get()

        m.__name__ = _enum.get_enum_name()
        # m.__annotations__ = {'return': list[str]}
        return m

    enum_methods[enum.get_enum_name()] = strawberry.field(
        create_function(enum),
    )


GraphQLEnum = strawberry.type(type('GraphQLEnum', (object,), enum_methods))

GraphQLAnalysisStatus = strawberry.enum(AnalysisStatus)  # type: ignore


# Create cohort GraphQL model
@strawberry.type
class GraphQLCohort:
    """Cohort GraphQL model"""

    id: str
    name: str
    description: str
    author: str

    project_id: strawberry.Private[int]

    @staticmethod
    def from_internal(internal: CohortInternal) -> 'GraphQLCohort':
        return GraphQLCohort(
            id=cohort_id_format(internal.id),
            name=internal.name,
            description=internal.description,
            author=internal.author,
            project_id=internal.project,
        )

    @strawberry.field()
    async def template(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLCohort'
    ) -> 'GraphQLCohortTemplate':
        connection = info.context['connection']
        template = await CohortLayer(connection).get_template_by_cohort_id(
            cohort_id_transform_to_raw(root.id)
        )

        projects = connection.get_and_check_access_to_projects_for_ids(
            project_ids=(
                template.criteria.projects if template.criteria.projects else []
            ),
            allowed_roles=ReadAccessRoles,
        )
        project_names = [p.name for p in projects if p.name]

        return GraphQLCohortTemplate.from_internal(
            template, project_names=project_names
        )

    @strawberry.field()
    async def sequencing_groups(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'GraphQLCohort',
        active_only: GraphQLFilter[bool] | None = None,
    ) -> list['GraphQLSequencingGroup']:
        connection = info.context['connection']
        cohort_layer = CohortLayer(connection)
        sg_ids = await cohort_layer.get_cohort_sequencing_group_ids(
            cohort_id_transform_to_raw(root.id)
        )

        sg_layer = SequencingGroupLayer(connection)
        filter = SequencingGroupFilter(
            id=GenericFilter(in_=sg_ids),
            active_only=active_only.to_internal_filter() if active_only else None,
        )
        sequencing_groups = await sg_layer.query(filter_=filter)

        return [GraphQLSequencingGroup.from_internal(sg) for sg in sequencing_groups]

    @strawberry.field()
    async def analyses(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLCohort'
    ) -> list['GraphQLAnalysis']:
        connection = info.context['connection']
        internal_analysis = await AnalysisLayer(connection).query(
            AnalysisFilter(
                cohort_id=GenericFilter(in_=[cohort_id_transform_to_raw(root.id)]),
            )
        )
        return [GraphQLAnalysis.from_internal(a) for a in internal_analysis]

    @strawberry.field()
    async def project(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLCohort'
    ) -> 'GraphQLProject':
        loader = info.context['loaders'][LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)


# Create cohort template GraphQL model
@strawberry.type
class GraphQLCohortTemplate:
    """CohortTemplate GraphQL model"""

    id: str
    name: str
    description: str
    criteria: strawberry.scalars.JSON

    project_id: strawberry.Private[int]

    @staticmethod
    def from_internal(
        internal: CohortTemplateInternal, project_names: list[str]
    ) -> 'GraphQLCohortTemplate':
        # At this point, the object that comes in doesn't have an ID field.
        return GraphQLCohortTemplate(
            id=cohort_template_id_format(internal.id),
            name=internal.name,
            description=internal.description,
            criteria=internal.criteria.to_external(
                project_names=project_names
            ).model_dump(),
            project_id=internal.project,
        )

    @strawberry.field()
    async def project(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLCohortTemplate'
    ) -> 'GraphQLProject':
        loader = info.context['loaders'][LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)


@strawberry.type
class GraphQLCommentVersion:
    """A version of a comment's content"""

    content: str
    author: str
    status: strawberry.enum(CommentStatus)  # type: ignore
    timestamp: datetime.datetime

    @staticmethod
    def from_internal(internal: CommentVersionInternal) -> 'GraphQLCommentVersion':
        return GraphQLCommentVersion(
            content=internal.content,
            author=internal.author,
            timestamp=internal.timestamp,
            status=internal.status,
        )


@strawberry.type
class GraphQLDiscussion:
    """A comment discussion, made up of flat lists of direct and related comments"""

    direct_comments: list['GraphQLComment']
    related_comments: list['GraphQLComment']

    @staticmethod
    def from_internal(
        internal: DiscussionInternal | None,
    ) -> 'GraphQLDiscussion':
        direct_comments = internal.direct_comments if internal is not None else []
        related_comments = internal.related_comments if internal is not None else []
        return GraphQLDiscussion(
            direct_comments=[GraphQLComment.from_internal(c) for c in direct_comments],
            related_comments=[
                GraphQLComment.from_internal(c) for c in related_comments
            ],
        )


@strawberry.type
class GraphQLComment:
    """A comment made on a entity"""

    id: int
    parentId: int | None
    content: str
    author: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    comment_entity_type: strawberry.Private[CommentEntityType]
    comment_entity_id: strawberry.Private[int]
    status: strawberry.enum(CommentStatus)  # type: ignore
    thread: list['GraphQLComment']
    versions: list[GraphQLCommentVersion]

    @strawberry.field()
    async def entity(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLComment'
    ) -> Annotated[
        'Union[GraphQLSample , GraphQLAssay , GraphQLSequencingGroup , GraphQLProject , GraphQLParticipant , GraphQLFamily]',
        strawberry.union('GraphQLCommentEntity'),
    ]:
        entity_type = root.comment_entity_type
        entity_id = root.comment_entity_id

        match entity_type:
            case CommentEntityType.sample:
                loader = info.context['loaders'][LoaderKeys.SAMPLES_FOR_IDS]
                sample = await loader.load(entity_id)
                return GraphQLSample.from_internal(sample)

            case CommentEntityType.sequencing_group:
                loader = info.context['loaders'][LoaderKeys.SEQUENCING_GROUPS_FOR_IDS]
                sg = await loader.load(entity_id)
                return GraphQLSequencingGroup.from_internal(sg)

            case CommentEntityType.project:
                loader = info.context['loaders'][LoaderKeys.PROJECTS_FOR_IDS]
                sg = await loader.load(entity_id)
                return GraphQLProject.from_internal(sg)

            case CommentEntityType.assay:
                loader = info.context['loaders'][LoaderKeys.ASSAYS_FOR_IDS]
                ay = await loader.load(entity_id)
                return GraphQLAssay.from_internal(ay)

            case CommentEntityType.participant:
                loader = info.context['loaders'][LoaderKeys.PARTICIPANTS_FOR_IDS]
                pt = await loader.load(entity_id)
                return GraphQLParticipant.from_internal(pt)

            case CommentEntityType.family:
                loader = info.context['loaders'][LoaderKeys.FAMILIES_FOR_IDS]
                fm = await loader.load(entity_id)
                return GraphQLFamily.from_internal(fm)

    @staticmethod
    def from_internal(internal: CommentInternal) -> 'GraphQLComment':
        return GraphQLComment(
            id=internal.id,
            parentId=internal.parent_id,
            content=internal.content,
            author=internal.author,
            created_at=internal.created_at,
            updated_at=internal.updated_at,
            comment_entity_type=internal.comment_entity_type,
            comment_entity_id=internal.comment_entity_id,
            thread=[GraphQLComment.from_internal(c) for c in internal.thread],
            status=internal.status,
            versions=[
                GraphQLCommentVersion.from_internal(v) for v in internal.versions
            ],
        )


@strawberry.type
class GraphQLProject:
    """Project GraphQL model"""

    id: int
    name: str
    dataset: str
    meta: strawberry.scalars.JSON
    roles: list[strawberry.enum(ProjectMemberRole)]  # type: ignore

    @staticmethod
    def from_internal(internal: Project) -> 'GraphQLProject':
        return GraphQLProject(
            id=internal.id,
            name=internal.name,
            dataset=internal.dataset,
            meta=internal.meta,
            roles=list(internal.roles),
        )

    @strawberry.field()
    async def analysis_runner(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'Project',
        ar_guid: GraphQLFilter[str] | None = None,
        author: GraphQLFilter[str] | None = None,
        repository: GraphQLFilter[str] | None = None,
        access_level: GraphQLFilter[str] | None = None,
        environment: GraphQLFilter[str] | None = None,
    ) -> list['GraphQLAnalysisRunner']:
        connection = info.context['connection']
        alayer = AnalysisRunnerLayer(connection)
        filter_ = AnalysisRunnerFilter(
            project=GenericFilter(eq=root.id),
            ar_guid=ar_guid.to_internal_filter() if ar_guid else None,
            submitting_user=author.to_internal_filter() if author else None,
            repository=repository.to_internal_filter() if repository else None,
            access_level=access_level.to_internal_filter() if access_level else None,
            environment=environment.to_internal_filter() if environment else None,
        )
        analysis_runners = await alayer.query(filter_)
        return [GraphQLAnalysisRunner.from_internal(ar) for ar in analysis_runners]

    @strawberry.field()
    async def pedigree(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'Project',
        internal_family_ids: list[int] | None = None,
        replace_with_participant_external_ids: bool = True,
        replace_with_family_external_ids: bool = True,
        include_participants_not_in_families: bool = False,
        empty_participant_value: str | None = None,
    ) -> list[strawberry.scalars.JSON]:
        connection = info.context['connection']
        family_layer = FamilyLayer(connection)

        if not root.id:
            raise ValueError('Project must have an id')

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
    async def families(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'GraphQLProject',
        id: GraphQLFilter[int] | None = None,
        external_id: GraphQLFilter[str] | None = None,
    ) -> list['GraphQLFamily']:
        # don't need a data loader here as we're presuming we're not often running
        # the "families" method for many projects at once. If so, we might need to fix that
        connection = info.context['connection']
        families = await FamilyLayer(connection).query(
            FamilyFilter(
                project=GenericFilter(eq=root.id),
                id=id.to_internal_filter() if id else None,
                external_id=external_id.to_internal_filter() if external_id else None,
            )
        )
        return [GraphQLFamily.from_internal(f) for f in families]

    @strawberry.field()
    async def participants(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'GraphQLProject',
        id: GraphQLFilter[int] | None = None,
        external_id: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
        reported_sex: GraphQLFilter[int] | None = None,
        reported_gender: GraphQLFilter[str] | None = None,
        karyotype: GraphQLFilter[str] | None = None,
    ) -> list['GraphQLParticipant']:
        loader = info.context['loaders'][LoaderKeys.PARTICIPANTS_FOR_PROJECTS]
        participants = await loader.load(
            {
                'id': root.id,
                'filter_': ParticipantFilter(
                    project=GenericFilter(eq=root.id),
                    id=id.to_internal_filter() if id else None,
                    external_id=(
                        external_id.to_internal_filter() if external_id else None
                    ),
                    meta=graphql_meta_filter_to_internal_filter(meta),
                    reported_gender=(
                        reported_gender.to_internal_filter()
                        if reported_gender
                        else None
                    ),
                    reported_sex=(
                        reported_sex.to_internal_filter() if reported_sex else None
                    ),
                    karyotype=karyotype.to_internal_filter() if karyotype else None,
                ),
            }
        )
        return [GraphQLParticipant.from_internal(p) for p in participants]

    @strawberry.field()
    async def samples(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'GraphQLProject',
        type: GraphQLFilter[str] | None = None,
        external_id: GraphQLFilter[str] | None = None,
        id: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
        parent_id: GraphQLFilter[str] | None = None,
        root_id: GraphQLFilter[str] | None = None,
    ) -> list['GraphQLSample']:
        loader = info.context['loaders'][LoaderKeys.SAMPLES_FOR_PROJECTS]
        filter_ = SampleFilter(
            type=type.to_internal_filter() if type else None,
            external_id=external_id.to_internal_filter() if external_id else None,
            id=id.to_internal_filter_mapped(sample_id_transform_to_raw) if id else None,
            meta=graphql_meta_filter_to_internal_filter(meta),
            sample_parent_id=(
                parent_id.to_internal_filter_mapped(sample_id_transform_to_raw)
                if parent_id
                else None
            ),
            sample_root_id=(
                root_id.to_internal_filter_mapped(sample_id_transform_to_raw)
                if root_id
                else None
            ),
        )
        samples = await loader.load({'id': root.id, 'filter': filter_})
        return [GraphQLSample.from_internal(p) for p in samples]

    @strawberry.field()
    async def sequencing_groups(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'GraphQLProject',
        id: GraphQLFilter[str] | None = None,
        external_id: GraphQLFilter[str] | None = None,
        type: GraphQLFilter[str] | None = None,
        technology: GraphQLFilter[str] | None = None,
        platform: GraphQLFilter[str] | None = None,
        active_only: GraphQLFilter[bool] | None = None,
    ) -> list['GraphQLSequencingGroup']:
        loader = info.context['loaders'][LoaderKeys.SEQUENCING_GROUPS_FOR_PROJECTS]
        filter_ = SequencingGroupFilter(
            id=(
                id.to_internal_filter_mapped(sequencing_group_id_transform_to_raw)
                if id
                else None
            ),
            external_id=external_id.to_internal_filter() if external_id else None,
            type=type.to_internal_filter() if type else None,
            technology=technology.to_internal_filter() if technology else None,
            platform=platform.to_internal_filter() if platform else None,
            active_only=(
                active_only.to_internal_filter()
                if active_only
                else GenericFilter(eq=True)
            ),
        )
        sequencing_groups = await loader.load({'id': root.id, 'filter': filter_})
        return [GraphQLSequencingGroup.from_internal(sg) for sg in sequencing_groups]

    @strawberry.field()
    async def analyses(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'Project',
        type: GraphQLFilter[str] | None = None,
        status: GraphQLFilter[GraphQLAnalysisStatus] | None = None,
        active: GraphQLFilter[bool] | None = None,
        meta: GraphQLMetaFilter | None = None,
        timestamp_completed: GraphQLFilter[datetime.datetime] | None = None,
        ids: GraphQLFilter[int] | None = None,
    ) -> list['GraphQLAnalysis']:
        connection = info.context['connection']
        internal_analysis = await AnalysisLayer(connection).query(
            AnalysisFilter(
                id=ids.to_internal_filter() if ids else None,
                type=type.to_internal_filter() if type else None,
                status=(
                    status.to_internal_filter()
                    if status
                    else GenericFilter(eq=AnalysisStatus.COMPLETED)
                ),
                active=active.to_internal_filter() if active else None,
                project=GenericFilter(eq=root.id),
                meta=meta,
                timestamp_completed=(
                    timestamp_completed.to_internal_filter()
                    if timestamp_completed
                    else None
                ),
            )
        )
        return [GraphQLAnalysis.from_internal(a) for a in internal_analysis]

    @strawberry.field()
    async def cohorts(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'Project',
        id: GraphQLFilter[str] | None = None,
        name: GraphQLFilter[str] | None = None,
        author: GraphQLFilter[str] | None = None,
        template_id: GraphQLFilter[str] | None = None,
        timestamp: GraphQLFilter[datetime.datetime] | None = None,
    ) -> list['GraphQLCohort']:
        connection = info.context['connection']

        c_filter = CohortFilter(
            id=id.to_internal_filter_mapped(cohort_id_transform_to_raw) if id else None,
            name=name.to_internal_filter() if name else None,
            author=author.to_internal_filter() if author else None,
            template_id=(
                template_id.to_internal_filter_mapped(
                    cohort_template_id_transform_to_raw
                )
                if template_id
                else None
            ),
            timestamp=timestamp.to_internal_filter() if timestamp else None,
            project=GenericFilter(eq=root.id),
        )

        cohorts = await CohortLayer(connection).query(c_filter)
        return [GraphQLCohort.from_internal(c) for c in cohorts]

    @strawberry.field()
    async def discussion(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLProject'
    ) -> GraphQLDiscussion:
        loader = info.context['loaders'][LoaderKeys.COMMENTS_FOR_PROJECT_IDS]
        discussion = await loader.load(root.id)
        return GraphQLDiscussion.from_internal(discussion)

    @strawberry.field()
    async def sequencing_group_history(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLProject'
    ) -> list['GraphQLSequencingGroupsByDate']:
        split_technology = False
        for field in info.selected_fields[0].selections:
            if field.name == 'technology':
                split_technology = True
                break

        loader = info.context['loaders'][
            LoaderKeys.SEQUENCING_GROUPS_COUNTS_FOR_PROJECT
        ]

        counts = await loader.load(root.id)
        # Used for grouping counts together under the same type.
        coalesced_by_type: dict[datetime.date, dict[str, int]] = defaultdict(
            lambda: defaultdict(lambda: 0)
        )

        results: list[GraphQLSequencingGroupsByDate] = []
        for month, type_dict in sorted(counts.items(), key=lambda x: x[0]):
            for type, tech_dict in type_dict.items():
                for tech, count in tech_dict.items():
                    coalesced_by_type[month][type] += count
                    if split_technology:
                        results.append(GraphQLSequencingGroupsByDate(date=month, type=type, technology=tech, count=count))

        if split_technology:
            return results
        
        for month, type_dict in coalesced_by_type.items():
            for type, count in type_dict.items():
                results.append(GraphQLSequencingGroupsByDate(date=month, type=type, technology='', count=count))

        return results


@strawberry.type
class GraphQLAuditLog:
    """AuditLog GraphQL model"""

    id: int
    author: str
    timestamp: datetime.datetime
    ar_guid: str | None
    comment: str | None
    meta: strawberry.scalars.JSON

    @staticmethod
    def from_internal(audit_log: AuditLogInternal) -> 'GraphQLAuditLog':
        return GraphQLAuditLog(
            id=audit_log.id,
            author=audit_log.author,
            timestamp=audit_log.timestamp,
            ar_guid=audit_log.ar_guid,
            comment=audit_log.comment,
            meta=audit_log.meta,
        )


@strawberry.type
class GraphQLAnalysis:
    """Analysis GraphQL model"""

    id: int
    type: str
    status: strawberry.enum(AnalysisStatus)  # type: ignore
    output: str | None
    outputs: strawberry.scalars.JSON | None
    timestamp_completed: datetime.datetime | None = None
    active: bool
    meta: strawberry.scalars.JSON | None

    project_id: strawberry.Private[int]

    @staticmethod
    def from_internal(internal: AnalysisInternal) -> 'GraphQLAnalysis':
        if not internal.id:
            raise ValueError('Analysis must have an id')
        return GraphQLAnalysis(
            id=internal.id,
            type=internal.type,
            status=internal.status,
            output=internal.output,
            outputs=internal.outputs,
            timestamp_completed=internal.timestamp_completed,
            active=internal.active,
            meta=internal.meta,
            project_id=internal.project,
        )

    @strawberry.field
    async def sequencing_groups(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLAnalysis'
    ) -> list['GraphQLSequencingGroup']:
        loader = info.context['loaders'][LoaderKeys.SEQUENCING_GROUPS_FOR_ANALYSIS]
        sgs = await loader.load(root.id)
        return [GraphQLSequencingGroup.from_internal(sg) for sg in sgs]

    @strawberry.field
    async def project(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLAnalysis'
    ) -> GraphQLProject:
        loader = info.context['loaders'][LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)

    @strawberry.field
    async def audit_logs(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLAnalysis'
    ) -> list[GraphQLAuditLog]:
        loader = info.context['loaders'][LoaderKeys.AUDIT_LOGS_BY_ANALYSIS_IDS]
        audit_logs = await loader.load(root.id)
        return [GraphQLAuditLog.from_internal(audit_log) for audit_log in audit_logs]


@strawberry.type
class GraphQLFamily:
    """GraphQL Family"""

    id: int
    external_id: str
    external_ids: strawberry.scalars.JSON

    description: str | None
    coded_phenotype: str | None

    # internal
    project_id: strawberry.Private[int]

    @staticmethod
    def from_internal(internal: FamilyInternal) -> 'GraphQLFamily':
        return GraphQLFamily(
            id=internal.id,
            external_id=internal.external_ids[PRIMARY_EXTERNAL_ORG],
            external_ids=internal.external_ids or {},
            description=internal.description,
            coded_phenotype=internal.coded_phenotype,
            project_id=internal.project,
        )

    @strawberry.field
    async def project(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLFamily'
    ) -> GraphQLProject:
        loader = info.context['loaders'][LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)

    @strawberry.field
    async def participants(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLFamily'
    ) -> list['GraphQLParticipant']:
        participants = await info.context['loaders'][
            LoaderKeys.PARTICIPANTS_FOR_FAMILIES
        ].load(root.id)
        return [GraphQLParticipant.from_internal(p) for p in participants]

    @strawberry.field
    async def family_participants(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLFamily'
    ) -> list['GraphQLFamilyParticipant']:
        family_participants = await info.context['loaders'][
            LoaderKeys.FAMILY_PARTICIPANTS_FOR_FAMILIES
        ].load(root.id)
        return [
            GraphQLFamilyParticipant.from_internal(fp) for fp in family_participants
        ]

    @strawberry.field()
    async def discussion(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLFamily'
    ) -> GraphQLDiscussion:
        loader = info.context['loaders'][LoaderKeys.COMMENTS_FOR_FAMILY_IDS]
        discussion = await loader.load(root.id)
        return GraphQLDiscussion.from_internal(discussion)


@strawberry.type
class GraphQLFamilyParticipant:
    """
    A FamilyParticipant, an individual in a family, noting that a Family is bounded
      by some 'affected' attribute
    """

    affected: int | None
    notes: str | None

    participant_id: strawberry.Private[int]
    family_id: strawberry.Private[int]

    @strawberry.field
    async def participant(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLFamilyParticipant'
    ) -> 'GraphQLParticipant':
        loader = info.context['loaders'][LoaderKeys.PARTICIPANTS_FOR_IDS]
        participant = await loader.load(root.participant_id)
        return GraphQLParticipant.from_internal(participant)

    @strawberry.field
    async def family(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLFamilyParticipant'
    ) -> GraphQLFamily:
        loader = info.context['loaders'][LoaderKeys.FAMILIES_FOR_IDS]
        family = await loader.load(root.family_id)
        return GraphQLFamily.from_internal(family)

    @staticmethod
    def from_internal(internal: PedRowInternal) -> 'GraphQLFamilyParticipant':
        return GraphQLFamilyParticipant(
            affected=internal.affected,
            notes=internal.notes,
            participant_id=internal.individual_id,
            family_id=internal.family_id,
        )


@strawberry.type
class GraphQLParticipant:
    """Participant GraphQL model"""

    id: int
    external_id: str
    external_ids: strawberry.scalars.JSON
    meta: strawberry.scalars.JSON

    reported_sex: int | None
    reported_gender: str | None
    karyotype: str | None

    project_id: strawberry.Private[int]
    audit_log_id: strawberry.Private[int | None]

    @staticmethod
    def from_internal(internal: ParticipantInternal) -> 'GraphQLParticipant':
        return GraphQLParticipant(
            id=internal.id,
            external_id=internal.external_ids[PRIMARY_EXTERNAL_ORG],
            external_ids=internal.external_ids or {},
            meta=internal.meta,
            reported_sex=internal.reported_sex,
            reported_gender=internal.reported_gender,
            karyotype=internal.karyotype,
            project_id=internal.project,
            audit_log_id=internal.audit_log_id,
        )

    @strawberry.field
    async def samples(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'GraphQLParticipant',
        type: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
        active: GraphQLFilter[bool] | None = None,
    ) -> list['GraphQLSample']:
        filter_ = SampleFilter(
            type=type.to_internal_filter() if type else None,
            meta=graphql_meta_filter_to_internal_filter(meta),
            active=active.to_internal_filter() if active else GenericFilter(eq=True),
        )
        q = {'id': root.id, 'filter': filter_}

        samples = await info.context['loaders'][
            LoaderKeys.SAMPLES_FOR_PARTICIPANTS
        ].load(q)
        return [GraphQLSample.from_internal(s) for s in samples]

    @strawberry.field
    async def phenotypes(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLParticipant'
    ) -> strawberry.scalars.JSON:
        loader = info.context['loaders'][LoaderKeys.PHENOTYPES_FOR_PARTICIPANTS]
        return await loader.load(root.id)

    @strawberry.field
    async def families(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLParticipant'
    ) -> list[GraphQLFamily]:
        fams = await info.context['loaders'][LoaderKeys.FAMILIES_FOR_PARTICIPANTS].load(
            root.id
        )
        return [GraphQLFamily.from_internal(f) for f in fams]

    @strawberry.field
    async def family_participants(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLParticipant'
    ) -> list[GraphQLFamilyParticipant]:
        family_participants = await info.context['loaders'][
            LoaderKeys.FAMILY_PARTICIPANTS_FOR_PARTICIPANTS
        ].load(root.id)
        return [
            GraphQLFamilyParticipant.from_internal(fp) for fp in family_participants
        ]

    @strawberry.field
    async def project(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLParticipant'
    ) -> GraphQLProject:
        loader = info.context['loaders'][LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)

    @strawberry.field
    async def audit_log(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLParticipant'
    ) -> GraphQLAuditLog | None:
        if root.audit_log_id is None:
            return None
        loader = info.context['loaders'][LoaderKeys.AUDIT_LOGS_BY_IDS]
        audit_log = await loader.load(root.audit_log_id)
        return GraphQLAuditLog.from_internal(audit_log)

    @strawberry.field()
    async def discussion(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLParticipant'
    ) -> GraphQLDiscussion:
        loader = info.context['loaders'][LoaderKeys.COMMENTS_FOR_PARTICIPANT_IDS]
        discussion = await loader.load(root.id)
        return GraphQLDiscussion.from_internal(discussion)


@strawberry.type
class GraphQLSample:
    """Sample GraphQL model"""

    id: str
    external_id: str
    external_ids: strawberry.scalars.JSON
    active: bool
    meta: strawberry.scalars.JSON
    type: str

    # keep as integers, because they're useful to reference in the fields below
    internal_id: strawberry.Private[int]
    participant_id: strawberry.Private[int]
    project_id: strawberry.Private[int]
    root_id: strawberry.Private[int | None]
    parent_id: strawberry.Private[int | None]

    @staticmethod
    def from_internal(sample: SampleInternal) -> 'GraphQLSample':
        return GraphQLSample(
            id=sample_id_format(sample.id),
            external_id=sample.external_ids[PRIMARY_EXTERNAL_ORG],
            external_ids=sample.external_ids or {},
            active=sample.active,
            meta=sample.meta,
            type=sample.type,
            # internals
            internal_id=sample.id,
            participant_id=sample.participant_id,
            project_id=sample.project,
            root_id=sample.sample_root_id,
            parent_id=sample.sample_parent_id,
        )

    @strawberry.field
    async def participant(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLSample'
    ) -> GraphQLParticipant | None:
        if root.participant_id is None:
            return None
        loader_participants_for_ids = info.context['loaders'][
            LoaderKeys.PARTICIPANTS_FOR_IDS
        ]
        participant = await loader_participants_for_ids.load(root.participant_id)
        return GraphQLParticipant.from_internal(participant)

    @strawberry.field
    async def assays(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'GraphQLSample',
        type: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
    ) -> list['GraphQLAssay']:
        loader_assays_for_sample_ids = info.context['loaders'][
            LoaderKeys.ASSAYS_FOR_SAMPLES
        ]
        filter_ = AssayFilter(
            type=type.to_internal_filter() if type else None,
            meta=meta,
        )
        assays = await loader_assays_for_sample_ids.load(
            {'id': root.internal_id, 'filter': filter_}
        )
        return [GraphQLAssay.from_internal(assay) for assay in assays]

    @strawberry.field
    async def project(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLSample'
    ) -> GraphQLProject:
        project = await info.context['loaders'][LoaderKeys.PROJECTS_FOR_IDS].load(
            root.project_id
        )
        return GraphQLProject.from_internal(project)

    @strawberry.field
    async def sequencing_groups(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'GraphQLSample',
        id: GraphQLFilter[str] | None = None,
        type: GraphQLFilter[str] | None = None,
        technology: GraphQLFilter[str] | None = None,
        platform: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
        active_only: GraphQLFilter[bool] | None = None,
    ) -> list['GraphQLSequencingGroup']:
        loader = info.context['loaders'][LoaderKeys.SEQUENCING_GROUPS_FOR_SAMPLES]

        _filter = SequencingGroupFilter(
            id=(
                id.to_internal_filter_mapped(sequencing_group_id_transform_to_raw)
                if id
                else None
            ),
            meta=graphql_meta_filter_to_internal_filter(meta),
            type=type.to_internal_filter() if type else None,
            technology=technology.to_internal_filter() if technology else None,
            platform=platform.to_internal_filter() if platform else None,
            active_only=(
                active_only.to_internal_filter()
                if active_only
                else GenericFilter(eq=True)
            ),
        )
        obj = {'id': root.internal_id, 'filter': _filter}
        sequencing_groups = await loader.load(obj)

        return [GraphQLSequencingGroup.from_internal(sg) for sg in sequencing_groups]

    @strawberry.field
    async def parent_sample(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'GraphQLSample',
    ) -> 'GraphQLSample | None':
        if root.parent_id is None:
            return None
        loader = info.context['loaders'][LoaderKeys.SAMPLES_FOR_IDS]
        parent = await loader.load(root.parent_id)
        return GraphQLSample.from_internal(parent)

    @strawberry.field
    async def root_sample(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'GraphQLSample',
    ) -> 'GraphQLSample | None':
        if root.root_id is None:
            return None
        loader = info.context['loaders'][LoaderKeys.SAMPLES_FOR_IDS]
        root_sample = await loader.load(root.root_id)
        return GraphQLSample.from_internal(root_sample)

    @strawberry.field
    async def nested_samples(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'GraphQLSample',
        type: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
    ) -> list['GraphQLSample']:
        loader = info.context['loaders'][LoaderKeys.SAMPLES_FOR_PARENTS]
        nested_samples = await loader.load(
            {
                'id': root.internal_id,
                'filter_': SampleFilter(
                    type=type.to_internal_filter() if type else None,
                    meta=graphql_meta_filter_to_internal_filter(meta),
                ),
            }
        )
        return [GraphQLSample.from_internal(s) for s in nested_samples]

    @strawberry.field()
    async def discussion(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLSample'
    ) -> GraphQLDiscussion:
        loader = info.context['loaders'][LoaderKeys.COMMENTS_FOR_SAMPLE_IDS]
        discussion = await loader.load(root.internal_id)
        return GraphQLDiscussion.from_internal(discussion)


@strawberry.type
class GraphQLSequencingGroup:
    """SequencingGroup GraphQL model"""

    id: str
    type: str
    technology: str
    platform: str
    meta: strawberry.scalars.JSON
    external_ids: strawberry.scalars.JSON
    archived: bool | None

    internal_id: strawberry.Private[int]
    sample_id: strawberry.Private[int]

    @staticmethod
    def from_internal(
        internal: SequencingGroupInternal,
    ) -> 'GraphQLSequencingGroup':
        if not internal.id:
            raise ValueError('SequencingGroup must have an id')

        return GraphQLSequencingGroup(
            id=sequencing_group_id_format(internal.id),
            type=internal.type,
            technology=internal.technology,
            platform=internal.platform,
            meta=internal.meta,
            external_ids=internal.external_ids or {},
            archived=internal.archived,
            # internal
            internal_id=internal.id,
            sample_id=internal.sample_id,
        )

    @strawberry.field
    async def sample(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLSequencingGroup'
    ) -> GraphQLSample:
        loader = info.context['loaders'][LoaderKeys.SAMPLES_FOR_IDS]
        sample = await loader.load(root.sample_id)
        return GraphQLSample.from_internal(sample)

    @strawberry.field
    async def analyses(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'GraphQLSequencingGroup',
        status: GraphQLFilter[GraphQLAnalysisStatus] | None = None,
        type: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
        active: GraphQLFilter[bool] | None = None,
        project: GraphQLFilter[str] | None = None,
    ) -> list[GraphQLAnalysis]:
        connection = info.context['connection']
        loader = info.context['loaders'][LoaderKeys.ANALYSES_FOR_SEQUENCING_GROUPS]

        _project_filter: GenericFilter[ProjectId] | None = None
        if project:
            project_names = project.all_values()
            projects = connection.get_and_check_access_to_projects_for_names(
                project_names=project_names,
                allowed_roles=ReadAccessRoles,
            )
            project_id_map: dict[str, int] = {
                p.name: p.id for p in projects if p.name and p.id
            }
            _project_filter = project.to_internal_filter_mapped(
                lambda p: project_id_map[p]
            )

        analyses = await loader.load(
            {
                'id': root.internal_id,
                'filter_': AnalysisFilter(
                    status=status.to_internal_filter() if status else None,
                    type=type.to_internal_filter() if type else None,
                    meta=graphql_meta_filter_to_internal_filter(meta),
                    active=(
                        active.to_internal_filter()
                        if active
                        else GenericFilter(eq=True)
                    ),
                    project=_project_filter,
                ),
            }
        )
        return [GraphQLAnalysis.from_internal(a) for a in analyses]

    @strawberry.field
    async def assays(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLSequencingGroup'
    ) -> list['GraphQLAssay']:
        loader = info.context['loaders'][LoaderKeys.ASSAYS_FOR_SEQUENCING_GROUPS]
        assays = await loader.load(root.internal_id)
        return [GraphQLAssay.from_internal(assay) for assay in assays]

    @strawberry.field()
    async def discussion(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLSequencingGroup'
    ) -> GraphQLDiscussion:
        loader = info.context['loaders'][LoaderKeys.COMMENTS_FOR_SEQUENCING_GROUP_IDS]
        discussion = await loader.load(root.internal_id)
        return GraphQLDiscussion.from_internal(discussion)


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
        if not internal.id:
            raise ValueError('Assay must have an id')

        return GraphQLAssay(
            id=internal.id,
            type=internal.type,
            meta=internal.meta,
            external_ids=internal.external_ids or {},
            # internal
            sample_id=internal.sample_id,
        )

    @strawberry.field
    async def sample(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLAssay'
    ) -> GraphQLSample:
        loader = info.context['loaders'][LoaderKeys.SAMPLES_FOR_IDS]
        sample = await loader.load(root.sample_id)
        return GraphQLSample.from_internal(sample)

    @strawberry.field()
    async def discussion(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLAssay'
    ) -> GraphQLDiscussion:
        loader = info.context['loaders'][LoaderKeys.COMMENTS_FOR_ASSAY_IDS]
        discussion = await loader.load(root.id)
        return GraphQLDiscussion.from_internal(discussion)


@strawberry.type
class GraphQLAnalysisRunner:
    """AnalysisRunner GraphQL model"""

    ar_guid: str
    output_path: str

    timestamp: datetime.datetime
    access_level: str
    repository: str
    commit: str
    script: str
    description: str
    driver_image: str
    config_path: str | None
    cwd: str | None
    environment: str
    hail_version: str | None
    batch_url: str
    submitting_user: str
    meta: strawberry.scalars.JSON

    internal_project: strawberry.Private[int]

    @staticmethod
    def from_internal(internal: AnalysisRunnerInternal) -> 'GraphQLAnalysisRunner':
        return GraphQLAnalysisRunner(
            ar_guid=internal.ar_guid,
            timestamp=internal.timestamp,
            access_level=internal.access_level,
            repository=internal.repository,
            commit=internal.commit,
            script=internal.script,
            description=internal.description,
            driver_image=internal.driver_image,
            config_path=internal.config_path,
            cwd=internal.cwd,
            environment=internal.environment,
            hail_version=internal.hail_version,
            batch_url=internal.batch_url,
            submitting_user=internal.submitting_user,
            meta=internal.meta,
            output_path=internal.output_path,
            # internal
            internal_project=internal.project,
        )

    @strawberry.field
    async def project(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLAnalysisRunner'
    ) -> GraphQLProject:
        loader = info.context['loaders'][LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.internal_project)
        return GraphQLProject.from_internal(project)


@strawberry.type
class GraphQLMetamistSettings:
    """
    Metamist settings that are useful to know on the client and in scripts
    """

    sample_prefix: str
    cohort_prefix: str
    sequencing_group_prefix: str
    primary_external_id_name: str


@strawberry.type
class GraphQLViewer:
    """
    The "Viewer" construct is the current user and contains information about the
    current user and their permissions
    """

    username: str
    projects: list[GraphQLProject]


@strawberry.type
class GraphQLSequencingGroupsByDate:
    """
    GraphQL representation of the number of Sequencing Groups of a given type that exist at a given date.
    """

    date: datetime.date
    type: str
    technology: str
    count: int

    @staticmethod
    def from_dict(
        date_type_count_map: dict[datetime.date, dict[str, int]], split_technology: bool
    ):
        entries: list[GraphQLSequencingGroupsByDate] = []
        for month, type_counts in sorted(
            date_type_count_map.items(), key=lambda x: x[0]
        ):
            for key, count in type_counts.items():
                type, tech = key.split(':') if split_technology else (key, '')
                entries.append(
                    GraphQLSequencingGroupsByDate(
                        date=month, type=type, technology=tech, count=count
                    )
                )

        return entries


@strawberry.type
class Query:  # entry point to graphql.
    """GraphQL Queries"""

    @strawberry.field()
    def enum(self, info: Info[GraphQLContext, 'Query']) -> GraphQLEnum:  # type: ignore
        return GraphQLEnum()

    @strawberry.field()
    def metamist_settings(self) -> GraphQLMetamistSettings:
        return GraphQLMetamistSettings(
            sample_prefix=SAMPLE_PREFIX,
            cohort_prefix=COHORT_PREFIX,
            sequencing_group_prefix=SEQUENCING_GROUP_PREFIX,
            primary_external_id_name=PRIMARY_EXTERNAL_ORG,
        )

    @strawberry.field()
    async def viewer(
        self,
        info: Info[GraphQLContext, 'Query'],
    ) -> GraphQLViewer:
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
                id.to_internal_filter_mapped(cohort_template_id_transform_to_raw)
                if id
                else None
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
        connection = info.context['connection']
        slayer = AssayLayer(connection)
        assay = await slayer.get_assay_by_id(id)
        return GraphQLAssay.from_internal(assay)

    @strawberry.field
    async def participant(
        self, info: Info[GraphQLContext, 'Query'], id: int
    ) -> GraphQLParticipant:
        loader = info.context['loaders'][LoaderKeys.PARTICIPANTS_FOR_IDS]
        return GraphQLParticipant.from_internal(await loader.load(id))

    @strawberry.field()
    async def family(
        self, info: Info[GraphQLContext, 'Query'], family_id: int
    ) -> GraphQLFamily:
        connection = info.context['connection']
        family = await FamilyLayer(connection).get_family_by_internal_id(family_id)
        return GraphQLFamily.from_internal(family)

    @strawberry.field
    async def my_projects(
        self, info: Info[GraphQLContext, 'Query']
    ) -> list[GraphQLProject]:
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
        connection = info.context['connection']
        analyses = await AnalysisLayer(connection).query(
            AnalysisFilter(id=id.to_internal_filter())
        )
        return [GraphQLAnalysis.from_internal(a) for a in analyses]


schema = strawberry.Schema(
    query=Query, mutation=Mutation, extensions=[QueryDepthLimiter(max_depth=10)]
)
MetamistGraphQLRouter: GraphQLRouter = GraphQLRouter(
    schema, graphql_ide='graphiql', context_getter=get_context
)
