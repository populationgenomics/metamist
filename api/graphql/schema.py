# pylint: disable=no-value-for-parameter,redefined-builtin,missing-function-docstring,unused-argument,too-many-lines
"""
Schema for GraphQL.

Note, we silence a lot of linting here because GraphQL looks at type annotations
and defaults to decide the GraphQL schema, so it might not necessarily look correct.
"""
import datetime
from inspect import isclass

import strawberry
from strawberry.extensions import QueryDepthLimiter
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info

from api.graphql.filters import (
    GraphQLFilter,
    GraphQLMetaFilter,
    graphql_meta_filter_to_internal_filter,
)
from api.graphql.loaders import LoaderKeys, get_context
from db.python import enum_tables
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
from db.python.tables.project import ProjectPermissionsTable
from db.python.tables.sample import SampleFilter
from db.python.tables.sequencing_group import SequencingGroupFilter
from db.python.utils import GenericFilter
from models.enums import AnalysisStatus
from models.models import (
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
from models.models.family import PedRowInternal
from models.models.project import ProjectId
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
        async def m(info: Info) -> list[str]:
            return await _enum(info.context['connection']).get()

        m.__name__ = _enum.get_enum_name()
        # m.__annotations__ = {'return': list[str]}
        return m

    enum_methods[enum.get_enum_name()] = strawberry.field(
        create_function(enum),
    )


GraphQLEnum = strawberry.type(type('GraphQLEnum', (object,), enum_methods))

GraphQLAnalysisStatus = strawberry.enum(AnalysisStatus)


# Create cohort GraphQL model
@strawberry.type
class GraphQLCohort:
    """Cohort GraphQL model"""

    id: str
    name: str
    description: str
    author: str

    @staticmethod
    def from_internal(internal: CohortInternal) -> 'GraphQLCohort':
        return GraphQLCohort(
            id=cohort_id_format(internal.id),
            name=internal.name,
            description=internal.description,
            author=internal.author,
        )

    @strawberry.field()
    async def template(
        self, info: Info, root: 'GraphQLCohort'
    ) -> 'GraphQLCohortTemplate':
        connection = info.context['connection']
        template = await CohortLayer(connection).get_template_by_cohort_id(
            cohort_id_transform_to_raw(root.id)
        )

        return GraphQLCohortTemplate.from_internal(template)

    @strawberry.field()
    async def sequencing_groups(
        self, info: Info, root: 'GraphQLCohort'
    ) -> list['GraphQLSequencingGroup']:
        connection = info.context['connection']
        cohort_layer = CohortLayer(connection)
        sg_ids = await cohort_layer.get_cohort_sequencing_group_ids(
            cohort_id_transform_to_raw(root.id)
        )

        sg_layer = SequencingGroupLayer(connection)
        sequencing_groups = await sg_layer.get_sequencing_groups_by_ids(sg_ids)
        return [GraphQLSequencingGroup.from_internal(sg) for sg in sequencing_groups]

    @strawberry.field()
    async def analyses(
        self, info: Info, root: 'GraphQLCohort'
    ) -> list['GraphQLAnalysis']:
        connection = info.context['connection']
        connection.project = root.project
        internal_analysis = await AnalysisLayer(connection).query(
            AnalysisFilter(
                cohort_id=GenericFilter(in_=[cohort_id_transform_to_raw(root.id)]),
            )
        )
        return [GraphQLAnalysis.from_internal(a) for a in internal_analysis]

    @strawberry.field()
    async def project(self, info: Info, root: 'GraphQLCohort') -> 'GraphQLProject':
        loader = info.context[LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project)
        return GraphQLProject.from_internal(project)


# Create cohort template GraphQL model
@strawberry.type
class GraphQLCohortTemplate:
    """CohortTemplate GraphQL model"""

    id: str
    name: str
    description: str
    criteria: strawberry.scalars.JSON

    @staticmethod
    def from_internal(internal: CohortTemplateInternal) -> 'GraphQLCohortTemplate':
        # At this point, the object that comes in doesn't have an ID field.
        return GraphQLCohortTemplate(
            id=cohort_template_id_format(internal.id),
            name=internal.name,
            description=internal.description,
            criteria=internal.criteria,
        )


@strawberry.type
class GraphQLProject:
    """Project GraphQL model"""

    id: int
    name: str
    dataset: str
    meta: strawberry.scalars.JSON

    @staticmethod
    def from_internal(internal: Project) -> 'GraphQLProject':
        return GraphQLProject(
            id=internal.id,
            name=internal.name,
            dataset=internal.dataset,
            meta=internal.meta,
        )

    @strawberry.field()
    async def analysis_runner(
        self,
        info: Info,
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
        info: Info,
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
        self, info: Info, root: 'Project'
    ) -> list['GraphQLParticipant']:
        loader = info.context[LoaderKeys.PARTICIPANTS_FOR_PROJECTS]
        participants = await loader.load(root.id)
        return [GraphQLParticipant.from_internal(p) for p in participants]

    @strawberry.field()
    async def samples(
        self,
        info: Info,
        root: 'GraphQLProject',
        type: GraphQLFilter[str] | None = None,
        external_id: GraphQLFilter[str] | None = None,
        id: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
    ) -> list['GraphQLSample']:
        loader = info.context[LoaderKeys.SAMPLES_FOR_PROJECTS]
        filter_ = SampleFilter(
            type=type.to_internal_filter() if type else None,
            external_id=external_id.to_internal_filter() if external_id else None,
            id=id.to_internal_filter_mapped(sample_id_transform_to_raw) if id else None,
            meta=graphql_meta_filter_to_internal_filter(meta),
        )
        samples = await loader.load({'id': root.id, 'filter': filter_})
        return [GraphQLSample.from_internal(p) for p in samples]

    @strawberry.field()
    async def sequencing_groups(
        self,
        info: Info,
        root: 'GraphQLProject',
        id: GraphQLFilter[str] | None = None,
        external_id: GraphQLFilter[str] | None = None,
        type: GraphQLFilter[str] | None = None,
        technology: GraphQLFilter[str] | None = None,
        platform: GraphQLFilter[str] | None = None,
        active_only: GraphQLFilter[bool] | None = None,
    ) -> list['GraphQLSequencingGroup']:
        loader = info.context[LoaderKeys.SEQUENCING_GROUPS_FOR_PROJECTS]
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
        info: Info,
        root: 'Project',
        type: GraphQLFilter[str] | None = None,
        status: GraphQLFilter[GraphQLAnalysisStatus] | None = None,
        active: GraphQLFilter[bool] | None = None,
        meta: GraphQLMetaFilter | None = None,
        timestamp_completed: GraphQLFilter[datetime.datetime] | None = None,
    ) -> list['GraphQLAnalysis']:
        connection = info.context['connection']
        connection.project = root.id
        internal_analysis = await AnalysisLayer(connection).query(
            AnalysisFilter(
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
        info: Info,
        root: 'Project',
        id: GraphQLFilter[int] | None = None,
        name: GraphQLFilter[str] | None = None,
        author: GraphQLFilter[str] | None = None,
        template_id: GraphQLFilter[int] | None = None,
        timestamp: GraphQLFilter[datetime.datetime] | None = None,
    ) -> list['GraphQLCohort']:
        connection = info.context['connection']
        connection.project = root.id

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
        )

        cohorts = await CohortLayer(connection).query(c_filter)
        return [GraphQLCohort.from_internal(c) for c in cohorts]


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
    timestamp_completed: datetime.datetime | None = None
    active: bool
    meta: strawberry.scalars.JSON | None

    @staticmethod
    def from_internal(internal: AnalysisInternal) -> 'GraphQLAnalysis':
        if not internal.id:
            raise ValueError('Analysis must have an id')
        return GraphQLAnalysis(
            id=internal.id,
            type=internal.type,
            status=internal.status,
            output=internal.output,
            timestamp_completed=internal.timestamp_completed,
            active=internal.active,
            meta=internal.meta,
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

    @strawberry.field
    async def audit_logs(
        self, info: Info, root: 'GraphQLAnalysis'
    ) -> list[GraphQLAuditLog]:
        loader = info.context[LoaderKeys.AUDIT_LOGS_BY_ANALYSIS_IDS]
        audit_logs = await loader.load(root.id)
        return [GraphQLAuditLog.from_internal(audit_log) for audit_log in audit_logs]


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

    @strawberry.field
    async def family_participants(
        self, info: Info, root: 'GraphQLFamily'
    ) -> list['GraphQLFamilyParticipant']:
        family_participants = await info.context[
            LoaderKeys.FAMILY_PARTICIPANTS_FOR_FAMILIES
        ].load(root.id)
        return [
            GraphQLFamilyParticipant.from_internal(fp) for fp in family_participants
        ]


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
        self, info: Info, root: 'GraphQLFamilyParticipant'
    ) -> 'GraphQLParticipant':
        loader = info.context[LoaderKeys.PARTICIPANTS_FOR_IDS]
        participant = await loader.load(root.participant_id)
        return GraphQLParticipant.from_internal(participant)

    @strawberry.field
    async def family(
        self, info: Info, root: 'GraphQLFamilyParticipant'
    ) -> GraphQLFamily:
        loader = info.context[LoaderKeys.FAMILIES_FOR_IDS]
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
            external_id=internal.external_id,
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
        info: Info,
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

        samples = await info.context[LoaderKeys.SAMPLES_FOR_PARTICIPANTS].load(q)
        return [GraphQLSample.from_internal(s) for s in samples]

    @strawberry.field
    async def phenotypes(
        self, info: Info, root: 'GraphQLParticipant'
    ) -> strawberry.scalars.JSON:
        loader = info.context[LoaderKeys.PHENOTYPES_FOR_PARTICIPANTS]
        return await loader.load(root.id)

    @strawberry.field
    async def families(
        self, info: Info, root: 'GraphQLParticipant'
    ) -> list[GraphQLFamily]:
        fams = await info.context[LoaderKeys.FAMILIES_FOR_PARTICIPANTS].load(root.id)
        return [GraphQLFamily.from_internal(f) for f in fams]

    @strawberry.field
    async def family_participants(
        self, info: Info, root: 'GraphQLParticipant'
    ) -> list[GraphQLFamilyParticipant]:
        family_participants = await info.context[
            LoaderKeys.FAMILY_PARTICIPANTS_FOR_PARTICIPANTS
        ].load(root.id)
        return [
            GraphQLFamilyParticipant.from_internal(fp) for fp in family_participants
        ]

    @strawberry.field
    async def project(self, info: Info, root: 'GraphQLParticipant') -> GraphQLProject:
        loader = info.context[LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)

    @strawberry.field
    async def audit_log(
        self, info: Info, root: 'GraphQLParticipant'
    ) -> GraphQLAuditLog | None:
        if root.audit_log_id is None:
            return None
        loader = info.context[LoaderKeys.AUDIT_LOGS_BY_IDS]
        audit_log = await loader.load(root.audit_log_id)
        return GraphQLAuditLog.from_internal(audit_log)


@strawberry.type
class GraphQLSample:
    """Sample GraphQL model"""

    id: str
    external_id: str
    active: bool
    meta: strawberry.scalars.JSON
    type: str

    # keep as integers, because they're useful to reference in the fields below
    internal_id: strawberry.Private[int]
    participant_id: strawberry.Private[int]
    project_id: strawberry.Private[int]

    @staticmethod
    def from_internal(sample: SampleInternal) -> 'GraphQLSample':
        return GraphQLSample(
            id=sample_id_format(sample.id),
            external_id=sample.external_id,
            active=sample.active,
            meta=sample.meta,
            type=sample.type,
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
        self,
        info: Info,
        root: 'GraphQLSample',
        type: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
    ) -> list['GraphQLAssay']:
        loader_assays_for_sample_ids = info.context[LoaderKeys.ASSAYS_FOR_SAMPLES]
        filter_ = AssayFilter(
            type=type.to_internal_filter() if type else None,
            meta=meta,
        )
        assays = await loader_assays_for_sample_ids.load(
            {'id': root.internal_id, 'filter': filter_}
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
        id: GraphQLFilter[str] | None = None,
        type: GraphQLFilter[str] | None = None,
        technology: GraphQLFilter[str] | None = None,
        platform: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
        active_only: GraphQLFilter[bool] | None = None,
    ) -> list['GraphQLSequencingGroup']:
        loader = info.context[LoaderKeys.SEQUENCING_GROUPS_FOR_SAMPLES]

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
        if not internal.id:
            raise ValueError('SequencingGroup must have an id')

        return GraphQLSequencingGroup(
            id=sequencing_group_id_format(internal.id),
            type=internal.type,
            technology=internal.technology,
            platform=internal.platform,
            meta=internal.meta,
            external_ids=internal.external_ids or {},
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
        status: GraphQLFilter[GraphQLAnalysisStatus] | None = None,
        type: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
        active: GraphQLFilter[bool] | None = None,
        project: GraphQLFilter[str] | None = None,
    ) -> list[GraphQLAnalysis]:
        connection = info.context['connection']
        loader = info.context[LoaderKeys.ANALYSES_FOR_SEQUENCING_GROUPS]

        _project_filter: GenericFilter[ProjectId] | None = None
        if project:
            ptable = ProjectPermissionsTable(connection)
            project_ids = project.all_values()
            projects = await ptable.get_and_check_access_to_projects_for_names(
                user=connection.author, project_names=project_ids, readonly=True
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
        self, info: Info, root: 'GraphQLSequencingGroup'
    ) -> list['GraphQLAssay']:
        loader = info.context[LoaderKeys.ASSAYS_FOR_SEQUENCING_GROUPS]
        assays = await loader.load(root.internal_id)
        return [GraphQLAssay.from_internal(assay) for assay in assays]


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
    async def sample(self, info: Info, root: 'GraphQLAssay') -> GraphQLSample:
        loader = info.context[LoaderKeys.SAMPLES_FOR_IDS]
        sample = await loader.load(root.sample_id)
        return GraphQLSample.from_internal(sample)


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
    config_path: str
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
        self, info: Info, root: 'GraphQLAnalysisRunner'
    ) -> GraphQLProject:
        loader = info.context[LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.internal_project)
        return GraphQLProject.from_internal(project)


@strawberry.type
class Query:  # entry point to graphql.
    """GraphQL Queries"""

    @strawberry.field()
    def enum(self, info: Info) -> GraphQLEnum:  # type: ignore
        return GraphQLEnum()

    @strawberry.field()
    async def cohort_templates(
        self,
        info: Info,
        id: GraphQLFilter[str] | None = None,
        project: GraphQLFilter[str] | None = None,
    ) -> list[GraphQLCohortTemplate]:
        connection = info.context['connection']
        cohort_layer = CohortLayer(connection)

        ptable = ProjectPermissionsTable(connection)
        project_name_map: dict[str, int] = {}
        project_filter = None
        if project:
            project_names = project.all_values()
            projects = await ptable.get_and_check_access_to_projects_for_names(
                user=connection.author, project_names=project_names, readonly=True
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
        return [
            GraphQLCohortTemplate.from_internal(cohort_template)
            for cohort_template in cohort_templates
        ]

    @strawberry.field()
    async def cohorts(
        self,
        info: Info,
        id: GraphQLFilter[str] | None = None,
        project: GraphQLFilter[str] | None = None,
        name: GraphQLFilter[str] | None = None,
        author: GraphQLFilter[str] | None = None,
        template_id: GraphQLFilter[int] | None = None,
    ) -> list[GraphQLCohort]:
        connection = info.context['connection']
        cohort_layer = CohortLayer(connection)

        ptable = ProjectPermissionsTable(connection)
        project_name_map: dict[str, int] = {}
        project_filter = None
        if project:
            project_names = project.all_values()
            projects = await ptable.get_and_check_access_to_projects_for_names(
                user=connection.author, project_names=project_names, readonly=True
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
    async def project(self, info: Info, name: str) -> GraphQLProject:
        connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection)
        project = await ptable.get_and_check_access_to_project_for_name(
            user=connection.author, project_name=name, readonly=True
        )
        return GraphQLProject.from_internal(project)

    @strawberry.field
    async def sample(
        self,
        info: Info,
        id: GraphQLFilter[str] | None = None,
        project: GraphQLFilter[str] | None = None,
        type: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
        external_id: GraphQLFilter[str] | None = None,
        participant_id: GraphQLFilter[int] | None = None,
        active: GraphQLFilter[bool] | None = None,
    ) -> list[GraphQLSample]:
        connection = info.context['connection']
        ptable = ProjectPermissionsTable(connection)
        slayer = SampleLayer(connection)

        if not id and not project:
            raise ValueError('Must provide either id or project')

        if external_id and not project:
            raise ValueError('Must provide project when using external_id filter')

        project_name_map: dict[str, int] = {}
        if project:
            project_names = project.all_values()
            projects = await ptable.get_and_check_access_to_projects_for_names(
                user=connection.author, project_names=project_names, readonly=True
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
        )

        samples = await slayer.query(filter_)
        return [GraphQLSample.from_internal(sample) for sample in samples]

    # pylint: disable=too-many-arguments
    @strawberry.field
    async def sequencing_groups(
        self,
        info: Info,
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
        ptable = ProjectPermissionsTable(connection)
        if not (project or sample_id or id):
            raise ValueError('Must filter by project, sample or id')

        # we list project names, but internally we want project ids
        _project_filter: GenericFilter[ProjectId] | None = None

        if project:
            project_names = project.all_values()
            projects = await ptable.get_and_check_access_to_projects_for_names(
                user=connection.author, project_names=project_names, readonly=True
            )
            project_id_map = {p.name: p.id for p in projects if p.name and p.id}
            _project_filter = project.to_internal_filter_mapped(
                lambda p: project_id_map[p]
            )

        filter_ = SequencingGroupFilter(
            project=_project_filter,
            sample_id=(
                sample_id.to_internal_filter_mapped(sample_id_transform_to_raw)
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
            assay_meta=assay_meta,
            has_cram=has_cram,
            has_gvcf=has_gvcf,
        )
        sgs = await sglayer.query(filter_)
        return [GraphQLSequencingGroup.from_internal(sg) for sg in sgs]

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
        ptable = ProjectPermissionsTable(connection)
        projects = await ptable.get_projects_accessible_by_user(
            connection.author, readonly=True
        )
        return [GraphQLProject.from_internal(p) for p in projects]

    @strawberry.field
    async def analysis_runner(
        self,
        info: Info,
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


schema = strawberry.Schema(
    query=Query, mutation=None, extensions=[QueryDepthLimiter(max_depth=10)]
)
MetamistGraphQLRouter: GraphQLRouter = GraphQLRouter(
    schema, graphiql=True, context_getter=get_context
)
