"""
Schema for GraphQL.

Note, we silence a lot of linting here because GraphQL looks at type annotations
and defaults to decide the GraphQL schema, so it might not necessarily look correct.
"""

import datetime
from typing import (
    TYPE_CHECKING,
    Annotated,
    Self,
)

import strawberry

from api.graphql.context import GraphQLContext
from api.graphql.filters import (
    GraphQLFilter,
    GraphQLMetaFilter,
    graphql_meta_filter_to_internal_filter,
)
from api.graphql.query.comment_loaders import CommentLoaderKeys
from api.graphql.query.participant_loaders import ParticipantLoaderKeys
from api.graphql.query.sample_loaders import SampleLoaderKeys
from api.graphql.query.sequencing_group_loaders import SequencingGroupLoaderKeys
from db.python.filters import GenericFilter
from db.python.layers import (
    AnalysisLayer,
    AnalysisRunnerLayer,
    CohortLayer,
    FamilyLayer,
    OurDnaDashboardLayer,
)
from db.python.tables.analysis import AnalysisFilter
from db.python.tables.analysis_runner import AnalysisRunnerFilter
from db.python.tables.cohort import CohortFilter
from db.python.tables.family import FamilyFilter
from db.python.tables.participant import ParticipantFilter
from db.python.tables.sample import SampleFilter
from db.python.tables.sequencing_group import SequencingGroupFilter
from models.enums import AnalysisStatus
from models.models import Project
from models.models.project import (
    ProjectMemberRole,
)
from models.models.sample import sample_id_transform_to_raw
from models.utils.cohort_id_format import cohort_id_transform_to_raw
from models.utils.cohort_template_id_format import (
    cohort_template_id_transform_to_raw,
)
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_transform_to_raw,
)

from .analysis import GraphQLAnalysisStatus

if TYPE_CHECKING:
    from .analysis import GraphQLAnalysis
    from .analysis_runner import GraphQLAnalysisRunner
    from .cohort import GraphQLCohort
    from .comment import GraphQLDiscussion
    from .family import GraphQLFamily
    from .ourdna import GraphQLOurDNADashboard
    from .participant import GraphQLParticipant
    from .sample import GraphQLSample
    from .sequencing_group import GraphQLSequencingGroup


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
        info: strawberry.Info[GraphQLContext, None],
        root: Self,
        ar_guid: GraphQLFilter[str] | None = None,
        author: GraphQLFilter[str] | None = None,
        repository: GraphQLFilter[str] | None = None,
        access_level: GraphQLFilter[str] | None = None,
        environment: GraphQLFilter[str] | None = None,
    ) -> list[Annotated['GraphQLAnalysisRunner', strawberry.lazy('.analysis_runner')]]:
        from .analysis_runner import GraphQLAnalysisRunner

        connection = info.context.connection
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

    @strawberry.field
    async def ourdna_dashboard(
        self, info: strawberry.Info, root: Self
    ) -> Annotated['GraphQLOurDNADashboard', strawberry.lazy('.ourdna')]:
        from .ourdna import GraphQLOurDNADashboard

        connection = info.context.connection
        ourdna_layer = OurDnaDashboardLayer(connection)
        if not root.id:
            raise ValueError('Project must have an id')
        ourdna_dashboard = await ourdna_layer.query(project_id=root.id)
        # pylint: disable=no-member
        return GraphQLOurDNADashboard.from_pydantic(ourdna_dashboard)

    @strawberry.field()
    async def pedigree(
        self,
        info: strawberry.Info[GraphQLContext, None],
        root: Self,
        internal_family_ids: list[int] | None = None,
        replace_with_participant_external_ids: bool = True,
        replace_with_family_external_ids: bool = True,
        include_participants_not_in_families: bool = False,
        empty_participant_value: str | None = None,
    ) -> list[strawberry.scalars.JSON]:
        connection = info.context.connection
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
        info: strawberry.Info[GraphQLContext, None],
        root: Self,
        id: GraphQLFilter[int] | None = None,
        external_id: GraphQLFilter[str] | None = None,
    ) -> list[Annotated['GraphQLFamily', strawberry.lazy('.family')]]:
        from .family import GraphQLFamily

        # don't need a data loader here as we're presuming we're not often running
        # the "families" method for many projects at once. If so, we might need to fix that
        connection = info.context.connection
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
        info: strawberry.Info[GraphQLContext, None],
        root: Self,
        id: GraphQLFilter[int] | None = None,
        external_id: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
        reported_sex: GraphQLFilter[int] | None = None,
        reported_gender: GraphQLFilter[str] | None = None,
        karyotype: GraphQLFilter[str] | None = None,
    ) -> list[Annotated['GraphQLParticipant', strawberry.lazy('.participant')]]:
        from .participant import GraphQLParticipant

        loader = info.context.loaders[ParticipantLoaderKeys.PARTICIPANTS_FOR_PROJECTS]
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
        info: strawberry.Info[GraphQLContext, None],
        root: Self,
        type: GraphQLFilter[str] | None = None,
        external_id: GraphQLFilter[str] | None = None,
        id: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
        parent_id: GraphQLFilter[str] | None = None,
        root_id: GraphQLFilter[str] | None = None,
    ) -> list[Annotated['GraphQLSample', strawberry.lazy('.sample')]]:
        from .sample import GraphQLSample

        loader = info.context.loaders[SampleLoaderKeys.SAMPLES_FOR_PROJECTS]
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
        info: strawberry.Info[GraphQLContext, None],
        root: Self,
        id: GraphQLFilter[str] | None = None,
        external_id: GraphQLFilter[str] | None = None,
        type: GraphQLFilter[str] | None = None,
        technology: GraphQLFilter[str] | None = None,
        platform: GraphQLFilter[str] | None = None,
        active_only: GraphQLFilter[bool] | None = None,
    ) -> list[
        Annotated['GraphQLSequencingGroup', strawberry.lazy('.sequencing_group')]
    ]:
        from .sequencing_group import GraphQLSequencingGroup

        loader = info.context.loaders[
            SequencingGroupLoaderKeys.SEQUENCING_GROUPS_FOR_PROJECTS
        ]
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
        info: strawberry.Info[GraphQLContext, None],
        root: Self,
        type: GraphQLFilter[str] | None = None,
        status: GraphQLFilter[GraphQLAnalysisStatus] | None = None,
        active: GraphQLFilter[bool] | None = None,
        meta: GraphQLMetaFilter | None = None,
        timestamp_completed: GraphQLFilter[datetime.datetime] | None = None,
        ids: GraphQLFilter[int] | None = None,
    ) -> list[Annotated['GraphQLAnalysis', strawberry.lazy('.analysis')]]:
        from .analysis import GraphQLAnalysis

        connection = info.context.connection
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
        info: strawberry.Info[GraphQLContext, None],
        root: Self,
        id: GraphQLFilter[str] | None = None,
        name: GraphQLFilter[str] | None = None,
        author: GraphQLFilter[str] | None = None,
        template_id: GraphQLFilter[str] | None = None,
        timestamp: GraphQLFilter[datetime.datetime] | None = None,
    ) -> list[Annotated['GraphQLCohort', strawberry.lazy('.cohort')]]:
        from .cohort import GraphQLCohort

        connection = info.context.connection

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
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLDiscussion', strawberry.lazy('.comment')]:
        from .comment import GraphQLDiscussion

        loader = info.context.loaders[CommentLoaderKeys.COMMENTS_FOR_PROJECT_IDS]
        discussion = await loader.load(root.id)
        return GraphQLDiscussion.from_internal(discussion)
