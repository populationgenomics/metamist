from typing import TYPE_CHECKING, Annotated, Self

import strawberry

from api.graphql.context import GraphQLContext
from api.graphql.filters import (
    GraphQLFilter,
    GraphQLMetaFilter,
    graphql_meta_filter_to_internal_filter,
)
from api.graphql.query.assay_loaders import AssayLoaderKeys
from api.graphql.query.comment_loaders import CommentLoaderKeys
from api.graphql.query.participant_loaders import ParticipantLoaderKeys
from api.graphql.query.project_loaders import ProjectLoaderKeys
from api.graphql.query.sample_loaders import SampleLoaderKeys
from api.graphql.query.sequencing_group_loaders import SequencingGroupLoaderKeys
from db.python.filters.generic import GenericFilter
from db.python.filters.sample import SampleFilter
from db.python.filters.sequencing_group import SequencingGroupFilter
from db.python.tables.assay import AssayFilter
from models.base import PRIMARY_EXTERNAL_ORG
from models.models.sample import SampleInternal
from models.utils.sample_id_format import sample_id_format
from models.utils.sequencing_group_id_format import sequencing_group_id_transform_to_raw

if TYPE_CHECKING:
    from .assay import GraphQLAssay
    from .comment import GraphQLDiscussion
    from .participant import GraphQLParticipant
    from .project import GraphQLProject
    from .sequencing_group import GraphQLSequencingGroup


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
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLParticipant', strawberry.lazy('.participant')] | None:
        from .participant import GraphQLParticipant

        if root.participant_id is None:
            return None
        loader_participants_for_ids = info.context.loaders[
            ParticipantLoaderKeys.PARTICIPANTS_FOR_IDS
        ]
        participant = await loader_participants_for_ids.load(root.participant_id)
        return GraphQLParticipant.from_internal(participant)

    @strawberry.field
    async def assays(
        self,
        info: strawberry.Info[GraphQLContext, None],
        root: Self,
        type: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
    ) -> list[Annotated['GraphQLAssay', strawberry.lazy('.assay')]]:
        from .assay import GraphQLAssay

        loader_assays_for_sample_ids = info.context.loaders[
            AssayLoaderKeys.ASSAYS_FOR_SAMPLES
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
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLProject', strawberry.lazy('.project')]:
        from .project import GraphQLProject

        project = await info.context.loaders[ProjectLoaderKeys.PROJECTS_FOR_IDS].load(
            root.project_id
        )
        return GraphQLProject.from_internal(project)

    @strawberry.field
    async def sequencing_groups(
        self,
        info: strawberry.Info[GraphQLContext, None],
        root: Self,
        id: GraphQLFilter[str] | None = None,
        type: GraphQLFilter[str] | None = None,
        technology: GraphQLFilter[str] | None = None,
        platform: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
        active_only: GraphQLFilter[bool] | None = None,
    ) -> list[
        Annotated['GraphQLSequencingGroup', strawberry.lazy('.sequencing_group')]
    ]:
        from .sequencing_group import GraphQLSequencingGroup

        loader = info.context.loaders[
            SequencingGroupLoaderKeys.SEQUENCING_GROUPS_FOR_SAMPLES
        ]

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
        info: strawberry.Info[GraphQLContext, None],
        root: Self,
    ) -> Annotated['GraphQLSample', strawberry.lazy('.sample')] | None:
        from .sample import GraphQLSample

        if root.parent_id is None:
            return None
        loader = info.context.loaders[SampleLoaderKeys.SAMPLES_FOR_IDS]
        parent = await loader.load(root.parent_id)
        return GraphQLSample.from_internal(parent)

    @strawberry.field
    async def root_sample(
        self,
        info: strawberry.Info[GraphQLContext, None],
        root: Self,
    ) -> Annotated['GraphQLSample', strawberry.lazy('.sample')] | None:
        from .sample import GraphQLSample

        if root.root_id is None:
            return None
        loader = info.context.loaders[SampleLoaderKeys.SAMPLES_FOR_IDS]
        root_sample = await loader.load(root.root_id)
        return GraphQLSample.from_internal(root_sample)

    @strawberry.field
    async def nested_samples(
        self,
        info: strawberry.Info[GraphQLContext, None],
        root: Self,
        type: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
    ) -> list[Annotated['GraphQLSample', strawberry.lazy('.sample')]]:
        from .sample import GraphQLSample

        loader = info.context.loaders[SampleLoaderKeys.SAMPLES_FOR_PARENTS]
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
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLDiscussion', strawberry.lazy('.comment')]:
        from .comment import GraphQLDiscussion

        loader = info.context.loaders[CommentLoaderKeys.COMMENTS_FOR_SAMPLE_IDS]
        discussion = await loader.load(root.internal_id)
        return GraphQLDiscussion.from_internal(discussion)
