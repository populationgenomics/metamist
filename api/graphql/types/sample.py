# pylint: disable=reimported,import-outside-toplevel,wrong-import-position,redefined-builtin
from typing import TYPE_CHECKING, Annotated, Optional

import strawberry
from strawberry.types import Info

from api.graphql.filters import (
    GraphQLFilter,
    GraphQLMetaFilter,
    graphql_meta_filter_to_internal_filter,
)
from api.graphql.loaders import GraphQLContext, LoaderKeys
from db.python.filters import GenericFilter
from db.python.tables.assay import AssayFilter
from db.python.tables.sample import SampleFilter
from db.python.tables.sequencing_group import SequencingGroupFilter
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    SampleInternal,
)
from models.utils.sample_id_format import sample_id_format
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_transform_to_raw,
)

if TYPE_CHECKING:
    from api.graphql.schema import Query
    from api.graphql.types.assay import GraphQLAssay
    from api.graphql.types.comments import GraphQLDiscussion
    from api.graphql.types.participant import GraphQLParticipant
    from api.graphql.types.project import GraphQLProject
    from api.graphql.types.sequencing_group import GraphQLSequencingGroup


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
        """Convert an internal SampleInternal model to a GraphQLSample instance."""
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
    ) -> Annotated[
        Optional['GraphQLParticipant'], strawberry.lazy('api.graphql.types.participant')  # noqa: F821
    ]:
        """Retrieve the participant associated with this sample."""
        from api.graphql.types.participant import GraphQLParticipant

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
    ) -> Annotated[list['GraphQLAssay'], strawberry.lazy('api.graphql.types.assay')]:
        """Retrieve assays associated with this sample."""
        from api.graphql.types.assay import GraphQLAssay

        loader_assays_for_sample_ids = info.context['loaders'][
            LoaderKeys.ASSAYS_FOR_SAMPLES
        ]
        filter_ = AssayFilter(
            type=type.to_internal_filter() if type else None,
            meta=meta,
        )
        assays = await loader_assays_for_sample_ids.load(
            {
                'id': root.internal_id,
                'filter': filter_,
            }
        )
        return [GraphQLAssay.from_internal(assay) for assay in assays]

    @strawberry.field
    async def project(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLSample'
    ) -> Annotated['GraphQLProject', strawberry.lazy('api.graphql.types.project')]:
        """Retrieve the project associated with this sample."""
        from api.graphql.types.project import GraphQLProject

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
    ) -> Annotated[
        list['GraphQLSequencingGroup'],
        strawberry.lazy('api.graphql.types.sequencing_group'),
    ]:
        """Retrieve sequencing groups associated with this sample."""
        from api.graphql.types.sequencing_group import GraphQLSequencingGroup

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
        """Retrieve the parent sample of this sample."""
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
        """Retrieve the root sample of this sample."""
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
        """Retrieve nested samples associated with this sample."""
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
    ) -> Annotated['GraphQLDiscussion', strawberry.lazy('api.graphql.types.comments')]:
        """Load the discussion associated with this sample."""
        from api.graphql.types.comments import GraphQLDiscussion

        loader = info.context['loaders'][LoaderKeys.COMMENTS_FOR_SAMPLE_IDS]
        discussion = await loader.load(root.internal_id)
        return GraphQLDiscussion.from_internal(discussion)


from api.graphql.types.assay import GraphQLAssay
from api.graphql.types.comments import GraphQLDiscussion
from api.graphql.types.participant import GraphQLParticipant
from api.graphql.types.project import GraphQLProject
from api.graphql.types.sequencing_group import GraphQLSequencingGroup
