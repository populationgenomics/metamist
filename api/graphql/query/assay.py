from typing import TYPE_CHECKING, Annotated, Self

import strawberry

from api.graphql.context import GraphQLContext
from api.graphql.query.comment_loaders import CommentLoaderKeys
from api.graphql.query.sample_loaders import SampleLoaderKeys
from models.models.assay import AssayInternal

if TYPE_CHECKING:
    from .comment import GraphQLDiscussion
    from .sample import GraphQLSample


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
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLSample', strawberry.lazy('.sample')]:
        loader = info.context.loaders[SampleLoaderKeys.SAMPLES_FOR_IDS]
        sample = await loader.load(root.sample_id)
        return GraphQLSample.from_internal(sample)

    @strawberry.field()
    async def discussion(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLDiscussion', strawberry.lazy('.comment')]:
        loader = info.context.loaders[CommentLoaderKeys.COMMENTS_FOR_ASSAY_IDS]
        discussion = await loader.load(root.id)
        return GraphQLDiscussion.from_internal(discussion)
