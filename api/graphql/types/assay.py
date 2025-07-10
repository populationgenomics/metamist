# pylint: disable=reimported,import-outside-toplevel,wrong-import-position
from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext, LoaderKeys
from models.models import (
    AssayInternal,
)

if TYPE_CHECKING:
    from api.graphql.schema import Query
    from api.graphql.types.comments import GraphQLDiscussion
    from api.graphql.types.sample import GraphQLSample


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
        """Convert an internal AssayInternal model to a GraphQLAssay instance."""
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
    ) -> Annotated['GraphQLSample', strawberry.lazy('api.graphql.types.sample')]:
        """Get the sample associated with this assay."""
        from api.graphql.types.sample import GraphQLSample

        loader = info.context['loaders'][LoaderKeys.SAMPLES_FOR_IDS]
        sample = await loader.load(root.sample_id)
        return GraphQLSample.from_internal(sample)

    @strawberry.field()
    async def discussion(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLAssay'
    ) -> Annotated['GraphQLDiscussion', strawberry.lazy('api.graphql.types.comments')]:
        """Get the discussion associated with this assay."""
        from api.graphql.types.comments import GraphQLDiscussion

        loader = info.context['loaders'][LoaderKeys.COMMENTS_FOR_ASSAY_IDS]
        discussion = await loader.load(root.id)
        return GraphQLDiscussion.from_internal(discussion)


from api.graphql.types.comments import GraphQLDiscussion
from api.graphql.types.sample import GraphQLSample
