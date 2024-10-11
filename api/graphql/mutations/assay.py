# pylint: disable=redefined-builtin, import-outside-toplevel

from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from api.graphql.types import AssayUpsertType
from db.python.layers.assay import AssayLayer
from db.python.layers.comment import CommentLayer
from models.models.assay import AssayUpsert
from models.models.comment import CommentEntityType

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment


@strawberry.input  # type: ignore [misc]
class AssayUpsertInput:
    """Assay upsert input"""

    id: int | None = None
    type: str | None = None
    external_ids: strawberry.scalars.JSON | None = None
    sample_id: str | None = None
    meta: strawberry.scalars.JSON | None = None


@strawberry.type
class AssayMutations:
    """Assay mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: int,
        info: Info[GraphQLContext, 'AssayMutations'],
    ) -> Annotated['GraphQLComment', strawberry.lazy('api.graphql.schema')]:
        """Add a comment to a assay"""
        # Import needed here to avoid circular import
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
        cl = CommentLayer(connection)
        result = await cl.add_comment_to_entity(
            entity=CommentEntityType.assay, entity_id=id, content=content
        )
        return GraphQLComment.from_internal(result)

    @strawberry.mutation
    async def create_assay(
        self, assay: AssayUpsertInput, info: Info
    ) -> AssayUpsertType:
        """Create new assay, attached to a sample"""
        connection = info.context['connection']
        assay_layer = AssayLayer(connection)
        upserted = await assay_layer.upsert_assay(
            AssayUpsert.from_dict(strawberry.asdict(assay)).to_internal()
        )
        return AssayUpsertType.from_upsert_internal(upserted)

    @strawberry.mutation
    async def update_assay(
        self,
        assay: AssayUpsertInput,
        info: Info,
    ) -> int:
        """Update assay for ID"""
        if not assay.id:
            raise ValueError('Assay must have an ID to update')
        connection = info.context['connection']
        assay_layer = AssayLayer(connection)
        await assay_layer.upsert_assay(
            AssayUpsert.from_dict(strawberry.asdict(assay)).to_internal()
        )

        return assay.id
