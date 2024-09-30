# pylint: disable=redefined-builtin, import-outside-toplevel
from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from api.graphql.mutations.assay import AssayUpsertInput
from db.python.layers.comment import CommentLayer
from models.models.comment import CommentEntityType
from models.utils.sequencing_group_id_format import sequencing_group_id_transform_to_raw

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment


@strawberry.input  # type: ignore [misc]
class SequencingGroupUpsertInput:
    """Sequencing group upsert input"""

    id: str  # should really be int | str | None but strawberry throws an error "Type `int` cannot be used in a GraphQL Union"
    type: str | None
    technology: str | None
    platform: str | None
    meta: strawberry.scalars.JSON | None
    sample_id: str | None
    external_ids: strawberry.scalars.JSON | None

    assays: list[AssayUpsertInput] | None


@strawberry.type
class SequencingGroupMutations:
    """Sequencing Group Mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: str,
        info: Info[GraphQLContext, 'SequencingGroupMutations'],
    ) -> Annotated['GraphQLComment', strawberry.lazy('api.graphql.schema')]:
        """Add a comment to a sequencing group"""
        # Import needed here to avoid circular import
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
        cl = CommentLayer(connection)
        result = await cl.add_comment_to_entity(
            entity=CommentEntityType.sequencing_group,
            entity_id=sequencing_group_id_transform_to_raw(id),
            content=content,
        )
        return GraphQLComment.from_internal(result)
