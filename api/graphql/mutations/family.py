# pylint: disable=redefined-builtin, import-outside-toplevel

from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.scalars import JSON
from strawberry.types import Info

from api.graphql.context import GraphQLContext
from db.python.layers.comment import CommentLayer
from db.python.layers.family import FamilyLayer
from models.models.comment import CommentEntityType

if TYPE_CHECKING:
    from api.graphql.query.comment import GraphQLComment
    from api.graphql.query.family import GraphQLFamily


@strawberry.input
class FamilyUpdateInput:
    """Family update type"""

    id: int
    external_ids: JSON | None = None
    description: str | None = None
    coded_phenotype: str | None = None


@strawberry.type
class FamilyMutations:
    """Family mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: int,
        info: Info[GraphQLContext, 'FamilyMutations'],
    ) -> Annotated['GraphQLComment', strawberry.lazy('api.graphql.query.comment')]:
        """Add a comment to a family"""
        # Import needed here to avoid circular import
        from api.graphql.query.comment import GraphQLComment

        connection = info.context.connection
        cl = CommentLayer(connection)
        result = await cl.add_comment_to_entity(
            entity=CommentEntityType.family, entity_id=id, content=content
        )
        return GraphQLComment.from_internal(result)

    @strawberry.mutation
    async def update_family(
        self,
        family: FamilyUpdateInput,
        info: Info,
    ) -> Annotated['GraphQLFamily', strawberry.lazy('api.graphql.query.family')]:
        """Update information for a single family"""
        from api.graphql.query.family import GraphQLFamily

        connection = info.context.connection
        flayer = FamilyLayer(connection)
        await flayer.update_family(
            id_=family.id,
            external_ids=family.external_ids,  # type: ignore [arg-type]
            description=family.description,  # type: ignore [arg-type]
            coded_phenotype=family.coded_phenotype,  # type: ignore [arg-type]
        )
        updated_family = await flayer.get_family_by_internal_id(family.id)  # type: ignore [arg-type]

        return GraphQLFamily.from_internal(updated_family)
