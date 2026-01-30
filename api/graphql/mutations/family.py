# pylint: disable=redefined-builtin, import-outside-toplevel

from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info
from strawberry.scalars import JSON

from api.graphql.loaders import GraphQLContext
from db.python.layers.comment import CommentLayer
from db.python.layers.family import FamilyLayer
from models.models.comment import CommentEntityType

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment, GraphQLFamily


@strawberry.input
class FamilyUpdateInput:
    """Family update type"""

    id: int
    external_ids: JSON | None = None
    description: str | None = None
    coded_phenotype: str | None = None
    meta: JSON | None = None


@strawberry.type
class FamilyMutations:
    """Family mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: int,
        info: Info[GraphQLContext, 'FamilyMutations'],
    ) -> Annotated['GraphQLComment', strawberry.lazy('api.graphql.schema')]:
        """Add a comment to a family"""
        # Import needed here to avoid circular import
        from api.graphql.schema import GraphQLComment

        connection = info.context['connection']
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
    ) -> Annotated['GraphQLFamily', strawberry.lazy('api.graphql.schema')]:
        """Update information for a single family"""
        from api.graphql.schema import GraphQLFamily

        connection = info.context['connection']
        flayer = FamilyLayer(connection)
        await flayer.update_family(
            id_=family.id,
            external_ids=family.external_ids,  # type: ignore [arg-type]
            description=family.description,  # type: ignore [arg-type]
            coded_phenotype=family.coded_phenotype,  # type: ignore [arg-type]
            meta=family.meta,  # type: ignore [arg-type]
        )
        updated_family = await flayer.get_family_by_internal_id(family.id)  # type: ignore [arg-type]

        return GraphQLFamily.from_internal(updated_family)
