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
    from api.graphql.schema import GraphQLComment


@strawberry.input
class FamilyUpdateInput:
    """Family update type"""

    id: int
    external_id: JSON | None = None
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
    ) -> bool:
        """Update information for a single family"""
        connection = info.context['connection']
        family_layer = FamilyLayer(connection)
        return await family_layer.update_family(
            id_=family.id,
            external_ids=family.external_id,
            description=family.description,
            coded_phenotype=family.coded_phenotype,
        )
