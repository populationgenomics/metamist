# pylint: disable=redefined-builtin, import-outside-toplevel
from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from api.graphql.mutations.assay import AssayUpsertInput
from db.python.connect import Connection
from db.python.layers.comment import CommentLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from models.models.comment import CommentEntityType
from models.models.project import FullWriteAccessRoles
from models.models.sequencing_group import SequencingGroupUpsertInternal
from models.utils.sequencing_group_id_format import sequencing_group_id_transform_to_raw

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment
    from api.graphql.mutations.project import ProjectMutations


@strawberry.input  # type: ignore [misc]
class SequencingGroupUpsertInput:
    """Sequencing group upsert input"""

    id: str | None = None
    type: str | None = None
    technology: str | None = None
    platform: str | None = None
    meta: strawberry.scalars.JSON | None = None
    sample_id: str | None = None
    external_ids: strawberry.scalars.JSON | None = None

    assays: list[AssayUpsertInput] | None = None


@strawberry.type
class SequencingGroupMutations:
    """Sequencing Group Mutations"""

    project_id: strawberry.Private[int]

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

    @strawberry.mutation
    async def update_sequencing_group(
        self,
        sequencing_group_id: str,
        sequencing_group: strawberry.scalars.JSON,
        info: Info,
        root: 'ProjectMutations',
    ) -> bool:
        """Update the meta fields of a sequencing group"""
        connection: Connection = info.context['connection']

        # Should be moved to the sequencing group layer
        connection.check_access_to_projects_for_ids(
            [root.project_id], FullWriteAccessRoles
        )

        st = SequencingGroupLayer(connection)
        await st.upsert_sequencing_groups(
            [
                SequencingGroupUpsertInternal(
                    id=sequencing_group_id_transform_to_raw(sequencing_group_id),
                    meta=sequencing_group,  # type: ignore [arg-type]
                )
            ]
        )
        return True
