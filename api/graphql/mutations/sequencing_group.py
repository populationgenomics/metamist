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
from models.utils.sample_id_format import sample_id_transform_to_raw

if TYPE_CHECKING:
    from api.graphql.schema import GraphQLComment, GraphQLSequencingGroup
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
        sequencing_group: SequencingGroupUpsertInput,
        info: Info,
        root: 'ProjectMutations',
    ) -> Annotated['GraphQLSequencingGroup', strawberry.lazy('api.graphql.schema')]:
        """Update the meta fields of a sequencing group"""
        from api.graphql.schema import GraphQLSequencingGroup

        connection: Connection = info.context['connection']

        # Should be moved to the sequencing group layer
        connection.check_access_to_projects_for_ids(
            [root.project_id], FullWriteAccessRoles
        )

        if not sequencing_group.id:
            raise ValueError('Sequencing group ID must be provided for update')

        # TODO: Review this against the route endpoint
        slayer = SequencingGroupLayer(connection)
        updated_sg: SequencingGroupUpsertInternal = (
            await slayer.upsert_sequencing_groups(
                [
                    SequencingGroupUpsertInternal(
                        id=sequencing_group_id_transform_to_raw(sequencing_group.id),
                        type=sequencing_group.type,
                        technology=sequencing_group.technology,
                        platform=sequencing_group.platform,
                        meta=sequencing_group.meta,  # type: ignore [arg-type]
                        sample_id=sample_id_transform_to_raw(sequencing_group.sample_id)
                        if sequencing_group.sample_id
                        else None,
                        external_ids=sequencing_group.external_ids,  # type: ignore [arg-type]
                        assays=sequencing_group.assays,  # type: ignore [arg-type]
                    )
                ]
            )
        )[0]
        return GraphQLSequencingGroup.from_internal(updated_sg)
