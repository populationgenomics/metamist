# pylint: disable=redefined-builtin, import-outside-toplevel
from typing import Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from api.graphql.mutations.assay import AssayUpsertInput
from api.graphql.types.comments import GraphQLComment
from api.graphql.types.sequencing_group import GraphQLSequencingGroup
from db.python.connect import Connection
from db.python.layers.comment import CommentLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from models.models.comment import CommentEntityType
from models.models.project import FullWriteAccessRoles
from models.models.sequencing_group import SequencingGroupUpsertInternal
from models.utils.sequencing_group_id_format import sequencing_group_id_transform_to_raw


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


@strawberry.input
class SequencingGroupMetaUpdateInput:
    """Sequencing group meta update input"""

    id: str | None = None
    meta: strawberry.scalars.JSON | None = None


@strawberry.type
class SequencingGroupMutations:
    """Sequencing Group Mutations"""

    @strawberry.mutation
    async def add_comment(
        self,
        content: str,
        id: str,
        info: Info[GraphQLContext, 'SequencingGroupMutations'],
    ) -> Annotated[GraphQLComment, strawberry.lazy('api.graphql.schema')]:
        """Add a comment to a sequencing group"""
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
        project: str,
        sequencing_group: SequencingGroupMetaUpdateInput,
        info: Info,
    ) -> Annotated[GraphQLSequencingGroup, strawberry.lazy('api.graphql.schema')]:
        """Update the meta fields of a sequencing group"""
        connection: Connection = info.context['connection']

        # Having to do the permission check here is a bit of a hack, we ideally want to
        # do it in the layer but this will involve potentially refactoring various other
        # parts of the codebase. Should be looked at during a future refactor overhaul.
        connection.check_access_to_projects_for_names([project], FullWriteAccessRoles)

        if not sequencing_group.id:
            raise ValueError('Sequencing group ID must be provided for update')

        slayer = SequencingGroupLayer(connection)
        updated_sg: SequencingGroupUpsertInternal = (
            await slayer.upsert_sequencing_groups(
                [
                    SequencingGroupUpsertInternal(
                        id=sequencing_group_id_transform_to_raw(sequencing_group.id),
                        meta=sequencing_group.meta,  # type: ignore [arg-type]
                    )
                ]
            )
        )[0]
        full_updated_sg = await slayer.get_sequencing_group_by_id(updated_sg.id)  # type: ignore [arg-type]
        return GraphQLSequencingGroup.from_internal(full_updated_sg)

    @strawberry.mutation
    async def archive_sequencing_groups(
        self,
        sequencing_group_ids: list[str],
        info: Info,
    ) -> list[Annotated[GraphQLSequencingGroup, strawberry.lazy('api.graphql.schema')]]:
        """Archive a list of sequencing groups"""
        connection: Connection = info.context['connection']

        slayer = SequencingGroupLayer(connection)
        raw_ids = [
            sequencing_group_id_transform_to_raw(sgid) for sgid in sequencing_group_ids
        ]
        await slayer.archive_sequencing_groups(raw_ids)

        updated_sgs = await slayer.get_sequencing_groups_by_ids(raw_ids)
        return [GraphQLSequencingGroup.from_internal(sg) for sg in updated_sgs]
