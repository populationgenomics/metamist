# pylint: disable=reimported,import-outside-toplevel,wrong-import-position
import datetime
from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext, LoaderKeys
from models.models.comment import (
    CommentEntityType,
    CommentInternal,
    CommentStatus,
    CommentVersionInternal,
    DiscussionInternal,
)

if TYPE_CHECKING:
    from api.graphql.schema import Query
    from api.graphql.types.assay import GraphQLAssay
    from api.graphql.types.family import GraphQLFamily
    from api.graphql.types.participant import GraphQLParticipant
    from api.graphql.types.project import GraphQLProject
    from api.graphql.types.sample import GraphQLSample
    from api.graphql.types.sequencing_group import GraphQLSequencingGroup


@strawberry.type
class GraphQLCommentVersion:
    """A version of a comment's content"""

    content: str
    author: str
    status: strawberry.enum(CommentStatus)  # type: ignore
    timestamp: datetime.datetime

    @staticmethod
    def from_internal(internal: CommentVersionInternal) -> 'GraphQLCommentVersion':
        """Convert a CommentVersionInternal to GraphQLCommentVersion"""
        return GraphQLCommentVersion(
            content=internal.content,
            author=internal.author,
            timestamp=internal.timestamp,
            status=internal.status,
        )


@strawberry.type
class GraphQLDiscussion:
    """A comment discussion, made up of flat lists of direct and related comments"""

    direct_comments: list['GraphQLComment']
    related_comments: list['GraphQLComment']

    @staticmethod
    def from_internal(
        internal: DiscussionInternal | None,
    ) -> 'GraphQLDiscussion':
        """Convert a DiscussionInternal to GraphQLDiscussion"""
        direct_comments = internal.direct_comments if internal is not None else []
        related_comments = internal.related_comments if internal is not None else []
        return GraphQLDiscussion(
            direct_comments=[GraphQLComment.from_internal(c) for c in direct_comments],
            related_comments=[
                GraphQLComment.from_internal(c) for c in related_comments
            ],
        )


@strawberry.type
class GraphQLComment:
    """A comment made on a entity"""

    id: int
    parentId: int | None
    content: str
    author: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    comment_entity_type: strawberry.Private[CommentEntityType]
    comment_entity_id: strawberry.Private[int]
    status: strawberry.enum(CommentStatus)  # type: ignore
    thread: list['GraphQLComment']
    versions: list[GraphQLCommentVersion]

    @strawberry.field()
    async def entity(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLComment'
    ) -> Annotated[
        'GraphQLSample | GraphQLAssay | GraphQLSequencingGroup | GraphQLProject | GraphQLParticipant | GraphQLFamily',
        strawberry.union('GraphQLCommentEntity'),
    ]:
        """Retrieve the entity this comment is associated with."""
        from api.graphql.types.assay import GraphQLAssay
        from api.graphql.types.family import GraphQLFamily
        from api.graphql.types.participant import GraphQLParticipant
        from api.graphql.types.project import GraphQLProject
        from api.graphql.types.sample import GraphQLSample
        from api.graphql.types.sequencing_group import GraphQLSequencingGroup

        entity_type = root.comment_entity_type
        entity_id = root.comment_entity_id

        match entity_type:
            case CommentEntityType.sample:
                loader = info.context['loaders'][LoaderKeys.SAMPLES_FOR_IDS]
                sample = await loader.load(entity_id)
                return GraphQLSample.from_internal(sample)

            case CommentEntityType.sequencing_group:
                loader = info.context['loaders'][LoaderKeys.SEQUENCING_GROUPS_FOR_IDS]
                sg = await loader.load(entity_id)
                return GraphQLSequencingGroup.from_internal(sg)

            case CommentEntityType.project:
                loader = info.context['loaders'][LoaderKeys.PROJECTS_FOR_IDS]
                sg = await loader.load(entity_id)
                return GraphQLProject.from_internal(sg)

            case CommentEntityType.assay:
                loader = info.context['loaders'][LoaderKeys.ASSAYS_FOR_IDS]
                ay = await loader.load(entity_id)
                return GraphQLAssay.from_internal(ay)

            case CommentEntityType.participant:
                loader = info.context['loaders'][LoaderKeys.PARTICIPANTS_FOR_IDS]
                pt = await loader.load(entity_id)
                return GraphQLParticipant.from_internal(pt)

            case CommentEntityType.family:
                loader = info.context['loaders'][LoaderKeys.FAMILIES_FOR_IDS]
                fm = await loader.load(entity_id)
                return GraphQLFamily.from_internal(fm)

    @staticmethod
    def from_internal(internal: CommentInternal) -> 'GraphQLComment':
        """Convert a CommentInternal to GraphQLComment"""
        return GraphQLComment(
            id=internal.id,
            parentId=internal.parent_id,
            content=internal.content,
            author=internal.author,
            created_at=internal.created_at,
            updated_at=internal.updated_at,
            comment_entity_type=internal.comment_entity_type,
            comment_entity_id=internal.comment_entity_id,
            thread=[GraphQLComment.from_internal(c) for c in internal.thread],
            status=internal.status,
            versions=[
                GraphQLCommentVersion.from_internal(v) for v in internal.versions
            ],
        )


from api.graphql.types.assay import GraphQLAssay
from api.graphql.types.family import GraphQLFamily
from api.graphql.types.participant import GraphQLParticipant
from api.graphql.types.project import GraphQLProject
from api.graphql.types.sample import GraphQLSample
from api.graphql.types.sequencing_group import GraphQLSequencingGroup
