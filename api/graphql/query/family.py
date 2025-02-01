from typing import TYPE_CHECKING, Annotated, Self

import strawberry

from api.graphql.context import GraphQLContext
from api.graphql.query.comment_loaders import CommentLoaderKeys
from api.graphql.query.family_loaders import FamilyLoaderKeys
from api.graphql.query.participant_loaders import ParticipantLoaderKeys
from api.graphql.query.project_loaders import ProjectLoaderKeys
from models.base import PRIMARY_EXTERNAL_ORG
from models.models.family import FamilyInternal, PedRowInternal

if TYPE_CHECKING:
    from .comment import GraphQLDiscussion
    from .participant import GraphQLParticipant
    from .project import GraphQLProject


@strawberry.type
class GraphQLFamily:
    """GraphQL Family"""

    id: int
    external_id: str
    external_ids: strawberry.scalars.JSON

    description: str | None
    coded_phenotype: str | None

    # internal
    project_id: strawberry.Private[int]

    @staticmethod
    def from_internal(internal: FamilyInternal) -> 'GraphQLFamily':
        return GraphQLFamily(
            id=internal.id,
            external_id=internal.external_ids[PRIMARY_EXTERNAL_ORG],
            external_ids=internal.external_ids or {},
            description=internal.description,
            coded_phenotype=internal.coded_phenotype,
            project_id=internal.project,
        )

    @strawberry.field
    async def project(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLProject', strawberry.lazy('.project')]:
        from .project import GraphQLProject

        loader = info.context.loaders[ProjectLoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)

    @strawberry.field
    async def participants(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> list[Annotated['GraphQLParticipant', strawberry.lazy('.participant')]]:
        from .participant import GraphQLParticipant

        participants = await info.context.loaders[
            ParticipantLoaderKeys.PARTICIPANTS_FOR_FAMILIES
        ].load(root.id)
        return [GraphQLParticipant.from_internal(p) for p in participants]

    @strawberry.field
    async def family_participants(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> list[Annotated['GraphQLFamilyParticipant', strawberry.lazy('.family')]]:
        from .family import GraphQLFamilyParticipant

        family_participants = await info.context.loaders[
            FamilyLoaderKeys.FAMILY_PARTICIPANTS_FOR_FAMILIES
        ].load(root.id)
        return [
            GraphQLFamilyParticipant.from_internal(fp) for fp in family_participants
        ]

    @strawberry.field()
    async def discussion(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLDiscussion', strawberry.lazy('.comment')]:
        from .comment import GraphQLDiscussion

        loader = info.context.loaders[CommentLoaderKeys.COMMENTS_FOR_FAMILY_IDS]
        discussion = await loader.load(root.id)
        return GraphQLDiscussion.from_internal(discussion)


@strawberry.type
class GraphQLFamilyParticipant:
    """
    A FamilyParticipant, an individual in a family, noting that a Family is bounded
      by some 'affected' attribute
    """

    affected: int | None
    notes: str | None

    participant_id: strawberry.Private[int]
    family_id: strawberry.Private[int]

    @strawberry.field
    async def participant(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLParticipant', strawberry.lazy('.participant')]:
        loader = info.context.loaders[ParticipantLoaderKeys.PARTICIPANTS_FOR_IDS]
        participant = await loader.load(root.participant_id)
        return GraphQLParticipant.from_internal(participant)

    @strawberry.field
    async def family(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLFamily', strawberry.lazy('.family')]:
        loader = info.context.loaders[FamilyLoaderKeys.FAMILIES_FOR_IDS]
        family = await loader.load(root.family_id)
        return GraphQLFamily.from_internal(family)

    @staticmethod
    def from_internal(
        internal: PedRowInternal,
    ) -> Annotated['GraphQLFamilyParticipant', strawberry.lazy('.family')]:
        from .family import GraphQLFamilyParticipant

        return GraphQLFamilyParticipant(
            affected=internal.affected,
            notes=internal.notes,
            participant_id=internal.individual_id,
            family_id=internal.family_id,
        )
