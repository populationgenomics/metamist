# pylint: disable=reimported,import-outside-toplevel,wrong-import-position
from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.loader_keys import LoaderKeys
from api.graphql.loaders import GraphQLContext
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    FamilyInternal,
)
from models.models.family import PedRowInternal

if TYPE_CHECKING:
    from api.graphql.schema import Query
    from api.graphql.types.comments import GraphQLDiscussion
    from api.graphql.types.participant import GraphQLParticipant
    from api.graphql.types.project import GraphQLProject


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
        """Convert a FamilyInternal to GraphQLFamily"""
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
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLFamily'
    ) -> Annotated['GraphQLProject', strawberry.lazy('api.graphql.types.project')]:
        """Load the project associated with this family."""
        from api.graphql.types.project import GraphQLProject

        loader = info.context['loaders'][LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)

    @strawberry.field
    async def participants(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLFamily'
    ) -> Annotated[
        list['GraphQLParticipant'], strawberry.lazy('api.graphql.types.participant')
    ]:
        """Load the participants associated with this family."""
        from api.graphql.types.participant import GraphQLParticipant

        participants = await info.context['loaders'][
            LoaderKeys.PARTICIPANTS_FOR_FAMILIES
        ].load(root.id)
        return [GraphQLParticipant.from_internal(p) for p in participants]

    @strawberry.field
    async def family_participants(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLFamily'
    ) -> list['GraphQLFamilyParticipant']:
        """Load the family participants associated with this family."""
        family_participants = await info.context['loaders'][
            LoaderKeys.FAMILY_PARTICIPANTS_FOR_FAMILIES
        ].load(root.id)
        return [
            GraphQLFamilyParticipant.from_internal(fp) for fp in family_participants
        ]

    @strawberry.field()
    async def discussion(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLFamily'
    ) -> Annotated['GraphQLDiscussion', strawberry.lazy('api.graphql.types.comments')]:
        """Load the discussion associated with this family."""
        from api.graphql.types.comments import GraphQLDiscussion

        loader = info.context['loaders'][LoaderKeys.COMMENTS_FOR_FAMILY_IDS]
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

    @staticmethod
    def from_internal(internal: PedRowInternal) -> 'GraphQLFamilyParticipant':
        """Convert a PedRowInternal to GraphQLFamilyParticipant."""
        return GraphQLFamilyParticipant(
            affected=internal.affected,
            notes=internal.notes,
            participant_id=internal.individual_id,
            family_id=internal.family_id,
        )

    @strawberry.field
    async def participant(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLFamilyParticipant'
    ) -> 'GraphQLParticipant':
        """Load the participant associated with this family participant."""
        from api.graphql.types.participant import GraphQLParticipant

        loader = info.context['loaders'][LoaderKeys.PARTICIPANTS_FOR_IDS]
        participant = await loader.load(root.participant_id)
        return GraphQLParticipant.from_internal(participant)

    @strawberry.field
    async def family(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLFamilyParticipant'
    ) -> GraphQLFamily:
        """Load the family associated with this family participant."""
        loader = info.context['loaders'][LoaderKeys.FAMILIES_FOR_IDS]
        family = await loader.load(root.family_id)
        return GraphQLFamily.from_internal(family)


from api.graphql.types.comments import GraphQLDiscussion
from api.graphql.types.participant import GraphQLParticipant
from api.graphql.types.project import GraphQLProject
