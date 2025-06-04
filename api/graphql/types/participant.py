from typing import TYPE_CHECKING

import strawberry
from strawberry.types import Info

from api.graphql.filters import (
    GraphQLFilter,
    GraphQLMetaFilter,
    graphql_meta_filter_to_internal_filter,
)
from api.graphql.loaders import GraphQLContext, LoaderKeys
from api.graphql.types.audit_log import GraphQLAuditLog
from api.graphql.types.comments import GraphQLDiscussion
from api.graphql.types.family import GraphQLFamily, GraphQLFamilyParticipant
from api.graphql.types.project import GraphQLProject
from api.graphql.types.sample import GraphQLSample
from db.python.filters import GenericFilter
from db.python.tables.sample import SampleFilter
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    ParticipantInternal,
)

if TYPE_CHECKING:
    from api.graphql.schema import Query


@strawberry.type
class GraphQLParticipant:
    """Participant GraphQL model"""

    id: int
    external_id: str
    external_ids: strawberry.scalars.JSON
    meta: strawberry.scalars.JSON

    reported_sex: int | None
    reported_gender: str | None
    karyotype: str | None

    project_id: strawberry.Private[int]
    audit_log_id: strawberry.Private[int | None]

    @staticmethod
    def from_internal(internal: ParticipantInternal) -> 'GraphQLParticipant':
        """Convert a ParticipantInternal to GraphQLParticipant"""
        return GraphQLParticipant(
            id=internal.id,
            external_id=internal.external_ids[PRIMARY_EXTERNAL_ORG],
            external_ids=internal.external_ids or {},
            meta=internal.meta,
            reported_sex=internal.reported_sex,
            reported_gender=internal.reported_gender,
            karyotype=internal.karyotype,
            project_id=internal.project,
            audit_log_id=internal.audit_log_id,
        )

    @strawberry.field
    async def samples(
        self,
        info: Info[GraphQLContext, 'Query'],
        root: 'GraphQLParticipant',
        type_: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
        active: GraphQLFilter[bool] | None = None,
    ) -> list[GraphQLSample]:
        """List samples for a participant with optional filters."""
        filter_ = SampleFilter(
            type=type_.to_internal_filter() if type_ else None,
            meta=graphql_meta_filter_to_internal_filter(meta),
            active=active.to_internal_filter() if active else GenericFilter(eq=True),
        )
        q = {'id': root.id, 'filter': filter_}

        samples = await info.context['loaders'][
            LoaderKeys.SAMPLES_FOR_PARTICIPANTS
        ].load(q)
        return [GraphQLSample.from_internal(s) for s in samples]

    @strawberry.field
    async def phenotypes(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLParticipant'
    ) -> strawberry.scalars.JSON:
        """Get phenotypes for a participant."""
        loader = info.context['loaders'][LoaderKeys.PHENOTYPES_FOR_PARTICIPANTS]
        return await loader.load(root.id)

    @strawberry.field
    async def families(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLParticipant'
    ) -> list[GraphQLFamily]:
        """Get families for a participant."""
        fams = await info.context['loaders'][LoaderKeys.FAMILIES_FOR_PARTICIPANTS].load(
            root.id
        )
        return [GraphQLFamily.from_internal(f) for f in fams]

    @strawberry.field
    async def family_participants(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLParticipant'
    ) -> list[GraphQLFamilyParticipant]:
        """Get family participants for a participant."""
        family_participants = await info.context['loaders'][
            LoaderKeys.FAMILY_PARTICIPANTS_FOR_PARTICIPANTS
        ].load(root.id)
        return [
            GraphQLFamilyParticipant.from_internal(fp) for fp in family_participants
        ]

    @strawberry.field
    async def project(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLParticipant'
    ) -> GraphQLProject:
        """Get the project associated with this participant."""
        loader = info.context['loaders'][LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)

    @strawberry.field
    async def audit_log(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLParticipant'
    ) -> GraphQLAuditLog | None:
        """Get the audit log associated with this participant."""
        if root.audit_log_id is None:
            return None
        loader = info.context['loaders'][LoaderKeys.AUDIT_LOGS_BY_IDS]
        audit_log = await loader.load(root.audit_log_id)
        return GraphQLAuditLog.from_internal(audit_log)

    @strawberry.field()
    async def discussion(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLParticipant'
    ) -> GraphQLDiscussion:
        """Get the discussion associated with this participant."""
        loader = info.context['loaders'][LoaderKeys.COMMENTS_FOR_PARTICIPANT_IDS]
        discussion = await loader.load(root.id)
        return GraphQLDiscussion.from_internal(discussion)
