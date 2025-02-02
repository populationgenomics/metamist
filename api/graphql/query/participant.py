from typing import TYPE_CHECKING, Annotated, Self

import strawberry

from api.graphql.context import GraphQLContext
from api.graphql.filters import (
    GraphQLFilter,
    GraphQLMetaFilter,
    graphql_meta_filter_to_internal_filter,
)
from api.graphql.query.audit_log_loaders import AuditLogLoaderKeys
from api.graphql.query.comment_loaders import CommentLoaderKeys
from api.graphql.query.family_loaders import FamilyLoaderKeys
from api.graphql.query.participant_loaders import ParticipantLoaderKeys
from api.graphql.query.project_loaders import ProjectLoaderKeys
from api.graphql.query.sample_loaders import SampleLoaderKeys
from db.python.filters.generic import GenericFilter
from db.python.filters.sample import SampleFilter
from models.base import PRIMARY_EXTERNAL_ORG
from models.models.participant import ParticipantInternal

from .audit_log import GraphQLAuditLog

if TYPE_CHECKING:
    from .comment import GraphQLDiscussion
    from .family import GraphQLFamily, GraphQLFamilyParticipant
    from .project import GraphQLProject
    from .sample import GraphQLSample


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
        info: strawberry.Info[GraphQLContext, None],
        root: Self,
        type: GraphQLFilter[str] | None = None,
        meta: GraphQLMetaFilter | None = None,
        active: GraphQLFilter[bool] | None = None,
    ) -> list[Annotated['GraphQLSample', strawberry.lazy('.sample')]]:
        from .sample import GraphQLSample

        filter_ = SampleFilter(
            type=type.to_internal_filter() if type else None,
            meta=graphql_meta_filter_to_internal_filter(meta),
            active=active.to_internal_filter() if active else GenericFilter(eq=True),
        )
        q = {'id': root.id, 'filter': filter_}

        samples = await info.context.loaders[
            SampleLoaderKeys.SAMPLES_FOR_PARTICIPANTS
        ].load(q)
        return [GraphQLSample.from_internal(s) for s in samples]

    @strawberry.field
    async def phenotypes(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> strawberry.scalars.JSON:
        loader = info.context.loaders[ParticipantLoaderKeys.PHENOTYPES_FOR_PARTICIPANTS]
        return await loader.load(root.id)

    @strawberry.field
    async def families(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> list[Annotated['GraphQLFamily', strawberry.lazy('.family')]]:
        from .family import GraphQLFamily

        fams = await info.context.loaders[
            FamilyLoaderKeys.FAMILIES_FOR_PARTICIPANTS
        ].load(root.id)
        return [GraphQLFamily.from_internal(f) for f in fams]

    @strawberry.field
    async def family_participants(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> list[Annotated['GraphQLFamilyParticipant', strawberry.lazy('.family')]]:
        from .family import GraphQLFamilyParticipant

        family_participants = await info.context.loaders[
            FamilyLoaderKeys.FAMILY_PARTICIPANTS_FOR_PARTICIPANTS
        ].load(root.id)
        return [
            GraphQLFamilyParticipant.from_internal(fp) for fp in family_participants
        ]

    @strawberry.field
    async def project(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLProject', strawberry.lazy('.project')]:
        from .project import GraphQLProject

        loader = info.context.loaders[ProjectLoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)

    @strawberry.field
    async def audit_log(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> GraphQLAuditLog | None:
        if root.audit_log_id is None:
            return None
        loader = info.context.loaders[AuditLogLoaderKeys.AUDIT_LOGS_BY_IDS]
        audit_log = await loader.load(root.audit_log_id)
        return GraphQLAuditLog.from_internal(audit_log)

    @strawberry.field()
    async def discussion(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLDiscussion', strawberry.lazy('.comment')]:
        from .comment import GraphQLDiscussion

        loader = info.context.loaders[CommentLoaderKeys.COMMENTS_FOR_PARTICIPANT_IDS]
        discussion = await loader.load(root.id)
        return GraphQLDiscussion.from_internal(discussion)
