import datetime
from typing import TYPE_CHECKING, Annotated, Self

import strawberry

from api.graphql.context import GraphQLContext
from api.graphql.query.audit_log_loaders import AuditLogLoaderKeys
from api.graphql.query.project_loaders import ProjectLoaderKeys
from api.graphql.query.sequencing_group_loaders import SequencingGroupLoaderKeys
from models.enums.analysis import AnalysisStatus
from models.models.analysis import AnalysisInternal

if TYPE_CHECKING:
    from .audit_log import GraphQLAuditLog
    from .project import GraphQLProject
    from .sequencing_group import GraphQLSequencingGroup

GraphQLAnalysisStatus = strawberry.enum(AnalysisStatus)  # type: ignore


@strawberry.type
class GraphQLAnalysis:
    """Analysis GraphQL model"""

    id: int
    type: str
    status: strawberry.enum(AnalysisStatus)  # type: ignore
    output: str | None
    outputs: strawberry.scalars.JSON | None
    timestamp_completed: datetime.datetime | None = None
    active: bool
    meta: strawberry.scalars.JSON | None

    project_id: strawberry.Private[int]

    @staticmethod
    def from_internal(internal: AnalysisInternal) -> 'GraphQLAnalysis':
        if not internal.id:
            raise ValueError('Analysis must have an id')
        return GraphQLAnalysis(
            id=internal.id,
            type=internal.type,
            status=internal.status,
            output=internal.output,
            outputs=internal.outputs,
            timestamp_completed=internal.timestamp_completed,
            active=internal.active,
            meta=internal.meta,
            project_id=internal.project,
        )

    @strawberry.field
    async def sequencing_groups(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> list[
        Annotated['GraphQLSequencingGroup', strawberry.lazy('.sequencing_group')]
    ]:
        loader = info.context.loaders[
            SequencingGroupLoaderKeys.SEQUENCING_GROUPS_FOR_ANALYSIS
        ]
        sgs = await loader.load(root.id)
        return [GraphQLSequencingGroup.from_internal(sg) for sg in sgs]

    @strawberry.field
    async def project(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> Annotated['GraphQLProject', strawberry.lazy('.project')]:
        from .project import GraphQLProject

        loader = info.context.loaders[ProjectLoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)

    @strawberry.field
    async def audit_logs(
        self, info: strawberry.Info[GraphQLContext, None], root: Self
    ) -> list[Annotated['GraphQLAuditLog', strawberry.lazy('.audit_log')]]:
        loader = info.context.loaders[AuditLogLoaderKeys.AUDIT_LOGS_BY_ANALYSIS_IDS]
        audit_logs = await loader.load(root.id)
        return [GraphQLAuditLog.from_internal(audit_log) for audit_log in audit_logs]
