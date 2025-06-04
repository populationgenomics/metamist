import datetime
from typing import TYPE_CHECKING

import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext, LoaderKeys
from api.graphql.types.audit_log import GraphQLAuditLog
from api.graphql.types.project import GraphQLProject
from api.graphql.types.sequencing_group import GraphQLSequencingGroup
from models.enums import AnalysisStatus
from models.models import (
    AnalysisInternal,
)

if TYPE_CHECKING:
    from api.graphql.schema import Query


@strawberry.type
class GraphQLAnalysis:
    """
    GraphQL type representing an analysis entity.

    Attributes:
        id: Unique identifier for the analysis.
        type: The type/category of the analysis.
        status: The current status of the analysis (see AnalysisStatus enum).
        output: Output string or summary, if available.
        outputs: JSON object containing multiple outputs, if any.
        timestamp_completed: Datetime when the analysis was completed.
        active: Whether the analysis is currently active.
        meta: Additional metadata as a JSON object.
        project_id: Internal project ID (private field).
    """

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
        """
        Create a GraphQLAnalysis instance from an internal AnalysisInternal model.

        Args:
            internal: The internal AnalysisInternal model instance.
        Returns:
            GraphQLAnalysis: The corresponding GraphQL type instance.
        Raises:
            ValueError: If the internal model does not have an id.
        """
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
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLAnalysis'
    ) -> list[GraphQLSequencingGroup]:
        """
        Retrieve sequencing groups associated with this analysis.

        Args:
            info: GraphQL resolver context.
            root: The root GraphQLAnalysis object.
        Returns:
            List of GraphQLSequencingGroup objects linked to this analysis.
        """
        loader = info.context['loaders'][LoaderKeys.SEQUENCING_GROUPS_FOR_ANALYSIS]
        sgs = await loader.load(root.id)
        return [GraphQLSequencingGroup.from_internal(sg) for sg in sgs]

    @strawberry.field
    async def project(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLAnalysis'
    ) -> GraphQLProject:
        """
        Retrieve the project associated with this analysis.

        Args:
            info: GraphQL resolver context.
            root: The root GraphQLAnalysis object.
        Returns:
            The GraphQLProject instance for this analysis.
        """
        loader = info.context['loaders'][LoaderKeys.PROJECTS_FOR_IDS]
        project = await loader.load(root.project_id)
        return GraphQLProject.from_internal(project)

    @strawberry.field
    async def audit_logs(
        self, info: Info[GraphQLContext, 'Query'], root: 'GraphQLAnalysis'
    ) -> list[GraphQLAuditLog]:
        """
        Retrieve audit logs related to this analysis.

        Args:
            info: GraphQL resolver context.
            root: The root GraphQLAnalysis object.
        Returns:
            List of GraphQLAuditLog objects for this analysis.
        """
        loader = info.context['loaders'][LoaderKeys.AUDIT_LOGS_BY_ANALYSIS_IDS]
        audit_logs = await loader.load(root.id)
        return [GraphQLAuditLog.from_internal(audit_log) for audit_log in audit_logs]
