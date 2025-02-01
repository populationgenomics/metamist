# pylint: disable=redefined-builtin, import-outside-toplevel

from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.types import Info

from api.graphql.context import GraphQLContext
from db.python.connect import Connection
from db.python.layers.analysis import AnalysisLayer
from models.enums.analysis import AnalysisStatus
from models.models.analysis import Analysis
from models.models.project import FullWriteAccessRoles

if TYPE_CHECKING:
    from api.graphql.query.analysis import GraphQLAnalysis

AnalysisStatusType = strawberry.enum(AnalysisStatus)  # type: ignore [misc]


@strawberry.input
class AnalysisInput:
    """Analysis input"""

    type: str
    status: AnalysisStatusType  # type: ignore [assignment]
    output: str | None = None
    outputs: strawberry.scalars.JSON | None = None
    sequencing_group_ids: list[str] | None = None
    cohort_ids: list[str] | None = None
    author: str | None = None
    timestamp_completed: str | None = None
    active: bool | None = None
    meta: strawberry.scalars.JSON


@strawberry.input
class AnalysisUpdateInput:
    """Analysis update input"""

    status: AnalysisStatusType  # type: ignore [assignment]
    output: str | None = None
    outputs: strawberry.scalars.JSON | None = None
    meta: strawberry.scalars.JSON | None = None
    active: bool | None = None


@strawberry.type
class AnalysisMutations:
    """Analysis mutations"""

    @strawberry.mutation
    async def create_analysis(
        self,
        project: str,
        analysis: AnalysisInput,
        info: Info[GraphQLContext, 'AnalysisMutations'],
    ) -> Annotated['GraphQLAnalysis', strawberry.lazy('api.graphql.query.analysis')]:
        """Create a new analysis"""
        from api.graphql.query.analysis import GraphQLAnalysis

        connection: Connection = info.context.connection

        # Should be moved to the analysis layer
        (target_project,) = connection.get_and_check_access_to_projects_for_names(
            [project], FullWriteAccessRoles
        )
        alayer = AnalysisLayer(connection)

        if analysis.author:
            # special tracking here, if we can't catch it through the header
            connection.on_behalf_of = analysis.author

        if not analysis.sequencing_group_ids and not analysis.cohort_ids:
            raise ValueError('Must specify "sequencing_group_ids" or "cohort_ids"')

        analysis_id = await alayer.create_analysis(
            Analysis(**analysis.__dict__).to_internal(),
            project=target_project.id,
        )
        created_analysis = await alayer.get_analysis_by_id(analysis_id)
        return GraphQLAnalysis.from_internal(created_analysis)

    @strawberry.mutation
    async def update_analysis(
        self,
        analysis_id: int,
        analysis: AnalysisUpdateInput,
        info: Info[GraphQLContext, 'AnalysisMutations'],
    ) -> Annotated['GraphQLAnalysis', strawberry.lazy('api.graphql.query.analysis')]:
        """Update status of analysis"""
        from api.graphql.query.analysis import GraphQLAnalysis

        connection = info.context.connection
        alayer = AnalysisLayer(connection)
        await alayer.update_analysis(
            analysis_id,
            status=analysis.status,
            output=analysis.output,
            outputs=analysis.outputs,  # type: ignore [arg-type]
            meta=analysis.meta,  # type: ignore [arg-type]
            active=analysis.active,
        )
        updated_analysis = await alayer.get_analysis_by_id(analysis_id)
        return GraphQLAnalysis.from_internal(updated_analysis)
