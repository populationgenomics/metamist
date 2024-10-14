from typing import TYPE_CHECKING
import strawberry
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from api.graphql.types import AnalysisStatusType
from db.python.connect import Connection
from db.python.layers.analysis import AnalysisLayer
from models.models import AnalysisInternal
from models.models.project import FullWriteAccessRoles

if TYPE_CHECKING:
    from api.graphql.mutations.project import ProjectMutations


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
    project: int
    active: bool | None = None
    meta: strawberry.scalars.JSON


@strawberry.input
class AnalysisUpdateInput:
    """Analysis update input"""

    status: AnalysisStatusType  # type: ignore [assignment]
    output: str | None
    outputs: strawberry.scalars.JSON | None
    meta: strawberry.scalars.JSON | None
    active: bool | None


@strawberry.type
class AnalysisMutations:
    """Analysis mutations"""

    project_id: strawberry.Private[int]

    @strawberry.mutation
    async def create_analysis(
        self,
        analysis: AnalysisInput,
        info: Info[GraphQLContext, 'AnalysisMutations'],
        root: 'ProjectMutations',
    ) -> int:
        """Create a new analysis"""
        connection: Connection = info.context['connection']

        # Should be moved to the analysis layer
        connection.check_access_to_projects_for_ids(
            project_ids=[analysis.project], allowed_roles=FullWriteAccessRoles
        )
        atable = AnalysisLayer(connection)

        if analysis.author:
            # special tracking here, if we can't catch it through the header
            connection.on_behalf_of = analysis.author

        if not analysis.sequencing_group_ids and not analysis.cohort_ids:
            raise ValueError('Must specify "sequencing_group_ids" or "cohort_ids"')

        analysis_id = await atable.create_analysis(
            AnalysisInternal.from_db(**strawberry.asdict(analysis)),
            project=root.project_id,
        )

        return analysis_id

    @strawberry.mutation
    async def update_analysis(
        self,
        analysis_id: int,
        analysis: AnalysisUpdateInput,
        info: Info[GraphQLContext, 'AnalysisMutations'],
    ) -> bool:
        """Update status of analysis"""
        connection = info.context['connection']
        atable = AnalysisLayer(connection)
        await atable.update_analysis(
            analysis_id,
            status=analysis.status,
            output=analysis.output,
            outputs=analysis.outputs,  # type: ignore [arg-type]
            meta=analysis.meta,  # type: ignore [arg-type]
            active=analysis.active,
        )
        return True
