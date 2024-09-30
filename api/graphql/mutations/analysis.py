import strawberry
from strawberry.types import Info

from api.graphql.types import AnalysisStatusType
from db.python.connect import Connection
from db.python.layers.analysis import AnalysisLayer
from models.models import AnalysisInternal
from models.models.project import FullWriteAccessRoles


@strawberry.input
class AnalysisInput:
    """Analysis input"""

    type: str
    status: AnalysisStatusType  # type: ignore [assignment]
    id: int | None
    output: str | None
    sequencing_group_ids: list[str] | None
    cohort_ids: list[str] | None
    author: str | None
    timestamp_completed: str | None
    project: int
    active: bool | None
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

    @strawberry.mutation
    async def create_analysis(
        self,
        analysis: AnalysisInput,
        info: Info,
    ) -> int:
        """Create a new analysis"""
        connection: Connection = info.context['connection']
        connection.get_and_check_access_to_projects_for_ids(
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
        )

        return analysis_id

    @strawberry.mutation
    async def update_analysis(
        self,
        analysis_id: int,
        analysis: AnalysisUpdateInput,
        info: Info,
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
