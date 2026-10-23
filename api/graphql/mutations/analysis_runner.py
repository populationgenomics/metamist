from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Annotated

import strawberry
from strawberry.scalars import JSON
from strawberry.types import Info

from api.graphql.loaders import GraphQLContext
from db.python.filters import GenericFilter
from db.python.layers.analysis_runner import AnalysisRunnerLayer
from db.python.tables.analysis_runner import AnalysisRunnerFilter
from models.models.analysis_runner import (
    AnalysisRunnerInternal,
    AnalysisRunnerUpdateBody,
)
from models.models.project import FullWriteAccessRoles


if TYPE_CHECKING:
    from api.graphql.schema import GraphQLAnalysisRunner


@strawberry.input
class AnalysisRunnerInput:
    """Analysis runner input"""

    ar_guid: str
    access_level: str
    repository: str
    commit: str
    script: str
    description: str
    driver_image: str
    environment: str
    batch_url: str
    submitting_user: str
    meta: JSON
    output_path: str
    hail_version: str | None = None
    cwd: str | None = None
    config_path: str | None = None


@strawberry.input
class AnalysisRunnerUpdateInput:
    """Input for updating analysis runner logs, only the given fields are updated"""

    config_path: str | None = None
    batch_url: str | None = None
    # merged into the existing meta, a null value removes the key
    meta: JSON | None = None


@strawberry.type
class AnalysisRunnerMutations:
    """Analysis Runner mutations"""

    @strawberry.mutation
    async def create_analysis_runner_log(
        self,
        project: str,
        analysis_runner: AnalysisRunnerInput,
        info: Info[GraphQLContext, AnalysisRunnerMutations],
    ) -> str:
        """Create a new analysis runner log"""
        async with info.context.get_connection() as connection:
            # Should be moved to the analysis runner layer
            (target_project,) = connection.get_and_check_access_to_projects_for_names(
                [project], FullWriteAccessRoles
            )
            alayer = AnalysisRunnerLayer(connection)

            analysis_id = await alayer.insert_analysis_runner_entry(
                AnalysisRunnerInternal(
                    ar_guid=analysis_runner.ar_guid,
                    timestamp=datetime.datetime.now(),
                    access_level=analysis_runner.access_level,
                    repository=analysis_runner.repository,
                    commit=analysis_runner.commit,
                    script=analysis_runner.script,
                    description=analysis_runner.description,
                    driver_image=analysis_runner.driver_image,
                    config_path=analysis_runner.config_path,
                    cwd=analysis_runner.cwd,
                    environment=analysis_runner.environment,
                    hail_version=analysis_runner.hail_version,
                    batch_url=analysis_runner.batch_url,
                    submitting_user=analysis_runner.submitting_user,
                    meta=analysis_runner.meta,  # type: ignore [arg-type]
                    project=target_project.id,
                    audit_log_id=None,
                    output_path=analysis_runner.output_path,
                )
            )

            return analysis_id

    @strawberry.mutation
    async def update_analysis_runner_log(
        self,
        ar_guid: str,
        ar_update_input: AnalysisRunnerUpdateInput,
        info: Info[GraphQLContext, AnalysisRunnerMutations],
    ) -> Annotated[GraphQLAnalysisRunner, strawberry.lazy('api.graphql.schema')]:
        """Update an analysis runner log"""
        from api.graphql.schema import GraphQLAnalysisRunner  # noqa: PLC0415

        async with info.context.get_connection() as connection:
            # write access to the log's project is checked in the layer
            alayer = AnalysisRunnerLayer(connection)
            await alayer.update_analysis_runner_entry(
                ar_update_body=AnalysisRunnerUpdateBody(
                    ar_guid=ar_guid,
                    config_path=ar_update_input.config_path,
                    batch_url=ar_update_input.batch_url,
                    meta=ar_update_input.meta,  # type: ignore [arg-type]
                ),
            )
            (updated_entry,) = await alayer.query(
                AnalysisRunnerFilter(ar_guid=GenericFilter(eq=ar_guid))
            )
            return GraphQLAnalysisRunner.from_internal(updated_entry)
