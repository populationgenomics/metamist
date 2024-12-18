import datetime
from typing import TYPE_CHECKING

import strawberry
from strawberry.types import Info
from strawberry.scalars import JSON

from db.python.connect import Connection
from db.python.layers.analysis_runner import AnalysisRunnerLayer
from models.models.analysis_runner import AnalysisRunnerInternal
from models.models.project import FullWriteAccessRoles

if TYPE_CHECKING:
    from api.graphql.mutations.project import ProjectMutations


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
    config_path: str
    environment: str
    batch_url: str
    submitting_user: str
    meta: JSON
    output_path: str
    hail_version: str | None = None
    cwd: str | None = None


@strawberry.type
class AnalysisRunnerMutations:
    """Analysis Runner mutations"""

    project_id: strawberry.Private[int]

    @strawberry.mutation
    async def create_analysis_runner_log(
        self,
        analysis_runner: AnalysisRunnerInput,
        info: Info,
        root: 'ProjectMutations',
    ) -> str:
        """Create a new analysis runner log"""
        connection: Connection = info.context['connection']

        # Should be moved to the analysis runner layer
        connection.check_access_to_projects_for_ids(
            [root.project_id], FullWriteAccessRoles
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
                project=root.project_id,
                audit_log_id=None,
                output_path=analysis_runner.output_path,
            )
        )

        return analysis_id
