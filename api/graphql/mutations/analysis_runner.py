import datetime

import strawberry
from strawberry.types import Info

from db.python.connect import Connection
from db.python.layers.analysis_runner import AnalysisRunnerLayer
from models.models.analysis_runner import AnalysisRunnerInternal
from models.models.project import FullWriteAccessRoles


@strawberry.type
class AnalysisRunnerMutations:
    """Analysis Runner mutations"""

    @strawberry.mutation
    async def create_analysis_runner_log(  # pylint: disable=too-many-arguments
        self,
        ar_guid: str,
        access_level: str,
        repository: str,
        commit: str,
        script: str,
        description: str,
        driver_image: str,
        config_path: str,
        environment: str,
        batch_url: str,
        submitting_user: str,
        meta: dict[str, str],
        output_path: str,
        hail_version: str | None,
        cwd: str | None,
        info: Info,
    ) -> str:
        """Create a new analysis runner log"""
        connection: Connection = info.context['connection']
        connection.check_access(FullWriteAccessRoles)
        alayer = AnalysisRunnerLayer(connection)

        if not connection.project:
            raise ValueError('Project not set')

        analysis_id = await alayer.insert_analysis_runner_entry(
            AnalysisRunnerInternal(
                ar_guid=ar_guid,
                timestamp=datetime.datetime.now(),
                access_level=access_level,
                repository=repository,
                commit=commit,
                script=script,
                description=description,
                driver_image=driver_image,
                config_path=config_path,
                cwd=cwd,
                environment=environment,
                hail_version=hail_version,
                batch_url=batch_url,
                submitting_user=submitting_user,
                meta=meta,
                project=connection.project_id,  # type: ignore [arg-type]
                audit_log_id=None,
                output_path=output_path,
            )
        )

        return analysis_id
