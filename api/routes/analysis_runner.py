import datetime

from fastapi import APIRouter

from api.utils.db import (
    Connection,
    get_project_readonly_connection,
    get_project_write_connection,
)
from db.python.layers.analysis_runner import AnalysisRunnerLayer
from db.python.tables.analysis_runner import AnalysisRunnerFilter
from db.python.utils import GenericFilter
from models.models.analysis_runner import AnalysisRunner, AnalysisRunnerInternal

router = APIRouter(prefix='/analysis-runner', tags=['analysis-runner'])


@router.put('/{project}/', operation_id='createAnalysisRunnerLog')
async def create_analysis_runner_log(  # pylint: disable=too-many-arguments
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
    hail_version: str | None = None,
    cwd: str | None = None,
    connection: Connection = get_project_write_connection,
) -> str:
    """Create a new analysis runner log"""

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
            project=connection.project,
            audit_log_id=None,
            output_path=output_path,
        )
    )

    return analysis_id


@router.get('/{project}/', operation_id='getAnalysisRunnerLogs')
async def get_analysis_runner_logs(
    project: str,
    ar_guid: str | None = None,
    submitting_user: str | None = None,
    repository: str | None = None,
    access_level: str | None = None,
    environment: str | None = None,
    connection: Connection = get_project_readonly_connection,
) -> list[AnalysisRunner]:
    """Get analysis runner logs"""

    atable = AnalysisRunnerLayer(connection)

    if not connection.project:
        raise ValueError('Project not set')

    filter_ = AnalysisRunnerFilter(
        ar_guid=GenericFilter(eq=ar_guid),
        submitting_user=GenericFilter(eq=submitting_user),
        repository=GenericFilter(eq=repository),
        access_level=GenericFilter(eq=access_level),
        environment=GenericFilter(eq=environment),
        project=GenericFilter(eq=connection.project),
    )

    logs = await atable.query(filter_)

    return [log.to_external({connection.project: project}) for log in logs]
