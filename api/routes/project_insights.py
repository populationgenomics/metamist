from fastapi import APIRouter

from api.utils.db import Connection, get_projectless_db_connection
from db.python.layers.project_insights import ProjectInsightsLayer
from models.models.project_insights import (
    ProjectInsightsDetails,
    ProjectInsightsSummary,
)

router = APIRouter(prefix='/project-insights', tags=['project-insights',])


@router.post(
    '/project-insights-summary',
    operation_id='getProjectInsightsSummary',
    response_model=list[ProjectInsightsSummary],
)
async def get_project_insights_summary(
    project_ids: list[int] = None,
    sequencing_types: list[str] = None,
    connection: Connection = get_projectless_db_connection,
):
    """
    Get summary stats for a list of projects and sequencing types
    """
    pilayer = ProjectInsightsLayer(connection)
    projects_insights_stats = await pilayer.get_project_insights_summary(
        project_ids=project_ids, sequencing_types=sequencing_types
    )

    return [s.to_external() for s in projects_insights_stats]


@router.post(
    '/project-insights-details',
    operation_id='getProjectInsightsDetails',
    response_model=list[ProjectInsightsDetails],
)
async def get_project_insights_details(
    project_ids: list[int] = None,
    sequencing_types: list[str] = None,
    connection: Connection = get_projectless_db_connection,
):
    """
    Get extensive sequencing group details for a list of projects and sequencing types
    """
    pilayer = ProjectInsightsLayer(connection)
    projects_insights_details = await pilayer.get_project_insights_details(
        project_ids=project_ids, sequencing_types=sequencing_types
    )

    return [s.to_external() for s in projects_insights_details]
