from fastapi import APIRouter

from api.utils.db import Connection, get_projectless_db_connection
from db.python.layers.seqr_projects_stats import SeqrProjectsStatsLayer
from models.models.seqr_projects_stats import SeqrProjectsDetails, SeqrProjectsSummary

router = APIRouter(prefix='/seqr-projects-stats', tags=['seqr-projects-stats'])


@router.post(
    '/seqr-projects-summary',
    operation_id='getSeqrProjectsStats',
    response_model=list[SeqrProjectsSummary],
)
async def get_seqr_projects_stats_summary(
    projects: list[int] = None,
    sequencing_types: list[str] = None,
    connection: Connection = get_projectless_db_connection,
):
    """
    Get the summary stats for a list of seqr projects
    """
    spslayer = SeqrProjectsStatsLayer(connection)
    projects_insights_stats = await spslayer.get_seqr_projects_stats_summary(
        projects=projects, sequencing_types=sequencing_types
    )

    return [s.to_external(links={}) for s in projects_insights_stats]


@router.post(
    '/seqr-projects-details',
    operation_id='getSeqrProjectsDetails',
    response_model=list[SeqrProjectsDetails],
)
async def get_seqr_projects_stats_details(
    projects: list[int] = None,
    sequencing_types: list[str] = None,
    connection: Connection = get_projectless_db_connection,
):
    """
    Get the detailed stats for a list of seqr projects
    """
    spslayer = SeqrProjectsStatsLayer(connection)
    projects_insights_details = await spslayer.get_seqr_projects_stats_details(
        projects=projects, sequencing_types=sequencing_types
    )

    return [s.to_external(links={}) for s in projects_insights_details]
