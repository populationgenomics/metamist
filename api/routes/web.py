"""
Web routes
"""
from typing import Optional, List

from fastapi import APIRouter, Request
from pydantic import BaseModel

from models.models.sample import sample_id_format
from db.python.layers.web import WebLayer, NestedParticipant

from api.utils.db import (
    get_project_readonly_connection,
    Connection,
)


class PagingLinks(BaseModel):
    """Model for PAGING"""

    self: str
    next: Optional[str]
    token: Optional[str]


class ProjectSummaryResponse(BaseModel):
    """Response for the project summary"""

    participants: List[NestedParticipant]
    total_samples: int
    sample_keys: List[str]
    sequence_keys: List[str]

    links: Optional[PagingLinks]

    class Config:
        """Config for ProjectSummaryResponse"""

        fields = {'links': '_links'}


router = APIRouter(prefix='/web', tags=['web'])


@router.get(
    '/{project}/summary',
    response_model=ProjectSummaryResponse,
    operation_id='getProjectSummary',
)
async def get_project_summary(
    request: Request,
    limit: int = 20,
    token: Optional[str] = None,
    connection: Connection = get_project_readonly_connection,
) -> ProjectSummaryResponse:
    """Creates a new sample, and returns the internal sample ID"""
    st = WebLayer(connection)

    summary = await st.get_project_summary(token=token, limit=limit)

    if len(summary.participants) == 0:
        return ProjectSummaryResponse(
            participants=[],
            sample_keys=[],
            sequence_keys=[],
            _links=None,
            total_samples=0,
        )

    participants = summary.participants

    collected_samples = sum(len(p.samples) for p in participants)
    new_token = None
    if collected_samples >= limit:
        new_token = max(sample.id for p in participants for sample in p.samples)

    for participant in participants:
        for sample in participant.samples:
            sample.id = sample_id_format(sample.id)

    links = PagingLinks(
        next=str(request.base_url) + request.url.path + f'?token={new_token}'
        if new_token
        else None,
        self=str(request.url),
        token=str(new_token) if new_token else None,
    )

    return ProjectSummaryResponse(
        participants=participants,
        total_samples=summary.total_samples,
        sample_keys=summary.sample_keys,
        sequence_keys=summary.sequence_keys,
        _links=links,
    )
