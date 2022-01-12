"""
"""
from typing import Optional, List, Dict, Union

from fastapi import APIRouter, Body, Request
from pydantic import BaseModel

from models.enums import SampleType
from models.enums.sequencing import SequenceStatus, SequenceType
from models.models.sample import (
    sample_id_transform_to_raw,
    sample_id_format,
    sample_id_transform_to_raw_list,
)
from db.python.layers.web import WebLayer, NestedParticipant
from db.python.tables.project import ProjectPermissionsTable

from api.utils.db import (
    get_project_write_connection,
    get_project_readonly_connection,
    Connection,
    get_projectless_db_connection,
)


class PagingLinks(BaseModel):
    self: str
    next: Optional[str]
    token: Optional[str]


class ProjectSummaryResponse(BaseModel):
    participants: List[NestedParticipant]
    total_samples: int
    sample_keys: List[str]
    sequence_keys: List[str]

    links: Optional[PagingLinks]

    class Config:
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

    if token is not None:
        # shhh, it's actually a sample ID
        token = int(token)
    summary = await st.get_project_summary(token=token, limit=limit)

    if len(summary.participants) == 0:
        return ProjectSummaryResponse(
            participants=[], sample_keys=[], sequence_keys=[], _links=None, total_samples=0
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
