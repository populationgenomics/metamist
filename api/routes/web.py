"""
Web routes
"""
from typing import Optional, List
from enum import Enum
from urllib import response

from fastapi import APIRouter, Request
from pydantic import BaseModel

from models.models.sample import sample_id_format
from db.python.layers.web import WebLayer, NestedParticipant

from api.utils.db import (
    get_project_readonly_connection,
    get_projectless_db_connection,
    Connection,
)

from typing import Union



class PagingLinks(BaseModel):
    """Model for PAGING"""

    self: str
    next: Optional[str]
    token: Optional[str]


class ProjectSummaryResponse(BaseModel):
    """Response for the project summary"""
    # high level stats
    total_participants: int
    total_samples: int
    participants_in_seqr: int
    sequence_stats: dict[str, dict[str, str]]

    # for display
    participants: List[NestedParticipant]
    participant_keys: List[str]
    sample_keys: List[str]
    sequence_keys: List[str]

    links: Optional[PagingLinks]

    class Config:
        """Config for ProjectSummaryResponse"""

        fields = {'links': '_links'}

class SearchResponseType(Enum):
    FAMILY = 'family'
    PARTICIPANT = 'participant'
    SAMPLE = 'sample'

class SearchResponse(BaseModel):
    type: SearchResponseType
    title: str
    data: dict[str, Union[str, List[str]]]

class SearchResponseModel(BaseModel):
    responses: list[SearchResponse]

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
    print(token)

    summary = await st.get_project_summary(token=token, limit=limit)

    if len(summary.participants) == 0:
        return ProjectSummaryResponse(
            participants=[],
            participant_keys=[],
            sample_keys=[],
            sequence_keys=[],
            _links=None,
            total_samples=0,
            total_participants=0,
            participants_in_seqr=0,
            sequence_stats={},
        )

    participants = summary.participants

    collected_samples = sum(len(p.samples) for p in participants)
    new_token = None
    if collected_samples >= limit:
        new_token = max(int(sample.id) for p in participants for sample in p.samples)    

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
        total_samples=summary.total_samples,
        total_participants=summary.total_participants,
        participants_in_seqr=summary.participants_in_seqr,
        sequence_stats=summary.sequence_stats,

        # other stuff
        participants=participants,
        participant_keys=summary.participant_keys,
        sample_keys=summary.sample_keys,
        sequence_keys=summary.sequence_keys,
        _links=links,

    )


@router.get('/search', response_model=SearchResponseModel, operation_id='searchByKeyword')
async def search_by_keyword(keyword: str, connection = get_projectless_db_connection):
    """
    This searches the keyword, in families, participants + samples in the projects
    that you are a part of (automatically).
    """

    EXAMPLE_PROJECT = 'greek-myth'
    EXAMPLE_FAM_INT = '1'
    EXAMPLE_FAM_EXT = 'GRKMYTH'
    EXAMPLE_PARTICIPANT_INT = '310'
    EXAMPLE_PARTICIPANT_EXT = 'Phoebe'
    EXAMPLE_SAMPLE_INT = 'CPGLCL101'
    EXAMPLE_SAMPLE_EXT = 'GRK1017'

    print('here')

    return SearchResponseModel(responses=[
            SearchResponse(
            type=SearchResponseType.FAMILY,    
            title=EXAMPLE_FAM_EXT,
            data={
                'id': EXAMPLE_FAM_INT,
                'project': EXAMPLE_PROJECT,
                'family_exernal_ids': [EXAMPLE_FAM_EXT],
            }
        ),
        SearchResponse(
            type=SearchResponseType.PARTICIPANT,
            title=EXAMPLE_PARTICIPANT_EXT,
            data={
                'id': EXAMPLE_PARTICIPANT_INT,
                'project': EXAMPLE_PROJECT,
                'family_exernal_ids': [EXAMPLE_FAM_EXT],
                'participant_external_ids': [EXAMPLE_PARTICIPANT_EXT],
            }
        ),
        SearchResponse(
            type=SearchResponseType.SAMPLE,
            # shows the corrected search text, so here, it's pretending you've search for CPG sample ID
            title=EXAMPLE_SAMPLE_EXT, 
            data={
                'id': EXAMPLE_SAMPLE_INT,
                'project': EXAMPLE_PROJECT,
                'family_exernal_ids': [EXAMPLE_FAM_EXT],
                'participant_external_ids': [EXAMPLE_PARTICIPANT_EXT],
                'sample_external_ids': [EXAMPLE_SAMPLE_EXT],
            }
        ),
        SearchResponse(
            type=SearchResponseType.SAMPLE,
            # shows the corrected search text, so here, it's pretending you've search for CPG sample ID
            title='GRK1027', 
            data={
                'id': EXAMPLE_SAMPLE_INT,
                'project': EXAMPLE_PROJECT,
                'family_exernal_ids': [EXAMPLE_FAM_EXT],
                'participant_external_ids': [EXAMPLE_PARTICIPANT_EXT],
                'sample_external_ids': [EXAMPLE_SAMPLE_EXT],
            }
        )
    ]

        )
