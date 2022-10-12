from db.python.utils import ProjectId
from models.base import SMBase
from models.enums.search import SearchResponseType


class ResponseData(SMBase):
    project: ProjectId | str


class FamilyResponseData(ResponseData):
    id: int


class ParticipantResponseData(ResponseData):
    id: int
    family_external_ids: list[str]
    participant_external_ids: list[str]


class SampleResponseData(ResponseData):
    id: str | None
    family_external_ids: list[str]
    participant_external_ids: list[str]
    sample_external_ids: list[str]


class SearchResponse(SMBase):
    type: SearchResponseType
    title: str
    data: SampleResponseData | ParticipantResponseData | FamilyResponseData
