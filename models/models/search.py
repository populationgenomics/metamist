from models.base import SMBase
from models.enums.search import SearchResponseType
from models.enums.web import MetaSearchEntityPrefix
from models.models.project import ProjectId


class SearchResponseData(SMBase):
    """Response data class for Search"""

    project: ProjectId | str


class FamilySearchResponseData(SearchResponseData):
    """Family search response data"""

    id: int
    family_external_ids: list[str]


class ParticipantSearchResponseData(SearchResponseData):
    """Participant search response data"""

    id: int
    family_external_ids: list[str]
    participant_external_ids: list[str]


class SampleSearchResponseData(SearchResponseData):
    """Sample search response data"""

    id: str | None
    family_external_ids: list[str]
    participant_external_ids: list[str]
    sample_external_ids: list[str]


class SequencingGroupSearchResponseData(SearchResponseData):
    """Sequencing group search response data"""

    id: str | None
    sample_external_id: str
    sg_external_id: str


class ErrorResponse(SMBase):
    """Error search response data"""

    error: str


class SearchResponse(SMBase):
    """Response class for each search result"""

    type: SearchResponseType
    title: str
    data: SampleSearchResponseData | ParticipantSearchResponseData | FamilySearchResponseData | SequencingGroupSearchResponseData
    error: ErrorResponse | None = None


class SearchItem(SMBase):
    """Summary Grid Filter Model"""

    model_type: MetaSearchEntityPrefix
    query: str
    field: str
    is_meta: bool
