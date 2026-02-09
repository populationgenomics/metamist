from enum import StrEnum


class SearchResponseType(StrEnum):
    """Define types of search results"""

    FAMILY = 'family'
    PARTICIPANT = 'participant'
    SAMPLE = 'sample'
    SEQGROUP = 'sequencing-group'
    ERROR = 'error'
