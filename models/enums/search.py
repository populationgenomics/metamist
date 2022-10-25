from enum import Enum


class SearchResponseType(str, Enum):
    """Define types of search results"""

    FAMILY = 'family'
    PARTICIPANT = 'participant'
    SAMPLE = 'sample'
