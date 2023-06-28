from enum import Enum


class MetaSearchEntityPrefix(Enum):
    """Links prefixes to tables"""

    PARTICIPANT = 'p'
    SAMPLE = 's'
    ASSAY = 'a'
    FAMILY = 'f'
    SEQUENCING_GROUP = 'sg'
