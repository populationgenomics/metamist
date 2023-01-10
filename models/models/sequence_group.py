from models.base import SMBase
from models.enums import SequenceType, SequenceTechnology


class SequenceGroup(SMBase):
    """
    A group of sequences that would be aligned and analysed together.
    A SequenceGroup must contain sequences that have the:
        same type + technology, ie: genome + short-read

    - They have an identifier, that realistically is what we should use in place of a
        sample identifier. We shouldn't use the sample ID for keying analyses results,
        because we could have multiple gvcfs with the same name + IDs, even though
        they use different types / technologies.
    - Sequence group members are immutable, a change in members results in a new group,
        this would invalidate any downstream results.
    - We probably should only have one active sequence group per type / tech / sample
    - This is also the ID we should use in analysis, instead of samples
    """

    # similar to a sample ID, this is stored internally as an integer,
    # but displayed as a string
    id: int | str
    type: SequenceType
    technology: SequenceTechnology
    platform: str  # uppercase
    meta: dict[str, str]
