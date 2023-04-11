from models.base import SMBase
from models.models.assay import AssayUpsert, AssayUpsertInternal, Assay, AssayInternal
from models.utils.sample_id_format import sample_id_transform_to_raw, sample_id_format
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_transform_to_raw,
    sequencing_group_id_format,
)


class SequencingGroupInternal(SMBase):
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
    id: int
    type: str
    technology: str
    platform: str  # uppercase
    meta: dict[str, str]
    sample_id: int

    project: int | None = None

    assays: list[AssayInternal] | None = None

    def to_external(self):
        """Convert to transport model"""
        return SequencingGroup(
            id=sequencing_group_id_format(self.id),
            type=self.type,
            technology=self.technology,
            platform=self.platform,
            meta=self.meta,
            sample_id=sample_id_format(self.sample_id),
            assays=[a.to_external() for a in self.assays or []],
        )


class SequencingGroupUpsertInternal(SMBase):
    """
    Upsert model for sequence group
    """

    id: int | None = None
    type: str
    technology: str  # fk
    platform: str | None  # fk
    meta: dict[str, str] | None = None
    sample_id: int | None = None

    assays: list[AssayUpsertInternal] | None = None


class SequencingGroup(SMBase):
    """External model for sequencing group"""

    id: str
    type: str
    technology: str
    platform: str  # uppercase
    meta: dict[str, str]
    sample_id: str

    assays: list[Assay] | None = None


class SequencingGroupUpsert(SMBase):
    """
    Upsert model for sequence group
    """

    id: int | str | None = None
    type: str
    technology: str
    platform: str  # uppercase
    meta: dict[str, str] | None = None
    sample_id: str | None = None

    assays: list[AssayUpsert] | None = None

    def to_internal(self) -> SequencingGroupUpsertInternal:
        """
        Convert to internal model
        """
        _id = None
        if self.id is not None:
            _id = sequencing_group_id_transform_to_raw(self.id)

        _sample_id = None
        if self.sample_id is not None:
            _sample_id = sample_id_transform_to_raw(self.sample_id)

        sg_internal = SequencingGroupUpsertInternal(
            id=_id,
            type=self.type,
            technology=self.technology,
            platform=self.platform,
            meta=self.meta,
            sample_id=_sample_id,
        )

        if self.assays is not None:
            sg_internal.assays = [a.to_internal() for a in self.assays]

        return sg_internal
