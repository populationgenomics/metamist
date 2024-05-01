import json

from models.base import OpenApiGenNoneType, SMBase
from models.models.assay import Assay, AssayInternal, AssayUpsert, AssayUpsertInternal
from models.utils.sample_id_format import sample_id_format, sample_id_transform_to_raw
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_format,
    sequencing_group_id_transform_to_raw,
)

SequencingGroupInternalId = int
SequencingGroupExternalId = str


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
    id: SequencingGroupInternalId | None = None
    type: str | None = None
    technology: str | None = None
    platform: str | None = None
    meta: dict[str, str] | None = None
    sample_id: int | None = None
    external_ids: dict[str, str] | None = {}
    archived: bool | None = None

    project: int | None = None

    assays: list[AssayInternal] | None = None

    @classmethod
    def from_db(cls, **kwargs):
        """From database model"""
        meta = kwargs.pop('meta')
        if meta and isinstance(meta, str):
            meta = json.loads(meta)

        _archived = kwargs.pop('archived', None)
        if _archived is not None:
            if isinstance(_archived, int):
                _archived = _archived != 0
            elif isinstance(_archived, bytes):
                _archived = _archived != b'\x00'
            else:
                raise TypeError(
                    f"Received type '{type(_archived)}' for SequencingGroup column 'archived'. "
                    + "Allowed types are either 'int' or 'bytes'."
                )

        return SequencingGroupInternal(**kwargs, archived=_archived, meta=meta)

    def to_external(self):
        """Convert to transport model"""
        return SequencingGroup(
            id=sequencing_group_id_format(self.id),
            type=self.type,
            technology=self.technology,
            platform=self.platform,
            external_ids=self.external_ids,
            meta=self.meta,
            sample_id=sample_id_format(self.sample_id),
            assays=[a.to_external() for a in self.assays or []],
            archived=self.archived,
        )


class NestedSequencingGroupInternal(SMBase):
    """SequencingGroupInternal with nested assays"""

    id: SequencingGroupInternalId | None = None
    type: str | None = None
    technology: str | None = None
    platform: str | None = None
    meta: dict[str, str] | None = None
    external_ids: dict[str, str] | None = None

    assays: list[AssayInternal] | None = None

    def to_external(self):
        """Convert to transport model"""
        return NestedSequencingGroup(
            id=sequencing_group_id_format(self.id),
            type=self.type,
            technology=self.technology,
            platform=self.platform,
            meta=self.meta,
            external_ids=self.external_ids,
            assays=[a.to_external() for a in self.assays or []],
        )


class SequencingGroupUpsertInternal(SMBase):
    """
    Upsert model for sequence group
    """

    id: SequencingGroupInternalId | None = None
    type: str | None = None
    technology: str | None = None  # fk
    platform: str | None = None  # fk
    meta: dict[str, str] | None = None
    sample_id: int | None = None
    external_ids: dict[str, str] | None = None

    assays: list[AssayUpsertInternal] | None = None

    def to_external(self):
        """
        Convert to external model
        """
        _id = None
        if self.id is not None:
            _id = sequencing_group_id_format(self.id)

        _sample_id = None
        if self.sample_id is not None:
            _sample_id = sample_id_format(self.sample_id)

        return SequencingGroupUpsert(
            id=_id,
            type=self.type,
            technology=self.technology,
            platform=self.platform,
            meta=self.meta,
            sample_id=_sample_id,
            assays=[a.to_external() for a in self.assays or []],
        )


class SequencingGroup(SMBase):
    """External model for sequencing group"""

    id: SequencingGroupExternalId
    type: str
    technology: str
    platform: str  # uppercase
    meta: dict[str, str]
    sample_id: str
    external_ids: dict[str, str]
    archived: bool
    assays: list[Assay]


class NestedSequencingGroup(SMBase):
    """External model for sequencing group with nested assays"""

    id: SequencingGroupExternalId
    type: str
    technology: str
    platform: str
    meta: dict[str, str]
    external_ids: dict[str, str]

    assays: list[Assay] | None = None


class SequencingGroupUpsert(SMBase):
    """
    Upsert model for sequence group
    """

    id: int | str | OpenApiGenNoneType = None
    type: str | OpenApiGenNoneType = None
    technology: str | OpenApiGenNoneType = None
    platform: str | OpenApiGenNoneType = None
    meta: dict[str, str] | OpenApiGenNoneType = None
    sample_id: str | OpenApiGenNoneType = None
    external_ids: dict[str, str] | OpenApiGenNoneType = None

    assays: list[AssayUpsert] | OpenApiGenNoneType = None

    def to_internal(self) -> SequencingGroupUpsertInternal:
        """
        Convert to internal model
        """
        _id = None
        if self.id is not None:
            _id = sequencing_group_id_transform_to_raw(str(self.id))

        _sample_id = None
        if self.sample_id is not None:
            _sample_id = sample_id_transform_to_raw(str(self.sample_id))

        sg_internal = SequencingGroupUpsertInternal(
            id=_id,
            type=self.type,  # type: ignore
            technology=self.technology,  # type: ignore
            platform=self.platform.lower() if self.platform else None,  # type: ignore
            meta=self.meta,  # type: ignore
            sample_id=_sample_id,
            external_ids=self.external_ids or {},  # type: ignore
        )

        if self.assays is not None:
            sg_internal.assays = [a.to_internal() for a in self.assays]  # type: ignore

        return sg_internal
