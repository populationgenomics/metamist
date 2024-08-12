from models.base import OpenApiGenNoneType, SMBase, parse_sql_bool, parse_sql_dict
from models.models.assay import Assay, AssayInternal, AssayUpsert, AssayUpsertInternal
from models.models.sequencing_group import (
    NestedSequencingGroup,
    NestedSequencingGroupInternal,
    SequencingGroupUpsert,
    SequencingGroupUpsertInternal,
)
from models.utils.sample_id_format import sample_id_format, sample_id_transform_to_raw


class SampleInternal(SMBase):
    """Internal model for a Sample"""

    id: int
    external_ids: dict[str, str]
    meta: dict
    project: int
    type: str | None
    participant_id: int | None
    active: bool | None
    sample_root_id: int | None
    sample_parent_id: int | None
    author: str | None = None

    @staticmethod
    def from_db(d: dict):
        """
        Convert from db keys, mainly converting id to id_
        """
        _id = d.pop('id', None)
        type_ = d.pop('type', None)
        meta = parse_sql_dict(d.pop('meta', None)) or {}
        active = parse_sql_bool(d.pop('active', None))

        external_ids = parse_sql_dict(d.pop('external_ids', None))

        if not external_ids:
            raise ValueError(f'Sample {sample_id_format(_id)} has no external_ids')

        return SampleInternal(
            id=_id,
            type=str(type_),
            meta=meta,
            active=active,
            external_ids=external_ids,
            **d,
        )

    def to_external(self):
        """Convert to transport model"""
        return Sample(
            id=sample_id_format(self.id),
            external_ids=self.external_ids,
            meta=self.meta,
            project=self.project,
            type=self.type,
            participant_id=self.participant_id,
            sample_root_id=self.sample_root_id,
            sample_parent_id=self.sample_parent_id,
            active=self.active,
        )


class NestedSampleInternal(SMBase):
    """SampleInternal with nested sequencing groups and assays"""

    id: int
    external_ids: dict[str, str]
    meta: dict
    type: str | None
    active: bool | None
    created_date: str | None

    sample_root_id: int | None
    sample_parent_id: int | None

    sequencing_groups: list[NestedSequencingGroupInternal]
    non_sequencing_assays: list[AssayInternal]

    def to_external(self):
        """Convert to transport model"""
        return NestedSample(
            id=sample_id_format(self.id),
            external_ids=self.external_ids,
            meta=self.meta,
            type=self.type,
            created_date=self.created_date,
            sample_root_id=(
                sample_id_format(self.sample_root_id) if self.sample_root_id else None
            ),
            sample_parent_id=(
                sample_id_format(self.sample_parent_id)
                if self.sample_parent_id
                else None
            ),
            sequencing_groups=[sg.to_external() for sg in self.sequencing_groups],
            non_sequencing_assays=[a.to_external() for a in self.non_sequencing_assays],
        )


class SampleUpsertInternal(SMBase):
    """Internal upsert model for sample"""

    id: int | None = None
    external_ids: dict[str, str | None] | None = None
    meta: dict | None = None
    project: int | None = None
    type: str | None = None
    participant_id: int | None = None
    active: bool | None = None

    nested_samples: list['SampleUpsertInternal'] | None = None

    sequencing_groups: list[SequencingGroupUpsertInternal] | None = None
    non_sequencing_assays: list[AssayUpsertInternal] | None = None

    def update_participant_id(self, participant_id: int):
        """Update the participant ID for the samples"""
        self.participant_id = participant_id
        for s in self.nested_samples or []:
            s.participant_id = participant_id

    def to_external(self):
        """Convert to transport model"""
        _id = None
        if self.id:
            _id = sample_id_format(self.id)

        return SampleUpsert(
            id=_id,
            external_ids=self.external_ids,  # type: ignore
            meta=self.meta,
            project=self.project,
            type=self.type,
            participant_id=self.participant_id,
            active=self.active,
            sequencing_groups=[sg.to_external() for sg in self.sequencing_groups or []],
            non_sequencing_assays=[
                a.to_external() for a in self.non_sequencing_assays or []
            ],
            nested_samples=[ns.to_external() for ns in self.nested_samples or []],
        )


class Sample(SMBase):
    """Model for a Sample"""

    id: str
    external_ids: dict[str, str]
    meta: dict
    project: int
    type: str | None
    participant_id: int | None
    active: bool | None
    sample_root_id: int | None
    sample_parent_id: int | None
    author: str | None = None

    def to_internal(self):
        """Convert to internal model"""
        return SampleInternal(
            id=sample_id_transform_to_raw(self.id),
            external_ids=self.external_ids,
            meta=self.meta,
            project=self.project,
            type=self.type,
            participant_id=self.participant_id,
            active=self.active,
            sample_root_id=self.sample_root_id,
            sample_parent_id=self.sample_parent_id,
            author='<EXT-TO-INT-CONVERSION>',
        )


class NestedSample(SMBase):
    """External sample model with nested sequencing groups and assays"""

    id: str
    external_ids: dict[str, str]
    meta: dict
    type: str | None
    created_date: str | None
    sample_root_id: str | None
    sample_parent_id: str | None

    sequencing_groups: list[NestedSequencingGroup] | None = None
    non_sequencing_assays: list[Assay]


class SampleUpsert(SMBase):
    """Upsert model for a Sample"""

    id: str | OpenApiGenNoneType = None
    external_ids: dict[str, str | OpenApiGenNoneType] | OpenApiGenNoneType = None
    meta: dict | OpenApiGenNoneType = None
    project: int | OpenApiGenNoneType = None
    type: str | OpenApiGenNoneType = None
    participant_id: int | OpenApiGenNoneType = None
    active: bool | OpenApiGenNoneType = None

    nested_samples: list['SampleUpsert'] | None = None
    sequencing_groups: list[SequencingGroupUpsert] | None = None
    non_sequencing_assays: list[AssayUpsert] | None = None

    def to_internal(self) -> SampleUpsertInternal:
        """Convert to internal model"""
        _id = None
        if self.id:
            _id = sample_id_transform_to_raw(str(self.id))

        sample_upsert = SampleUpsertInternal(
            id=_id,
            external_ids=self.external_ids,  # type: ignore
            meta=self.meta,  # type: ignore
            project=self.project,  # type: ignore
            type=self.type,  # type: ignore
            participant_id=self.participant_id,  # type: ignore
            active=self.active,  # type: ignore
            nested_samples=[ns.to_internal() for ns in (self.nested_samples or [])],
        )

        if self.sequencing_groups:
            sample_upsert.sequencing_groups = [
                sg.to_internal() for sg in (self.sequencing_groups or [])
            ]

        if self.non_sequencing_assays:
            sample_upsert.non_sequencing_assays = [
                a.to_internal() for a in (self.non_sequencing_assays or [])
            ]

        return sample_upsert
