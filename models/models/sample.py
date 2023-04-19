import json

from models.base import SMBase
from models.models.assay import AssayUpsertInternal, AssayUpsert
from models.models.sequencing_group import (
    SequencingGroupUpsert,
    SequencingGroupUpsertInternal,
)
from models.utils.sample_id_format import (
    sample_id_format,
    sample_id_transform_to_raw,
)


class SampleInternal(SMBase):
    """Internal model for a Sample"""

    id: int
    external_id: str
    meta: dict
    project: int
    type: str | None
    participant_id: int | None
    active: bool | None
    author: str | None

    @staticmethod
    def from_db(d: dict):
        """
        Convert from db keys, mainly converting id to id_
        """
        _id = d.pop('id', None)
        type_ = d.pop('type', None)
        meta = d.pop('meta', None)
        active = d.pop('active', None)
        if active is not None:
            active = bool(active)
        if meta:
            if isinstance(meta, bytes):
                meta = meta.decode()
            if isinstance(meta, str):
                meta = json.loads(meta)

        return SampleInternal(id=_id, type=str(type_), meta=meta, active=active, **d)

    def to_external(self):
        """Convert to transport model"""
        return Sample(
            id=sample_id_format(self.id),
            external_id=self.external_id,
            meta=self.meta,
            project=self.project,
            type=self.type,
            participant_id=self.participant_id,
            active=self.active,
        )


class SampleUpsertInternal(SMBase):
    """Internal upsert model for sample"""

    id: int | None = None
    external_id: str | None = None
    meta: dict | None = None
    project: int | None = None
    type: str | None = None
    participant_id: int | None = None
    active: bool | None = None

    sequencing_groups: list[SequencingGroupUpsertInternal] | None = None
    non_sequencing_assays: list[AssayUpsertInternal] | None = None

    def to_external(self):
        """Convert to transport model"""
        _id = None
        if self.id:
            _id = sample_id_format(self.id)

        return SampleUpsert(
            id=_id,
            external_id=self.external_id,
            meta=self.meta,
            project=self.project,
            type=self.type,
            participant_id=self.participant_id,
            active=self.active,
            sequencing_groups=[sg.to_external() for sg in self.sequencing_groups or []],
            non_sequencing_assays=[
                a.to_external() for a in self.non_sequencing_assays or []
            ],
        )


class Sample(SMBase):
    """Model for a Sample"""

    id: str
    external_id: str
    meta: dict
    project: int
    type: str | None
    participant_id: int | None
    active: bool | None
    author: str | None = None

    def to_internal(self):
        """Convert to internal model"""
        return SampleInternal(
            id=sample_id_transform_to_raw(self.id),
            external_id=self.external_id,
            meta=self.meta,
            project=self.project,
            type=self.type,
            participant_id=self.participant_id,
            active=self.active,
        )


class SampleUpsert(SMBase):
    """Upsert model for a Sample"""

    id: str | None = None
    external_id: str | None = None
    meta: dict | None = None
    project: int | None = None
    type: str | None = None
    participant_id: int | None = None
    active: bool | None = None

    sequencing_groups: list[SequencingGroupUpsert] | None = None
    non_sequencing_assays: list[AssayUpsert] | None = None

    def to_internal(self) -> SampleUpsertInternal:
        """Convert to internal model"""
        _id = None
        if self.id:
            _id = sample_id_transform_to_raw(self.id)

        sample_upsert = SampleUpsertInternal(
            id=_id,
            external_id=self.external_id,
            meta=self.meta,
            project=self.project,
            type=self.type,
            participant_id=self.participant_id,
            active=self.active,
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
