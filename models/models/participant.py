import json

from db.python.utils import ProjectId
from models.base import SMBase
from models.models.family import FamilySimple, FamilySimpleInternal
from models.models.sample import (
    NestedSample,
    NestedSampleInternal,
    SampleUpsert,
    SampleUpsertInternal,
)


class ParticipantInternal(SMBase):
    """Update participant model"""

    id: int
    project: ProjectId
    external_id: str = None
    reported_sex: int | None = None
    reported_gender: str | None = None
    karyotype: str | None = None
    meta: dict

    @classmethod
    def from_db(cls, data: dict):
        """Convert from db keys, mainly converting parsing meta"""
        if 'meta' in data and isinstance(data['meta'], str):
            data['meta'] = json.loads(data['meta'])

        return ParticipantInternal(**data)

    def to_external(self):
        """Convert to transport model"""
        return Participant(
            id=self.id,
            project=self.project,
            external_id=self.external_id,
            reported_sex=self.reported_sex,
            reported_gender=self.reported_gender,
            karyotype=self.karyotype,
            meta=self.meta,
        )


class NestedParticipantInternal(SMBase):
    """ParticipantInternal with nested samples"""

    id: int
    external_id: str = None
    reported_sex: int | None = None
    reported_gender: str | None = None
    karyotype: str | None = None
    meta: dict
    samples: list[NestedSampleInternal] | None = None
    families: list[FamilySimpleInternal] | None = None

    def to_external(self):
        """Convert to transport model"""
        return NestedParticipant(
            id=self.id,
            external_id=self.external_id,
            reported_sex=self.reported_sex,
            reported_gender=self.reported_gender,
            karyotype=self.karyotype,
            meta=self.meta,
            samples=[s.to_external() for s in self.samples or []],
            families=[f.to_external() for f in self.families or []],
        )


class ParticipantUpsertInternal(SMBase):
    """Internal upsert model for participant"""

    id: int | None = None
    external_id: str = None
    reported_sex: int | None = None
    reported_gender: str | None = None
    karyotype: str | None = None
    meta: dict | None = None

    samples: list[SampleUpsertInternal] | None = None

    def to_external(self):
        """Convert to transport model"""
        return ParticipantUpsert(
            id=self.id,
            external_id=self.external_id,
            reported_sex=self.reported_sex,
            reported_gender=self.reported_gender,
            karyotype=self.karyotype,
            meta=self.meta,
            samples=[s.to_external() for s in self.samples or []],
        )


class Participant(SMBase):
    """External participant model"""

    id: int
    project: ProjectId
    external_id: str = None
    reported_sex: int | None = None
    reported_gender: str | None = None
    karyotype: str | None = None
    meta: dict


class NestedParticipant(SMBase):
    """External participant model with nested samples"""

    id: int
    external_id: str = None
    reported_sex: int | None = None
    reported_gender: str | None = None
    karyotype: str | None = None
    meta: dict

    samples: list[NestedSample]
    families: list[FamilySimple]


class ParticipantUpsert(SMBase):
    """External upsert model for participant"""

    id: int | None = None
    external_id: str | None = None
    reported_sex: int | None = None
    reported_gender: str | None = None
    karyotype: str | None = None
    meta: dict | None = None

    samples: list[SampleUpsert] | None = None

    def to_internal(self):
        """Convert to internal model, doesn't really do much"""
        p = ParticipantUpsertInternal(
            id=self.id,
            external_id=self.external_id,
            reported_sex=self.reported_sex,
            reported_gender=self.reported_gender,
            karyotype=self.karyotype,
            meta=self.meta,
        )

        if self.samples:
            p.samples = [s.to_internal() for s in self.samples or []]

        return p
