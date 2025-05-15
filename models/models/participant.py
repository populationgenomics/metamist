from typing import Any

from models.base import OpenApiGenNoneType, SMBase, parse_sql_dict
from models.models.family import FamilySimple, FamilySimpleInternal
from models.models.project import ProjectId
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
    external_ids: dict[str, str]
    reported_sex: int | None = None
    reported_gender: str | None = None
    karyotype: str | None = None
    meta: dict

    audit_log_id: int | None = None

    @classmethod
    def from_db(cls, data: dict):
        """Convert from db keys, mainly converting JSON-encoded fields"""
        meta = parse_sql_dict(data.pop('meta', {}))
        external_ids = parse_sql_dict(data.pop('external_ids', {}))
        return ParticipantInternal(**data, meta=meta, external_ids=external_ids)

    def to_external(self):
        """Convert to transport model"""
        return Participant(
            id=self.id,
            project=self.project,
            external_ids=self.external_ids,
            reported_sex=self.reported_sex,
            reported_gender=self.reported_gender,
            karyotype=self.karyotype,
            meta=self.meta,
        )


class NestedParticipantInternal(SMBase):
    """ParticipantInternal with nested samples"""

    id: int
    external_ids: dict[str, str]
    reported_sex: int | None = None
    reported_gender: str | None = None
    karyotype: str | None = None
    meta: dict
    samples: list[NestedSampleInternal]
    families: list[FamilySimpleInternal]

    def to_external(self):
        """Convert to transport model"""
        return NestedParticipant(
            id=self.id,
            external_ids=self.external_ids,
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
    external_ids: dict[str, str | None] | None = None
    reported_sex: int | None = None
    reported_gender: str | None = None
    karyotype: str | None = None
    meta: dict | None = None

    samples: list[SampleUpsertInternal] | None = None
    phenotypes: dict[str, Any] | None = None

    def to_external(self):
        """Convert to transport model"""
        return ParticipantUpsert(
            id=self.id,
            external_ids=self.external_ids,  # type: ignore
            reported_sex=self.reported_sex,
            reported_gender=self.reported_gender,
            karyotype=self.karyotype,
            meta=self.meta,
            phenotypes=self.phenotypes,
            samples=[s.to_external() for s in self.samples or []],
        )


class Participant(SMBase):
    """External participant model"""

    id: int
    project: ProjectId
    external_ids: dict[str, str]
    reported_sex: int | None = None
    reported_gender: str | None = None
    karyotype: str | None = None
    meta: dict


class NestedParticipant(SMBase):
    """External participant model with nested samples"""

    id: int
    external_ids: dict[str, str]
    reported_sex: int | None = None
    reported_gender: str | None = None
    karyotype: str | None = None
    meta: dict

    samples: list[NestedSample]
    families: list[FamilySimple]


class ParticipantUpsert(SMBase):
    """External upsert model for participant"""

    id: int | OpenApiGenNoneType = None
    external_ids: dict[str, str | OpenApiGenNoneType] | OpenApiGenNoneType = None
    reported_sex: int | OpenApiGenNoneType = None
    reported_gender: str | OpenApiGenNoneType = None
    karyotype: str | OpenApiGenNoneType = None
    meta: dict | OpenApiGenNoneType = None

    samples: list[SampleUpsert] | None = None
    phenotypes: dict[str, Any] | None = None

    def to_internal(self):
        """Convert to internal model, doesn't really do much"""
        p = ParticipantUpsertInternal(
            id=self.id,  # type: ignore
            external_ids=self.external_ids,  # type: ignore
            reported_sex=self.reported_sex,  # type: ignore
            reported_gender=self.reported_gender,  # type: ignore
            karyotype=self.karyotype,  # type: ignore
            phenotypes=self.phenotypes,
            meta=self.meta,  # type: ignore
        )

        if self.samples:
            p.samples = [s.to_internal() for s in self.samples or []]

        return p
