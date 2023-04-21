import json
from typing import Any

from pydantic import BaseModel

from models.utils.sample_id_format import (
    sample_id_format,
    sample_id_transform_to_raw,
)


class AssayInternal(BaseModel):
    """Internal model for Assay"""

    id: int | None
    sample_id: int
    meta: dict[str, Any] | None
    type: str
    external_ids: dict[str, str] | None = None

    def __repr__(self):
        return ', '.join(f'{k}={v}' for k, v in vars(self).items())

    def __eq__(self, other):
        if self.id is not None:
            return self.id == other.id
        return False

    @staticmethod
    def from_db(d: dict):
        """Take DB mapping object, and return SampleSequencing"""
        meta = d.pop('meta', None)

        if meta:
            if isinstance(meta, bytes):
                meta = meta.decode()
            if isinstance(meta, str):
                meta = json.loads(meta)
        return AssayInternal(meta=meta, **d)

    def to_external(self):
        """Convert to transport model"""
        return Assay(
            id=self.id,
            type=self.type,
            external_ids=self.external_ids,
            sample_id=sample_id_format(self.sample_id),
            meta=self.meta,
        )


class AssayUpsertInternal(BaseModel):
    """Internal upsert model for assay"""

    id: int | None = None
    type: str | None = None
    external_ids: dict[str, str | None] | None = None
    sample_id: int | None = None
    meta: dict | None = None

    def to_external(self):
        """Convert to external model"""
        return AssayUpsert(
            id=self.id,
            type=self.type,
            external_ids=self.external_ids,
            sample_id=sample_id_format(self.sample_id) if self.sample_id else None,
            meta=self.meta,
        )


class Assay(BaseModel):
    """Asssay model for external use"""

    id: int
    external_ids: dict[str, str]
    sample_id: str
    meta: dict[str, Any]
    type: str

    def to_internal(self):
        """Convert to internal model"""
        return AssayInternal(
            id=self.id,
            type=self.type,
            external_ids=self.external_ids,
            sample_id=sample_id_transform_to_raw(self.sample_id),
            meta=self.meta,
        )


class AssayUpsert(BaseModel):
    """Assay upsert model for external use"""

    id: int | None = None
    type: str | None = None
    external_ids: dict[str, str] | None = None
    sample_id: str | None = None
    meta: dict[str, Any] | None = None

    def to_internal(self):
        """Convert to internal model"""

        # Sample ID may be left blank + passed in during the parsers
        _sample_id = None
        if self.sample_id:
            # but may be provided directly when inserting directly
            _sample_id = sample_id_transform_to_raw(self.sample_id)

        return AssayUpsertInternal(
            id=self.id,
            type=self.type,
            external_ids=self.external_ids,
            sample_id=_sample_id,
            meta=self.meta,
        )
