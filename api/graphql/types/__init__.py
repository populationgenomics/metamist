import strawberry

from models.models.assay import AssayUpsert
from models.models.sample import SampleUpsert
from models.models.sequencing_group import SequencingGroupUpsert


@strawberry.experimental.pydantic.input(model=AssayUpsert)  # type: ignore [misc]
class AssayUpsertInput():
    """Assay upsert input"""

    id: int | None
    type: str | None
    external_ids: strawberry.scalars.JSON | None
    sample_id: str | None
    meta: strawberry.scalars.JSON | None


@strawberry.experimental.pydantic.type(model=AssayUpsert)  # type: ignore [misc]
class AssayUpsertType():
    """Assay upsert type"""

    id: int | None
    type: str | None
    external_ids: strawberry.scalars.JSON | None
    sample_id: str | None
    meta: strawberry.scalars.JSON | None


@strawberry.experimental.pydantic.input(model=SequencingGroupUpsert)  # type: ignore [misc]
class SequencingGroupUpsertInput():
    """Sequencing group upsert input"""

    id: str  # should really be int | str | None but strawberry throws an error "Type `int` cannot be used in a GraphQL Union"
    type: str | None
    technology: str | None
    platform: str | None
    meta: strawberry.scalars.JSON | None
    sample_id: str | None
    external_ids: strawberry.scalars.JSON | None

    assays: list[AssayUpsertInput] | None


@strawberry.experimental.pydantic.type(model=SampleUpsert)  # type: ignore [misc]
class SequencingGroupUpsertType():
    """Sequencing group upsert type"""

    id: str  # should really be int | str | None but strawberry throws an error "Type `int` cannot be used in a GraphQL Union"
    type: str | None
    technology: str | None
    platform: str | None
    meta: strawberry.scalars.JSON | None
    sample_id: str | None
    external_ids: strawberry.scalars.JSON | None
    assays: list[AssayUpsertType] | None


@strawberry.experimental.pydantic.input(model=SampleUpsert)  # type: ignore [misc]
class SampleUpsertInput():
    """Sample upsert input"""

    id: str | None
    external_id: str | None
    meta: strawberry.scalars.JSON | None
    project: int | None
    type: str | None
    participant_id: int | None
    active: bool | None
    sequencing_groups: list[SequencingGroupUpsertInput] | None = None
    non_sequencing_assays: list[AssayUpsertInput] | None = None


@strawberry.experimental.pydantic.type(model=SampleUpsert)  # type: ignore [misc]
class SampleUpsertType():
    """Sample upsert type"""

    id: str | None
    external_id: str | None
    meta: strawberry.scalars.JSON | None
    project: int | None
    type: str | None
    participant_id: int | None
    active: bool | None
    sequencing_groups: list[SequencingGroupUpsertType] | None = None
    non_sequencing_assays: list[AssayUpsertType] | None = None
