import strawberry

from models.enums.analysis import AnalysisStatus
from models.models.assay import AssayUpsert, AssayUpsertInternal
from models.models.cohort import NewCohortInternal
from models.models.participant import ParticipantUpsertInternal
from models.models.sample import SampleUpsertInternal
from models.models.sequencing_group import SequencingGroupUpsertInternal
from models.utils.cohort_id_format import cohort_id_format
from models.utils.sample_id_format import sample_id_format
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_format,
    sequencing_group_id_format_list,
)


AnalysisStatusType = strawberry.enum(AnalysisStatus)  # type: ignore [misc]


@strawberry.type  # type: ignore [misc]
class AssayUpsertType:
    """Assay upsert type"""

    id: int | None
    type: str | None
    external_ids: strawberry.scalars.JSON | None
    sample_id: str | None
    meta: strawberry.scalars.JSON | None

    @staticmethod
    def from_internal(internal: AssayUpsert) -> 'AssayUpsertType':
        """Returns graphql model from internal model"""

        return AssayUpsertType(
            id=internal.id,  # type: ignore [arg-type]
            type=internal.type,  # type: ignore [arg-type]
            external_ids=internal.external_ids,  # type: ignore [arg-type]
            sample_id=(
                sample_id_format(internal.sample_id) if internal.sample_id else None  # type: ignore [arg-type]
            ),
            meta=internal.meta,  # type: ignore [arg-type]
        )

    @staticmethod
    def from_upsert_internal(internal: AssayUpsertInternal) -> 'AssayUpsertType':
        """Returns graphql model from upsert internal model"""
        return AssayUpsertType(
            id=internal.id,
            type=internal.type,
            external_ids=internal.external_ids,  # type: ignore [arg-type]
            sample_id=(
                sample_id_format(internal.sample_id) if internal.sample_id else None
            ),
            meta=internal.meta,  # type: ignore [arg-type]
        )


@strawberry.type  # type: ignore [misc]
class SequencingGroupUpsertType:
    """Sequencing group upsert type"""

    id: (
        str | None
    )  # should really be int | str | None but strawberry throws an error "Type `int` cannot be used in a GraphQL Union"
    type: str | None
    technology: str | None
    platform: str | None
    meta: strawberry.scalars.JSON | None
    sample_id: str | None
    external_ids: strawberry.scalars.JSON | None
    assays: list[AssayUpsertType] | None

    @staticmethod
    def from_upsert_internal(
        internal: SequencingGroupUpsertInternal,
    ) -> 'SequencingGroupUpsertType':
        """Returns graphql model from upsert internal model"""
        _id = None
        if internal.id is not None:
            _id = sequencing_group_id_format(internal.id)

        _sample_id = None
        if internal.sample_id is not None:
            _sample_id = sample_id_format(internal.sample_id)
        return SequencingGroupUpsertType(
            id=_id,
            type=internal.type,
            technology=internal.technology,
            platform=internal.platform,
            meta=internal.meta,  # type: ignore [arg-type]
            sample_id=_sample_id,
            external_ids=internal.external_ids,  # type: ignore [arg-type]
            assays=[
                AssayUpsertType.from_upsert_internal(a) for a in internal.assays or []
            ],
        )


@strawberry.type  # type: ignore [misc]
class SampleUpsertType:
    """Sample upsert type"""

    id: str | None
    external_ids: strawberry.scalars.JSON | None
    meta: strawberry.scalars.JSON | None
    project: int | None
    type: str | None
    participant_id: int | None
    active: bool | None
    sequencing_groups: list[SequencingGroupUpsertType] | None = None
    non_sequencing_assays: list[AssayUpsertType] | None = None

    @staticmethod
    def from_upsert_internal(sample: SampleUpsertInternal) -> 'SampleUpsertType':
        """Returns graphql model from internal model"""
        _id = None
        if sample.id:
            _id = sample_id_format(sample.id)

        return SampleUpsertType(
            id=_id,
            external_ids=sample.external_ids,  # type: ignore [arg-type]
            meta=sample.meta,  # type: ignore [arg-type]
            project=sample.project,
            type=sample.type,
            participant_id=sample.participant_id,
            active=sample.active,
            # TODO Fix sequencing group serialization
            sequencing_groups=[
                SequencingGroupUpsertType.from_upsert_internal(sg)
                for sg in sample.sequencing_groups or []
            ],
            non_sequencing_assays=[
                AssayUpsertType.from_upsert_internal(a)
                for a in sample.non_sequencing_assays or []
            ],
        )


@strawberry.type
class ParticipantUpsertType:
    """Participant upsert input"""

    id: int | None = None
    external_ids: strawberry.scalars.JSON | None = None
    reported_sex: int | None = None
    reported_gender: str | None = None
    karyotype: str | None = None
    meta: strawberry.scalars.JSON | None = None

    samples: list[SampleUpsertType] | None = None

    @staticmethod
    def from_upsert_internal(
        internal: ParticipantUpsertInternal,
    ) -> 'ParticipantUpsertType':
        """Returns graphql model from upsert internal model"""
        return ParticipantUpsertType(
            id=internal.id,
            external_ids=internal.external_ids,  # type: ignore [arg-type]
            meta=internal.meta,  # type: ignore [arg-type]
            reported_sex=internal.reported_sex,
            reported_gender=internal.reported_gender,
            karyotype=internal.karyotype,
            samples=[
                SampleUpsertType.from_upsert_internal(s) for s in internal.samples or []
            ],
        )


@strawberry.type
class UpdateParticipantFamilyType:
    """Update participant family type"""

    family_id: int
    participant_id: int

    @staticmethod
    def from_tuple(t: tuple[int, int]) -> 'UpdateParticipantFamilyType':
        """Returns graphql model from tuple"""
        return UpdateParticipantFamilyType(family_id=t[0], participant_id=t[1])


@strawberry.type
class NewCohortType:
    """Represents a cohort, which is a collection of sequencing groups."""

    dry_run: bool
    cohort_id: str
    sequencing_group_ids: list[str]

    @staticmethod
    def from_internal(internal: NewCohortInternal) -> 'NewCohortType':
        """Returns graphql model from internal model"""
        return NewCohortType(
            dry_run=internal.dry_run,
            cohort_id=(
                cohort_id_format(internal.cohort_id)
                if internal.cohort_id
                else 'CREATE NEW'
            ),
            sequencing_group_ids=(
                sequencing_group_id_format_list(internal.sequencing_group_ids)
                if internal.sequencing_group_ids
                else []
            ),
        )
