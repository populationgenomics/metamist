from typing import NewType

import strawberry

from models.enums.analysis import AnalysisStatus
from models.models.assay import AssayUpsert, AssayUpsertInternal
from models.models.cohort import NewCohortInternal
from models.models.participant import ParticipantUpsertInternal
from models.models.sample import SampleUpsertInternal
from models.models.sequencing_group import (
    SequencingGroupUpsert,
    SequencingGroupUpsertInternal,
)
from models.utils.cohort_id_format import cohort_id_format
from models.utils.sample_id_format import sample_id_format
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_format,
    sequencing_group_id_format_list,
)

CustomJSON = strawberry.scalar(
    NewType('CustomJSON', object),
    description='The `JSON` scalar type represents JSON values as specified by ECMA-404',
    serialize=lambda v: v,
    parse_value=lambda v: v,
)

AnalysisStatusType = strawberry.enum(AnalysisStatus)


@strawberry.input
class ProjectMemberUpdateInput:
    """Project member update input"""

    member: str
    roles: list[str]


@strawberry.input
class AnalysisInput:
    """Analysis input"""

    type: str
    status: AnalysisStatusType
    id: int | None
    output: str | None
    sequencing_group_ids: list[str] | None
    cohort_ids: list[str] | None
    author: str | None
    timestamp_completed: str | None
    project: int
    active: bool | None
    meta: strawberry.scalars.JSON


@strawberry.input
class AnalysisUpdateInput:
    """Analysis update input"""

    status: AnalysisStatusType
    output: str | None
    meta: strawberry.scalars.JSON | None
    active: bool | None


@strawberry.input  # type: ignore [misc]
class AssayUpsertInput:
    """Assay upsert input"""

    id: int | None
    type: str | None
    external_ids: strawberry.scalars.JSON | None
    sample_id: str | None
    meta: strawberry.scalars.JSON | None


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
            external_ids=internal.external_ids,
            sample_id=(
                sample_id_format(internal.sample_id) if internal.sample_id else None  # type: ignore [arg-type]
            ),
            meta=internal.meta,
        )

    @staticmethod
    def from_upsert_internal(internal: AssayUpsertInternal) -> 'AssayUpsertType':
        """Returns graphql model from upsert internal model"""
        return AssayUpsertType(
            id=internal.id,
            type=internal.type,
            external_ids=internal.external_ids,
            sample_id=(
                sample_id_format(internal.sample_id) if internal.sample_id else None
            ),
            meta=internal.meta,
        )


@strawberry.experimental.pydantic.input(model=SequencingGroupUpsert)  # type: ignore [misc]
class SequencingGroupUpsertInput:
    """Sequencing group upsert input"""

    id: str  # should really be int | str | None but strawberry throws an error "Type `int` cannot be used in a GraphQL Union"
    type: str | None
    technology: str | None
    platform: str | None
    meta: strawberry.scalars.JSON | None
    sample_id: str | None
    external_ids: strawberry.scalars.JSON | None

    assays: list[AssayUpsertInput] | None


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
            meta=internal.meta,
            sample_id=_sample_id,
            external_ids=internal.external_ids,
            assays=[
                AssayUpsertType.from_upsert_internal(a) for a in internal.assays or []
            ],
        )


@strawberry.input  # type: ignore [misc]
class SampleUpsertInput:
    """Sample upsert input"""

    id: int | None = None
    external_ids: strawberry.scalars.JSON | None = None
    meta: strawberry.scalars.JSON | None = None
    project: int | None = None
    type: str | None = None
    participant_id: int | None = None
    active: bool | None = None
    sequencing_groups: list[SequencingGroupUpsertInput] | None = None
    non_sequencing_assays: list[AssayUpsertInput] | None = None


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
            external_ids=sample.external_ids,
            meta=sample.meta,
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


@strawberry.input
class GetSamplesCriteria:
    """Get samples filter criteria"""

    sample_ids: list[str] | None = None
    meta: strawberry.scalars.JSON | None = None
    participant_ids: list[int] | None = None
    project_ids: list[str] | None = None
    active: bool = True


@strawberry.input
class ParticipantUpsertInput:
    """Participant upsert input"""

    id: int | None = None
    external_ids: strawberry.scalars.JSON | None = None
    reported_sex: int | None = None
    reported_gender: str | None = None
    karyotype: str | None = None
    meta: strawberry.scalars.JSON | None = None

    samples: list[SampleUpsertInput] | None = None


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
            external_ids=internal.external_ids,
            meta=internal.meta,
            reported_sex=internal.reported_sex,
            reported_gender=internal.reported_gender,
            karyotype=internal.karyotype,
            samples=[
                SampleUpsertType.from_upsert_internal(s) for s in internal.samples or []
            ],
        )


@strawberry.input
class QueryParticipantCriteria:
    """Query criteria for participants"""

    external_participant_ids: list[str] | None = None
    internal_participant_ids: list[int] | None = None


@strawberry.type
class UpdateParticipantFamilyType:
    """Update participant family type"""

    family_id: int
    participant_id: int

    @staticmethod
    def from_tuple(t: tuple[int, int]) -> 'UpdateParticipantFamilyType':
        """Returns graphql model from tuple"""
        return UpdateParticipantFamilyType(family_id=t[0], participant_id=t[1])


@strawberry.input
class FamilyUpdateInput:
    """Family update type"""

    id: int
    external_id: str | None
    description: str | None
    coded_phenotype: str | None


@strawberry.input
class CohortCriteriaInput:
    """Cohort criteria"""

    projects: list[str] | None
    sg_ids_internal: list[str] | None
    excluded_sgs_internal: list[str] | None
    sg_technology: list[str] | None
    sg_platform: list[str] | None
    sg_type: list[str] | None
    sample_type: list[str] | None


@strawberry.input
class CohortBodyInput:
    """Cohort body"""

    name: str
    description: str
    template_id: str | None


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


@strawberry.input
class CohortTemplateInput:
    """Cohort template input"""

    id: int | None
    name: str
    description: str
    criteria: CohortCriteriaInput
