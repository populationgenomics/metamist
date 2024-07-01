# pylint: disable=too-many-instance-attributes
import dataclasses

from db.python.filters.generic import (
    GenericFilter,
    GenericFilterModel,
    GenericMetaFilter,
)
from db.python.filters.sample import SampleFilter
from models.models.project import ProjectId


@dataclasses.dataclass(kw_only=True)
class ParticipantFilter(GenericFilterModel):
    """
    Participant filter model
    """

    @dataclasses.dataclass(kw_only=True)
    class ParticipantFamilyFilter(GenericFilterModel):
        """
        Participant sample filter model
        """

        id: GenericFilter[int] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None

    @dataclasses.dataclass(kw_only=True)
    class ParticipantSampleFilter(GenericFilterModel):
        """
        Participant sample filter model
        """

        id: GenericFilter[int] | None = None
        type: GenericFilter[str] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None

        sample_root_id: GenericFilter[int] | None = None
        sample_parent_id: GenericFilter[int] | None = None

    @dataclasses.dataclass(kw_only=True)
    class ParticipantSequencingGroupFilter(GenericFilterModel):
        """
        Participant sequencing group filter model
        """

        id: GenericFilter[int] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None
        type: GenericFilter[str] | None = None
        technology: GenericFilter[str] | None = None
        platform: GenericFilter[str] | None = None

        archived: GenericFilter[bool] | None = dataclasses.field(
            default_factory=lambda: GenericFilter(eq=False)
        )

    @dataclasses.dataclass(kw_only=True)
    class ParticipantAssayFilter(GenericFilterModel):
        """
        Participant assay filter model
        """

        id: GenericFilter[int] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None
        type: GenericFilter[str] | None = None
        technology: GenericFilter[str] | None = None
        platform: GenericFilter[str] | None = None

    id: GenericFilter[int] | None = None
    meta: GenericMetaFilter | None = None
    external_id: GenericFilter[str] | None = None
    reported_sex: GenericFilter[int] | None = None
    reported_gender: GenericFilter[str] | None = None
    karyotype: GenericFilter[str] | None = None
    project: GenericFilter[ProjectId] | None = None

    family: ParticipantFamilyFilter | None = None
    sample: ParticipantSampleFilter | None = None
    sequencing_group: ParticipantSequencingGroupFilter | None = None
    assay: ParticipantAssayFilter | None = None

    def get_sample_filter(self) -> SampleFilter:
        """Get sample filter"""

        return SampleFilter(
            id=self.sample.id if self.sample else None,
            external_id=self.sample.external_id if self.sample else None,
            type=self.sample.type if self.sample else None,
            meta=self.sample.meta if self.sample else None,
            sample_root_id=self.sample.sample_root_id if self.sample else None,
            sample_parent_id=self.sample.sample_parent_id if self.sample else None,
            project=self.project,
            participant_id=self.id,
            sequencing_group=(
                SampleFilter.SampleSequencingGroupFilter(
                    id=self.sequencing_group.id,
                    external_id=self.sequencing_group.external_id,
                    meta=self.sequencing_group.meta,
                    type=self.sequencing_group.type,
                    technology=self.sequencing_group.technology,
                    platform=self.sequencing_group.platform,
                )
                if self.sequencing_group
                else None
            ),
            assay=(
                SampleFilter.SampleAssayFilter(
                    id=self.assay.id,
                    external_id=self.assay.external_id,
                    type=self.assay.type,
                    meta=self.assay.meta,
                )
                if self.assay
                else None
            ),
        )
