from db.python.db_filters.generic import GenericFilter, GenericMetaFilter
from db.python.db_filters.participant import ParticipantFilter
from models.base import SMBase
from models.models.project import ProjectId
from models.utils.sample_id_format import sample_id_transform_to_raw
from models.utils.sequencing_group_id_format import sequencing_group_id_transform_to_raw

# these ones have to inherit from SMBase to get better error checking on the web


class ProjectParticipantGridFilter(SMBase):
    """filters for participant grid"""

    class ParticipantGridParticipantFilter(SMBase):
        """participant filter option for participant grid"""

        id: GenericFilter[int] | None = None
        meta: GenericMetaFilter | None = None
        external_id: GenericFilter[str] | None = None
        reported_sex: GenericFilter[int] | None = None
        reported_gender: GenericFilter[str] | None = None
        karyotype: GenericFilter[str] | None = None

    class ParticipantGridFamilyFilter(SMBase):
        """family filter option for participant grid"""

        id: GenericFilter[int] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None

    class ParticipantGridSampleFilter(SMBase):
        """sample filter option for participant grid"""

        id: GenericFilter[str] | None = None
        type: GenericFilter[str] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None

    class ParticipantGridSequencingGroupFilter(SMBase):
        """sequencing group filter option for participant grid"""

        id: GenericFilter[str] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None
        type: GenericFilter[str] | None = None
        technology: GenericFilter[str] | None = None
        platform: GenericFilter[str] | None = None

    class ParticipantGridAssayFilter(SMBase):
        """assay filter option for participant grid"""

        id: GenericFilter[int] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None
        type: GenericFilter[str] | None = None

    family: ParticipantGridFamilyFilter | None = None
    participant: ParticipantGridParticipantFilter | None = None
    sample: ParticipantGridSampleFilter | None = None
    sequencing_group: ParticipantGridSequencingGroupFilter | None = None
    assay: ParticipantGridAssayFilter | None = None

    def to_participant_internal(self, project: ProjectId) -> ParticipantFilter:
        """Convert to participant internal filter object"""
        return ParticipantFilter(
            id=self.participant.id if self.participant else None,
            external_id=self.participant.external_id if self.participant else None,
            reported_sex=self.participant.reported_sex if self.participant else None,
            reported_gender=(
                self.participant.reported_gender if self.participant else None
            ),
            karyotype=self.participant.karyotype if self.participant else None,
            meta=self.participant.meta if self.participant else None,
            project=GenericFilter(eq=project),
            # nested models
            family=(
                ParticipantFilter.ParticipantFamilyFilter(
                    id=self.family.id,
                    external_id=self.family.external_id,
                    meta=self.family.meta,
                )
                if self.family
                else None
            ),
            sample=(
                ParticipantFilter.ParticipantSampleFilter(
                    id=(
                        self.sample.id.transform(sample_id_transform_to_raw)
                        if self.sample.id
                        else None
                    ),
                    type=self.sample.type,
                    external_id=self.sample.external_id,
                    meta=self.sample.meta,
                )
                if self.sample
                else None
            ),
            sequencing_group=(
                ParticipantFilter.ParticipantSequencingGroupFilter(
                    id=(
                        self.sequencing_group.id.transform(
                            sequencing_group_id_transform_to_raw
                        )
                        if self.sequencing_group.id
                        else None
                    ),
                    type=self.sequencing_group.type,
                    external_id=self.sequencing_group.external_id,
                    meta=self.sequencing_group.meta,
                    technology=self.sequencing_group.technology,
                    platform=self.sequencing_group.platform,
                )
                if self.sequencing_group
                else None
            ),
            assay=(
                ParticipantFilter.ParticipantAssayFilter(
                    id=self.assay.id,
                    type=self.assay.type,
                    external_id=self.assay.external_id,
                    meta=self.assay.meta,
                )
                if self.assay
                else None
            ),
        )
