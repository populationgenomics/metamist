# pylint: disable=too-many-instance-attributes
import dataclasses

from db.python.filters.generic import (
    GenericFilter,
    GenericFilterModel,
    GenericMetaFilter,
)
from db.python.filters.sequencing_group import SequencingGroupFilter
from models.models.project import ProjectId


@dataclasses.dataclass(kw_only=True)
class SampleFilter(GenericFilterModel):
    """
    Sample filter model
    """

    @dataclasses.dataclass(kw_only=True)
    class SampleSequencingGroupFilter(GenericFilterModel):
        """
        Participant sequencing group filter model
        """

        id: GenericFilter[int] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None
        type: GenericFilter[str] | None = None
        technology: GenericFilter[str] | None = None
        platform: GenericFilter[str] | None = None

    @dataclasses.dataclass(kw_only=True)
    class SampleAssayFilter(GenericFilterModel):
        """
        Participant assay filter model
        """

        id: GenericFilter[int] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None
        type: GenericFilter[str] | None = None

    # fields
    id: GenericFilter[int] | None = None
    type: GenericFilter[str] | None = None
    meta: GenericMetaFilter | None = None
    external_id: GenericFilter[str] | None = None
    active: GenericFilter[bool] | None = None

    sample_root_id: GenericFilter[int] | None = None
    sample_parent_id: GenericFilter[int] | None = None

    # nested
    sequencing_group: SampleSequencingGroupFilter | None = None
    assay: SampleAssayFilter | None = None

    # links
    participant_id: GenericFilter[int] | None = None
    project: GenericFilter[ProjectId] | None = None

    def get_sg_filter(self) -> SequencingGroupFilter:
        """
        Get the sequencing group filter for sample attributes
        """

        return SequencingGroupFilter(
            id=self.sequencing_group.id if self.sequencing_group else None,
            sample=SequencingGroupFilter.SequencingGroupSampleFilter(
                id=self.id,
                type=self.type,
                meta=self.meta,
                external_id=self.external_id,
            ),
            external_id=(
                self.sequencing_group.external_id if self.sequencing_group else None
            ),
            technology=(
                self.sequencing_group.technology if self.sequencing_group else None
            ),
            platform=self.sequencing_group.platform if self.sequencing_group else None,
            type=self.sequencing_group.type if self.sequencing_group else None,
            meta=self.sequencing_group.meta if self.sequencing_group else None,
            assay=(
                SequencingGroupFilter.SequencingGroupAssayFilter(
                    id=self.assay.id,
                    external_id=self.assay.external_id,
                    meta=self.assay.meta,
                    type=self.assay.type,
                )
                if self.assay
                else None
            ),
        )
