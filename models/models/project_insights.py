# pylint: disable=too-many-instance-attributes
import dataclasses

from models.base import SMBase


class PagingLinks(SMBase):
    """Model for PAGING"""

    self: str
    next: str | None
    token: str | None


@dataclasses.dataclass
class ProjectInsightsDetailsInternal:
    """Return class for the projects seqr details endpoint"""

    project: int
    dataset: str
    sequencing_type: str
    family_id: int
    family_ext_id: str
    participant_id: int
    participant_ext_id: str
    sample_id: str
    sample_ext_id: str
    sequencing_group_id: str
    completed_cram: bool
    in_latest_es_index: bool
    in_latest_annotate_dataset: bool
    sequencing_group_report_links: dict[str, str]

    def to_external(self, links):
        """Convert to transport model"""
        return ProjectInsightsDetails(
            project=self.project,
            dataset=self.dataset,
            sequencing_type=self.sequencing_type,
            family_id=self.family_id,
            family_ext_id=self.family_ext_id,
            participant_id=self.participant_id,
            participant_ext_id=self.participant_ext_id,
            sample_id=self.sample_id,
            sample_ext_id=self.sample_ext_id,
            sequencing_group_id=self.sequencing_group_id,
            completed_cram=self.completed_cram,
            in_latest_es_index=self.in_latest_es_index,
            in_latest_annotate_dataset=self.in_latest_annotate_dataset,
            sequencing_group_report_links=self.sequencing_group_report_links,
            links=links,
        )


class ProjectInsightsDetails(SMBase):
    """Return class for the projects seqr details endpoint"""

    project: int
    dataset: str
    sequencing_type: str
    family_id: int
    family_ext_id: str
    participant_id: int
    participant_ext_id: str
    sample_id: str
    sample_ext_id: str
    sequencing_group_id: str
    completed_cram: bool
    in_latest_es_index: bool
    in_latest_annotate_dataset: bool
    sequencing_group_report_links: dict[str, str]

    links: PagingLinks | None

    class Config:
        """Config for ProjectInsightsDetailsResponse"""

        fields = {'links': '_links'}


@dataclasses.dataclass
class ProjectInsightsStatsInternal:
    """Return class for the projects seqr stats endpoint"""

    project: int
    dataset: str
    sequencing_type: str
    total_families: int
    total_participants: int
    total_samples: int
    total_sequencing_groups: int
    total_crams: int
    latest_es_index_id: int
    total_sgs_in_latest_es_index: int
    latest_annotate_dataset_id: int
    total_sgs_in_latest_annotate_dataset: int

    def to_external(self, links):
        """Convert to transport model"""
        return ProjectInsightsStats(
            project=self.project,
            dataset=self.dataset,
            sequencing_type=self.sequencing_type,
            total_families=self.total_families,
            total_participants=self.total_participants,
            total_samples=self.total_samples,
            total_sequencing_groups=self.total_sequencing_groups,
            total_crams=self.total_crams,
            latest_es_index_id=self.latest_es_index_id,
            total_sgs_in_latest_es_index=self.total_sgs_in_latest_es_index,
            latest_annotate_dataset_id=self.latest_annotate_dataset_id,
            total_sgs_in_latest_annotate_dataset=self.total_sgs_in_latest_annotate_dataset,
            links=links,
        )


class ProjectInsightsStats(SMBase):
    """Return class for the projects seqr stats endpoint"""

    project: int
    dataset: str
    sequencing_type: str
    total_families: int
    total_participants: int
    total_samples: int
    total_sequencing_groups: int
    total_crams: int
    latest_es_index_id: int
    total_sgs_in_latest_es_index: int
    latest_annotate_dataset_id: int
    total_sgs_in_latest_annotate_dataset: int

    links: PagingLinks | None

    class Config:
        """Config for ProjectInsightsStatsResponse"""

        fields = {'links': '_links'}
