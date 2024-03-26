# pylint: disable=too-many-instance-attributes
from dataclasses import dataclass, field

from models.base import SMBase


class PagingLinks(SMBase):
    """Model for PAGING"""

    self: str
    next: str | None
    token: str | None


@dataclass
class AnalysisStats:
    """Model for Analysis Sequencing Group Stats"""
    id: int = 0
    sg_count: int = 0


@dataclass
class ProjectInsightsDetailsInternal:
    """Return class for the project insights details endpoint"""

    project: int
    dataset: str
    sequencing_type: str
    sample_type: str
    family_id: int
    family_ext_id: str
    participant_id: int
    participant_ext_id: str
    sample_id: str
    sample_ext_id: str
    sequencing_group_id: str
    completed_cram: bool
    in_latest_annotate_dataset: bool
    in_latest_snv_es_index: bool
    in_latest_sv_es_index: bool
    in_latest_gcnv_es_index: bool
    sequencing_group_report_links: dict[str, str]

    def to_external(self, links):
        """Convert to transport model"""
        return ProjectInsightsDetails(
            project=self.project,
            dataset=self.dataset,
            sequencing_type=self.sequencing_type,
            sample_type=self.sample_type,
            family_id=self.family_id,
            family_ext_id=self.family_ext_id,
            participant_id=self.participant_id,
            participant_ext_id=self.participant_ext_id,
            sample_id=self.sample_id,
            sample_ext_id=self.sample_ext_id,
            sequencing_group_id=self.sequencing_group_id,
            completed_cram=self.completed_cram,
            in_latest_annotate_dataset=self.in_latest_annotate_dataset,
            in_latest_snv_es_index=self.in_latest_snv_es_index,
            in_latest_sv_es_index=self.in_latest_sv_es_index,
            in_latest_gcnv_es_index=self.in_latest_gcnv_es_index,
            sequencing_group_report_links=self.sequencing_group_report_links,
            links=links,
        )


class ProjectInsightsDetails(SMBase):
    """Return class for the project insights details endpoint"""

    project: int
    dataset: str
    sequencing_type: str
    sample_type: str
    family_id: int
    family_ext_id: str
    participant_id: int
    participant_ext_id: str
    sample_id: str
    sample_ext_id: str
    sequencing_group_id: str
    completed_cram: bool
    in_latest_annotate_dataset: bool
    in_latest_snv_es_index: bool
    in_latest_sv_es_index: bool
    in_latest_gcnv_es_index: bool
    sequencing_group_report_links: dict[str, str]

    links: PagingLinks | None

    class Config:
        """Config for ProjectInsightsDetailsResponse"""

        fields = {'links': '_links'}


@dataclass
class ProjectInsightsStatsInternal:
    """Return class for the project insights stats endpoint"""

    project: int
    dataset: str
    sequencing_type: str
    total_families: int = 0
    total_participants: int = 0
    total_samples: int = 0
    total_sequencing_groups: int = 0
    total_crams: int = 0
    latest_annotate_dataset: AnalysisStats = field(default_factory=AnalysisStats)
    latest_snv_es_index: AnalysisStats = field(default_factory=AnalysisStats)
    latest_sv_es_index: AnalysisStats = field(default_factory=AnalysisStats)
    latest_gcnv_es_index: AnalysisStats = field(default_factory=AnalysisStats)

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
            latest_annotate_dataset=self.latest_annotate_dataset,
            latest_snv_es_index=self.latest_snv_es_index,
            latest_sv_es_index=self.latest_sv_es_index,
            latest_gcnv_es_index=self.latest_gcnv_es_index,
            links=links,
        )


class ProjectInsightsStats(SMBase):
    """Return class for the project insights stats endpoint"""

    project: int
    dataset: str
    sequencing_type: str
    total_families: int
    total_participants: int
    total_samples: int
    total_sequencing_groups: int
    total_crams: int
    latest_annotate_dataset: AnalysisStats
    latest_snv_es_index: AnalysisStats
    latest_sv_es_index: AnalysisStats
    latest_gcnv_es_index: AnalysisStats

    links: PagingLinks | None

    class Config:
        """Config for ProjectInsightsStatsResponse"""

        fields = {'links': '_links'}
