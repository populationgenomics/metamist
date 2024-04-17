# pylint: disable=too-many-instance-attributes
from dataclasses import dataclass

from models.base import SMBase
from models.utils import sample_id_format, sequencing_group_id_format


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

    def to_external(self):
        """Convert to transport model"""
        return {'id': self.id, 'sg_count': self.sg_count}


@dataclass
class SeqrProjectsDetailsInternal:
    """Return class for the seqr projects stats details endpoint"""

    project: int
    dataset: str
    sequencing_type: str
    sample_type: str
    family_id: int
    family_ext_id: str
    participant_id: int
    participant_ext_id: str
    sample_id: int
    sample_ext_id: list[str]
    sequencing_group_id: int
    completed_cram: bool
    in_latest_annotate_dataset: bool
    in_latest_snv_es_index: bool
    in_latest_sv_es_index: bool
    sequencing_group_report_links: dict[str, str]

    def to_external(self, links):
        """Convert to transport model"""
        return SeqrProjectsDetails(
            project=self.project,
            dataset=self.dataset,
            sequencing_type=self.sequencing_type,
            sample_type=self.sample_type,
            family_id=self.family_id,
            family_ext_id=self.family_ext_id,
            participant_id=self.participant_id,
            participant_ext_id=self.participant_ext_id,
            sample_id=sample_id_format.sample_id_format(self.sample_id),
            sample_ext_id=self.sample_ext_id,
            sequencing_group_id=sequencing_group_id_format.sequencing_group_id_format(self.sequencing_group_id),
            completed_cram=self.completed_cram,
            in_latest_annotate_dataset=self.in_latest_annotate_dataset,
            in_latest_snv_es_index=self.in_latest_snv_es_index,
            in_latest_sv_es_index=self.in_latest_sv_es_index,
            sequencing_group_report_links=self.sequencing_group_report_links,
            links=links,
        )


class SeqrProjectsDetails(SMBase):
    """External return class for the seqr projects stats details endpoint"""

    project: int
    dataset: str
    sequencing_type: str
    sample_type: str
    family_id: int
    family_ext_id: str
    participant_id: int
    participant_ext_id: str
    sample_id: str
    sample_ext_id: list[str]
    sequencing_group_id: str
    completed_cram: bool
    in_latest_annotate_dataset: bool
    in_latest_snv_es_index: bool
    in_latest_sv_es_index: bool
    sequencing_group_report_links: dict[str, str]

    links: PagingLinks | None

    class Config:
        """Config for SeqrProjectsDetailsResponse"""

        fields = {'links': '_links'}


@dataclass
class SeqrProjectsSummaryInternal:
    """Return class for the seqr projects summary endpoint"""

    project: int
    dataset: str
    sequencing_type: str
    total_families: int = 0
    total_participants: int = 0
    total_samples: int = 0
    total_sequencing_groups: int = 0
    total_crams: int = 0
    latest_annotate_dataset: AnalysisStats = None
    latest_snv_es_index: AnalysisStats = None
    latest_sv_es_index: AnalysisStats = None

    def to_external(self, links):
        """Convert to transport model"""
        return SeqrProjectsSummary(
            project=self.project,
            dataset=self.dataset,
            sequencing_type=self.sequencing_type,
            total_families=self.total_families,
            total_participants=self.total_participants,
            total_samples=self.total_samples,
            total_sequencing_groups=self.total_sequencing_groups,
            total_crams=self.total_crams,
            latest_annotate_dataset=self.latest_annotate_dataset.to_external(),
            latest_snv_es_index=self.latest_snv_es_index.to_external(),
            latest_sv_es_index=self.latest_sv_es_index.to_external(),
            links=links,
        )


class SeqrProjectsSummary(SMBase):
    """Return class for the project insights stats endpoint"""

    project: int
    dataset: str
    sequencing_type: str
    total_families: int
    total_participants: int
    total_samples: int
    total_sequencing_groups: int
    total_crams: int
    latest_annotate_dataset: dict[str, int]
    latest_snv_es_index: dict[str, int]
    latest_sv_es_index: dict[str, int]

    links: PagingLinks | None

    class Config:
        """Config for ProjectInsightsStatsResponse"""

        fields = {'links': '_links'}
