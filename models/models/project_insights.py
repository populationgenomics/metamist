# pylint: disable=too-many-instance-attributes
from dataclasses import dataclass

from models.base import SMBase
from models.utils import sample_id_format, sequencing_group_id_format

@dataclass
class AnalysisStats:
    """Model for Analysis Sequencing Group Stats"""
    id: int | None = None
    name: str | None = None
    sg_count: int | None = None

    def to_external(self):
        """Convert to transport model"""
        if self.id is None:
            return None
        return {'id': self.id, 'sg_count': self.sg_count}


@dataclass
class ProjectInsightsDetailsInternal:
    """Return class for the seqr projects insights details endpoint"""

    project: int
    dataset: str
    sequencing_type: str
    sequencing_platform: str
    sequencing_technology: str
    sample_type: str
    family_id: int
    family_ext_id: str
    participant_id: int
    participant_ext_id: str
    sample_id: int
    sample_ext_ids: list[str]
    sequencing_group_id: int
    completed_cram: bool
    in_latest_annotate_dataset: bool
    in_latest_snv_es_index: bool
    in_latest_sv_es_index: bool
    sequencing_group_report_links: dict[str, str]

    def to_external(self):
        """Convert to transport model"""
        return ProjectInsightsDetails(
            project=self.project,
            dataset=self.dataset,
            sequencing_type=self.sequencing_type,
            sequencing_platform=self.sequencing_platform,
            sequencing_technology=self.sequencing_technology,
            sample_type=self.sample_type,
            family_id=self.family_id,
            family_ext_id=self.family_ext_id,
            participant_id=self.participant_id,
            participant_ext_id=self.participant_ext_id,
            sample_id=sample_id_format.sample_id_format(self.sample_id),
            sample_ext_ids=self.sample_ext_ids,
            sequencing_group_id=sequencing_group_id_format.sequencing_group_id_format(self.sequencing_group_id),
            completed_cram=self.completed_cram,
            in_latest_annotate_dataset=self.in_latest_annotate_dataset,
            in_latest_snv_es_index=self.in_latest_snv_es_index,
            in_latest_sv_es_index=self.in_latest_sv_es_index,
            sequencing_group_report_links=self.sequencing_group_report_links,
        )


class ProjectInsightsDetails(SMBase):
    """External return class for the project insights details endpoint"""

    project: int
    dataset: str
    sequencing_type: str
    sequencing_platform: str
    sequencing_technology: str
    sample_type: str
    family_id: int
    family_ext_id: str
    participant_id: int
    participant_ext_id: str
    sample_id: str
    sample_ext_ids: list[str]
    sequencing_group_id: str
    completed_cram: bool
    in_latest_annotate_dataset: bool
    in_latest_snv_es_index: bool
    in_latest_sv_es_index: bool
    sequencing_group_report_links: dict[str, str]


@dataclass
class ProjectInsightsSummaryInternal:
    """Return class for the project insights summary endpoint"""

    project: int
    dataset: str
    sequencing_type: str
    sequencing_platform: str
    sequencing_technology: str
    total_families: int = 0
    total_participants: int = 0
    total_samples: int = 0
    total_sequencing_groups: int = 0
    total_crams: int = 0
    latest_annotate_dataset: AnalysisStats = None
    latest_snv_es_index: AnalysisStats = None
    latest_sv_es_index: AnalysisStats = None

    def to_external(self):
        """Convert to transport model"""
        return ProjectInsightsSummary(
            project=self.project,
            dataset=self.dataset,
            sequencing_type=self.sequencing_type,
            sequencing_platform=self.sequencing_platform,
            sequencing_technology=self.sequencing_technology,
            total_families=self.total_families,
            total_participants=self.total_participants,
            total_samples=self.total_samples,
            total_sequencing_groups=self.total_sequencing_groups,
            total_crams=self.total_crams,
            latest_annotate_dataset=self.latest_annotate_dataset.to_external() if self.latest_annotate_dataset else None,
            latest_snv_es_index=self.latest_snv_es_index.to_external() if self.latest_snv_es_index else None,
            latest_sv_es_index=self.latest_sv_es_index.to_external() if self.latest_sv_es_index else None,
        )


class ProjectInsightsSummary(SMBase):
    """Return class for the project insights summary endpoint"""

    project: int
    dataset: str
    sequencing_type: str
    sequencing_platform: str
    sequencing_technology: str
    total_families: int
    total_participants: int
    total_samples: int
    total_sequencing_groups: int
    total_crams: int
    latest_annotate_dataset: dict[str, int] | None
    latest_snv_es_index: dict[str, int] | None
    latest_sv_es_index: dict[str, int] | None
