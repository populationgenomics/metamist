# pylint: disable=too-many-instance-attributes
import dataclasses

from models.base import SMBase
from models.models.participant import NestedParticipant, NestedParticipantInternal


class WebProject(SMBase):
    """Return class for Project, minimal fields"""

    id: int
    name: str
    dataset: str
    meta: dict


class PagingLinks(SMBase):
    """Model for PAGING"""

    self: str
    next: str | None
    token: str | None


@dataclasses.dataclass
class ProjectSummaryInternal:
    """Return class for the project summary endpoint"""

    project: WebProject

    # stats
    total_samples: int
    total_samples_in_query: int
    total_participants: int
    total_sequencing_groups: int
    total_assays: int
    cram_seqr_stats: dict[str, dict[str, str]]
    batch_sequencing_group_stats: dict[str, dict[str, str]]

    # grid
    participants: list[NestedParticipantInternal]
    participant_keys: list[tuple[str, str]]
    sample_keys: list[tuple[str, str]]
    sequencing_group_keys: list[tuple[str, str]]
    assay_keys: list[tuple[str, str]]

    # seqr
    seqr_links: dict[str, str]
    seqr_sync_types: list[str]

    def to_external(self, links):
        """Convert to transport model"""
        return ProjectSummary(
            project=self.project,
            total_samples=self.total_samples,
            total_samples_in_query=self.total_samples_in_query,
            total_participants=self.total_participants,
            total_sequencing_groups=self.total_sequencing_groups,
            total_assays=self.total_assays,
            cram_seqr_stats=self.cram_seqr_stats,
            batch_sequencing_group_stats=self.batch_sequencing_group_stats,
            participants=[p.to_external() for p in self.participants],
            participant_keys=self.participant_keys,
            sample_keys=self.sample_keys,
            sequencing_group_keys=self.sequencing_group_keys,
            assay_keys=self.assay_keys,
            seqr_links=self.seqr_links,
            seqr_sync_types=self.seqr_sync_types,
            links=links,
        )

    @classmethod
    def empty(cls, project: WebProject):
        """Get an empty project summary"""
        return cls(
            project=project,
            total_samples=0,
            total_samples_in_query=0,
            total_participants=0,
            total_sequencing_groups=0,
            total_assays=0,
            cram_seqr_stats={},
            batch_sequencing_group_stats={},
            participants=[],
            participant_keys=[],
            sample_keys=[],
            sequencing_group_keys=[],
            assay_keys=[],
            seqr_links={},
            seqr_sync_types=[],
        )


class ProjectSummary(SMBase):
    """Return class for the project summary endpoint"""

    project: WebProject

    # stats
    total_samples: int
    total_samples_in_query: int
    total_participants: int
    total_sequencing_groups: int
    total_assays: int
    cram_seqr_stats: dict[str, dict[str, str]]
    batch_sequencing_group_stats: dict[str, dict[str, str]]

    # grid
    participants: list[NestedParticipant]
    participant_keys: list[tuple[str, str]]
    sample_keys: list[tuple[str, str]]
    sequencing_group_keys: list[tuple[str, str]]
    assay_keys: list[tuple[str, str]]

    # seqr
    seqr_links: dict[str, str]
    seqr_sync_types: list[str]

    links: PagingLinks | None
