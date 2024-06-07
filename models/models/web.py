# pylint: disable=too-many-instance-attributes
import dataclasses
from typing import Sequence

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

    current: str
    next: str | None
    token: str | None


@dataclasses.dataclass
class ProjectSummaryInternal:
    """Return class for the project summary endpoint"""

    project: WebProject

    # stats
    total_samples: int
    total_participants: int
    total_sequencing_groups: int
    total_assays: int
    cram_seqr_stats: dict[str, dict[str, str]]
    batch_sequencing_group_stats: dict[str, dict[str, str]]

    # seqr
    seqr_links: dict[str, str]
    seqr_sync_types: list[str]

    def to_external(self):
        """Convert to transport model"""
        return ProjectSummary(
            project=self.project,
            total_samples=self.total_samples,
            total_participants=self.total_participants,
            total_sequencing_groups=self.total_sequencing_groups,
            total_assays=self.total_assays,
            cram_seqr_stats=self.cram_seqr_stats,
            batch_sequencing_group_stats=self.batch_sequencing_group_stats,
            seqr_links=self.seqr_links,
            seqr_sync_types=self.seqr_sync_types,
        )

    @classmethod
    def empty(cls, project: WebProject):
        """Get an empty project summary"""
        return cls(
            project=project,
            total_samples=0,
            total_participants=0,
            total_sequencing_groups=0,
            total_assays=0,
            cram_seqr_stats={},
            batch_sequencing_group_stats={},
            seqr_links={},
            seqr_sync_types=[],
        )


class ProjectSummary(SMBase):
    """Return class for the project summary endpoint"""

    project: WebProject

    # stats
    total_samples: int
    total_participants: int
    total_sequencing_groups: int
    total_assays: int
    cram_seqr_stats: dict[str, dict[str, str]]
    batch_sequencing_group_stats: dict[str, dict[str, str]]

    # seqr
    seqr_links: dict[str, str]
    seqr_sync_types: list[str]


class ProjectParticipantGridResponse(SMBase):
    """
    Web GridResponse including keys
    """

    # grid
    participants: list[NestedParticipant]
    total_results: int

    family_keys: list[tuple[str, str]]
    participant_keys: list[tuple[str, str]]
    sample_keys: list[tuple[str, str]]
    sequencing_group_keys: list[tuple[str, str]]
    assay_keys: list[tuple[str, str]]

    @staticmethod
    def from_params(
        participants: list[NestedParticipantInternal],
        total_results: int,
    ):
        """Convert to transport model"""
        entity_keys = ProjectParticipantGridResponse.get_entity_keys(participants)

        return ProjectParticipantGridResponse(
            participants=[p.to_external() for p in participants],
            total_results=total_results,
            family_keys=entity_keys.family_keys,
            participant_keys=entity_keys.participant_keys,
            sample_keys=entity_keys.sample_keys,
            sequencing_group_keys=entity_keys.sequencing_group_keys,
            assay_keys=entity_keys.assay_keys,
        )

    @dataclasses.dataclass
    class ParticipantGridEntityKeys:
        """
        Tiny dataclass as returntype for get_entity_keys
        """

        family_keys: list[tuple[str, str]]
        participant_keys: list[tuple[str, str]]
        sample_keys: list[tuple[str, str]]
        sequencing_group_keys: list[tuple[str, str]]
        assay_keys: list[tuple[str, str]]

    @staticmethod
    def get_entity_keys(
        participants: Sequence[NestedParticipantInternal | NestedParticipant],
    ) -> 'ParticipantGridEntityKeys':
        """
        Read through nested participants and full out the keys for the grid response
        """
        ignore_sample_meta_keys = {'reads', 'vcfs', 'gvcf'}
        ignore_assay_meta_keys = {
            'reads',
            'vcfs',
            'gvcf',
            'sequencing_platform',
            'sequencing_technology',
            'sequencing_type',
        }
        ignore_sg_meta_keys: set[str] = set()

        family_meta_keys: set[str] = set()
        participant_meta_keys: set[str] = set()
        sample_meta_keys: set[str] = set()
        sg_meta_keys: set[str] = set()
        assay_meta_keys: set[str] = set()

        for p in participants:
            if p.meta:
                participant_meta_keys.update(p.meta.keys())
            if not p.samples:
                continue
            for s in p.samples:
                if s.meta:
                    sample_meta_keys.update(s.meta.keys())
                if not s.sequencing_groups:
                    continue

                for sg in s.sequencing_groups or []:
                    if sg.meta:
                        sg_meta_keys.update(sg.meta.keys())

                    if not sg.assays:
                        continue
                    for a in sg.assays:
                        if a.meta:
                            assay_meta_keys.update(a.meta.keys())

        has_reported_sex = any(p.reported_sex is not None for p in participants)
        has_reported_gender = any(p.reported_gender is not None for p in participants)
        has_karyotype = any(p.karyotype is not None for p in participants)

        family_keys: list[tuple[str, str]] = [('external_id', 'Family ID')]
        family_keys.extend(('meta.' + k, k) for k in family_meta_keys)
        participant_keys: list[tuple[str, str]] = [('external_id', 'Participant ID')]

        if has_reported_sex:
            participant_keys.append(('reported_sex', 'Reported sex'))
        if has_reported_gender:
            participant_keys.append(('reported_gender', 'Reported gender'))
        if has_karyotype:
            participant_keys.append(('karyotype', 'Karyotype'))

        participant_keys.extend(('meta.' + k, k) for k in participant_meta_keys)
        sample_keys: list[tuple[str, str]] = [
            ('id', 'Sample ID'),
            ('external_id', 'External Sample ID'),
            ('created_date', 'Created date'),
        ] + [
            ('meta.' + k, k)
            for k in sample_meta_keys
            if k not in ignore_sample_meta_keys
        ]

        assay_keys = [('type', 'type')] + sorted(
            [
                ('meta.' + k, k)
                for k in assay_meta_keys
                if k not in ignore_assay_meta_keys
            ]
        )
        sequencing_group_keys = [
            ('id', 'Sequencing Group ID'),
            ('platform', 'Platform'),
            ('technology', 'Technology'),
            ('type', 'Type'),
        ] + sorted(
            [('meta.' + k, k) for k in sg_meta_keys if k not in ignore_sg_meta_keys]
        )

        return ProjectParticipantGridResponse.ParticipantGridEntityKeys(
            family_keys=family_keys,
            participant_keys=participant_keys,
            sample_keys=sample_keys,
            sequencing_group_keys=sequencing_group_keys,
            assay_keys=assay_keys,
        )
