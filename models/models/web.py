# pylint: disable=too-many-instance-attributes
import dataclasses
from enum import Enum
from typing import Sequence

from db.python.filters.web import ProjectParticipantGridFilter
from models.base import SMBase
from models.enums.web import MetaSearchEntityPrefix
from models.models.participant import NestedParticipant, NestedParticipantInternal


class WebProject(SMBase):
    """Return class for Project, minimal fields"""

    id: int
    name: str
    dataset: str
    meta: dict


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


class ProjectParticipantGridFilterType(Enum):
    """Filter types for grid response"""

    eq = 'eq'
    neq = 'neq'
    startswith = 'startswith'
    icontains = 'icontains'


class ProjectParticipantGridField(SMBase):
    """Field for grid response"""

    key: str
    label: str
    is_visible: bool
    # use this key when you want to filter
    filter_key: str | None = None
    # assume all if filter_key provided and is None
    filter_types: list[ProjectParticipantGridFilterType] | None = None


class ProjectParticipantGridResponse(SMBase):
    """
    Web GridResponse including keys
    """

    # grid
    participants: list[NestedParticipant]
    total_results: int

    fields: dict[MetaSearchEntityPrefix, list[ProjectParticipantGridField]]

    @staticmethod
    def from_params(
        participants: list[NestedParticipantInternal],
        filter_fields: ProjectParticipantGridFilter,
        total_results: int,
    ):
        """Convert to transport model"""
        fields = ProjectParticipantGridResponse.get_entity_keys(
            participants, filter_fields=filter_fields
        )

        return ProjectParticipantGridResponse(
            participants=[p.to_external() for p in participants],
            total_results=total_results,
            fields=fields,
        )

    @staticmethod
    def get_entity_keys(
        participants: Sequence[NestedParticipantInternal | NestedParticipant],
        # useful for showing fields that are not in the response, but in the filter
        filter_fields: ProjectParticipantGridFilter,
    ) -> dict[MetaSearchEntityPrefix, list[ProjectParticipantGridField]]:
        """
        Read through nested participants and full out the keys for the grid response
        """
        hidden_participant_meta_keys: set[str] = set()
        hidden_sample_meta_keys = {'reads', 'vcfs', 'gvcf'}
        hidden_assay_meta_keys = {
            'reads',
            'vcfs',
            'gvcf',
            'sequencing_platform',
            'sequencing_technology',
            'sequencing_type',
        }
        hidden_sg_meta_keys: set[str] = set()

        family_meta_keys: set[str] = set()
        participant_meta_keys: set[str] = set()
        sample_meta_keys: set[str] = set()
        sg_meta_keys: set[str] = set()
        assay_meta_keys: set[str] = set()
        has_nested_samples = False

        if filter_fields.family and filter_fields.family.meta:
            family_meta_keys.update(filter_fields.family.meta.keys())
        if filter_fields.participant and filter_fields.participant.meta:
            participant_meta_keys.update(filter_fields.participant.meta.keys())
            hidden_participant_meta_keys -= set(filter_fields.participant.meta.keys())
        if filter_fields.sample and filter_fields.sample.meta:
            sample_meta_keys.update(filter_fields.sample.meta.keys())
            hidden_participant_meta_keys -= set(filter_fields.sample.meta.keys())
        if filter_fields.sequencing_group and filter_fields.sequencing_group.meta:
            sg_meta_keys.update(filter_fields.sequencing_group.meta.keys())
            hidden_sg_meta_keys -= set(filter_fields.sequencing_group.meta.keys())
        if filter_fields.assay and filter_fields.assay.meta:
            assay_meta_keys.update(filter_fields.assay.meta.keys())
            hidden_assay_meta_keys -= set(filter_fields.assay.meta.keys())

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
                if s.sample_parent_id is not None:
                    has_nested_samples = True

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

        # dumb alias
        Field = ProjectParticipantGridField

        family_fields: list[ProjectParticipantGridField] = [
            Field(
                key='external_id',
                label='Family ID',
                is_visible=True,
                filter_key='external_id',
            )
        ]
        family_fields.extend(
            Field(key='meta.' + k, label=k, is_visible=True, filter_key='meta.' + k)
            for k in family_meta_keys
        )
        participant_fields = [
            Field(
                key='external_ids',
                label='Participant ID',
                is_visible=True,
                filter_key='external_id',
            ),
            Field(
                key='reported_sex',
                label='Reported sex',
                is_visible=has_reported_sex,
                filter_key='reported_sex',
            ),
            Field(
                key='reported_gender',
                label='Reported gender',
                is_visible=has_reported_gender,
                filter_key='reported_gender',
            ),
            Field(
                key='karyotype',
                label='Karyotype',
                is_visible=has_karyotype,
                filter_key='karyotype',
            ),
        ]
        participant_fields.extend(
            Field(
                key='meta.' + k,
                label=k,
                is_visible=k not in hidden_participant_meta_keys,
                filter_key='meta.' + k,
            )
            for k in participant_meta_keys
        )

        sample_fields = [
            Field(
                key='id',
                label='Sample ID',
                is_visible=True,
                filter_key='id',
                filter_types=[
                    ProjectParticipantGridFilterType.eq,
                    ProjectParticipantGridFilterType.neq,
                ],
            ),
            Field(
                key='external_ids',
                label='External Sample ID',
                is_visible=True,
                filter_key='external_id',
            ),
            Field(
                key='sample_root_id',
                label='Root Sample ID',
                is_visible=False,
                filter_key='sample_root_id',
            ),
            Field(
                key='sample_parent_id',
                label='Parent Sample ID',
                is_visible=has_nested_samples,
                filter_key='sample_root_id',
            ),
            Field(
                key='created_date',
                label='Created date',
                is_visible=True,
                # filter_key='created_date',
            ),
        ]
        sample_fields.extend(
            Field(
                key='meta.' + k,
                label=k,
                is_visible=k not in hidden_sample_meta_keys,
                filter_key='meta.' + k,
            )
            for k in sample_meta_keys
        )
        assay_fields = [
            Field(
                key='type',
                label='Type',
                is_visible=True,
                filter_key='type',
            )
        ]
        assay_fields.extend(
            Field(
                key='meta.' + k,
                label=k,
                is_visible=k not in hidden_assay_meta_keys,
                filter_key='meta.' + k,
            )
            for k in assay_meta_keys
        )

        sequencing_group_fields = [
            Field(
                key='id',
                label='Sequencing Group ID',
                is_visible=True,
                filter_key='id',
                filter_types=[
                    ProjectParticipantGridFilterType.eq,
                    ProjectParticipantGridFilterType.neq,
                ],
            ),
            Field(
                key='type',
                label='Type',
                is_visible=True,
                filter_key='type',
            ),
            Field(
                key='technology',
                label='Technology',
                is_visible=True,
                filter_key='technology',
            ),
            Field(
                key='platform',
                label='Platform',
                is_visible=True,
                filter_key='platform',
            ),
        ]

        sequencing_group_fields.extend(
            Field(
                key='meta.' + k,
                label=k,
                is_visible=k not in hidden_sg_meta_keys,
                filter_key='meta.' + k,
            )
            for k in sg_meta_keys
        )

        return {
            MetaSearchEntityPrefix.FAMILY: family_fields,
            MetaSearchEntityPrefix.PARTICIPANT: participant_fields,
            MetaSearchEntityPrefix.SAMPLE: sample_fields,
            MetaSearchEntityPrefix.SEQUENCING_GROUP: sequencing_group_fields,
            MetaSearchEntityPrefix.ASSAY: assay_fields,
        }
