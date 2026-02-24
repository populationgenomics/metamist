from datetime import datetime
from typing import NamedTuple

from models.models import ProjectId
from models.models.sequencing_group import SequencingGroupInternalId


AnalysisId = int
SequencingType = str
SequencingTechnology = str
SequencingPlatform = str

# util data models for project insights queries


# This layer has a lot of different queries, so we'll define some namedtuples to help us keep track of the keys
class ProjectSeqTypeKey(NamedTuple):  # noqa: D101
    project: ProjectId
    sequencing_type: SequencingType


class ProjectSeqTypeTechnologyKey(NamedTuple):  # noqa: D101
    project: ProjectId
    sequencing_type: SequencingType
    sequencing_technology: SequencingTechnology


class ProjectSeqTypeTechnologyPlatformKey(NamedTuple):  # noqa: D101
    project: ProjectId
    sequencing_type: SequencingType
    sequencing_technology: SequencingTechnology
    sequencing_platform: SequencingPlatform


class ProjectSeqTypeStageKey(NamedTuple):  # noqa: D101
    project: ProjectId
    sequencing_type: SequencingType
    stage: str


class ProjectSeqGroupKey(NamedTuple):  # noqa: D101
    project: ProjectId
    sequencing_group_id: SequencingGroupInternalId


# Namedtuples for the rows returned by the queries
class AnalysisRow(NamedTuple):  # noqa: D101
    id: AnalysisId
    output: str
    timestamp_completed: datetime


class SequencingGroupDetailRow(NamedTuple):  # noqa: D101
    family_id: int
    family_external_id: str
    participant_id: int
    participant_external_id: str
    sample_id: int
    sample_external_ids: list[str]
    sample_type: str
    sequencing_group_id: SequencingGroupInternalId


class StripyReportRow(NamedTuple):  # noqa: D101
    id: AnalysisId
    output: str
    outliers_detected: bool
    outlier_loci: str
    timestamp_completed: datetime
