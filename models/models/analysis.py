import enum
import json
from datetime import date, datetime
from typing import Any, Optional, Union

from pydantic import BaseModel, ConfigDict

from models.base import SMBase
from models.enums import AnalysisStatus
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_format_list,
    sequencing_group_id_transform_to_raw_list,
)


class AnalysisInternal(SMBase):
    """Model for Analysis"""
    model_config = ConfigDict(extra='allow')

    id: Optional[int] = None
    type: str
    status: AnalysisStatus
    output: Optional[Union[str, dict]] = None
    outputs: Optional[Union[str, dict]] = {}
    sequencing_group_ids: list[int] = []
    timestamp_completed: Optional[datetime] = None
    project: Optional[int] = None
    active: Optional[bool] = None
    meta: dict[str, Any] = {}
    author: Optional[str] = None

    @staticmethod
    def from_db(**kwargs):
        """
        Convert from db keys, mainly converting id to id_
        """
        analysis_type = kwargs.pop('type', None)
        status = kwargs.pop('status', None)
        timestamp_completed = kwargs.pop('timestamp_completed', None)
        meta = kwargs.get('meta')

        if meta and isinstance(meta, str):
            meta = json.loads(meta)

        if timestamp_completed and isinstance(timestamp_completed, str):
            timestamp_completed = datetime.fromisoformat(timestamp_completed)

        sequencing_group_ids = []
        if sg := kwargs.pop('sequencing_group_id', None):
            sequencing_group_ids.append(sg)

        return AnalysisInternal(
            id=kwargs.pop('id'),
            type=analysis_type,
            status=AnalysisStatus(status),
            sequencing_group_ids=sequencing_group_ids or [],
            output=kwargs.pop('output', None),  # TODO deprecate
            outputs=kwargs.pop('outputs', []),
            timestamp_completed=timestamp_completed,
            project=kwargs.get('project'),
            meta=meta,
            active=bool(kwargs.get('active')),
            author=kwargs.get('author'),
        )

    def to_external(self):
        """
        Convert to external model
        """
        return Analysis(
            id=self.id,
            type=self.type,
            status=self.status,
            sequencing_group_ids=sequencing_group_id_format_list(
                self.sequencing_group_ids
            ),
            output=self.output,
            outputs=self.outputs,
            timestamp_completed=self.timestamp_completed.isoformat()
            if self.timestamp_completed
            else None,
            project=self.project,
            active=self.active,
            meta=self.meta,
            author=self.author,
        )


class Analysis(BaseModel):
    """Model for Analysis"""

    type: str
    status: AnalysisStatus
    id: Optional[int] = None
    output: Optional[Union[str, dict]] = None
    outputs: Optional[Union[str, dict]] = {}
    sequencing_group_ids: list[str] = []
    author: Optional[str] = None
    timestamp_completed: Optional[str] = None
    project: Optional[int] = None
    active: Optional[bool] = None
    meta: dict[str, Any] = {}

    def to_internal(self):
        """
        Convert to internal model
        """
        return AnalysisInternal(
            id=self.id,
            type=self.type,
            status=self.status,
            sequencing_group_ids=sequencing_group_id_transform_to_raw_list(
                self.sequencing_group_ids
            ),
            output=self.output,
            outputs=self.outputs,
            # don't allow this to be set
            timestamp_completed=None,
            project=self.project,
            active=self.active,
            meta=self.meta,
            author=self.author,
        )


class DateSizeModel(BaseModel):
    """Date Size model"""

    start: date
    end: date | None
    size: dict[str, int]


class SequencingGroupSizeModel(BaseModel):
    """Project Size model"""

    sequencing_group: str
    dates: list[DateSizeModel]


class ProjectSizeModel(BaseModel):
    """Project Size model"""

    project: str
    sequencing_groups: list[SequencingGroupSizeModel]


class ProportionalDateProjectModel(BaseModel):
    """Stores the percentage / total size of a project on a date"""

    project: str
    percentage: float | int
    size: int


class ProportionalDateModel(BaseModel):
    """
    Stores the percentage / total size of all projects for a date
    """

    date: date
    projects: list[ProportionalDateProjectModel]


class ProportionalDateTemporalMethod(enum.Enum):
    """Method for which to calculate the "start" date"""

    SAMPLE_CREATE_DATE = 'SAMPLE_CREATE_DATE'
    SG_ES_INDEX_DATE = 'ES_INDEX_DATE'
