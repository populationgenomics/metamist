import dataclasses
from datetime import date

from db.python.db_filters.generic import (
    GenericFilter,
    GenericFilterModel,
    GenericMetaFilter,
)
from models.models.project import ProjectId


@dataclasses.dataclass(kw_only=True)
class SequencingGroupFilter(GenericFilterModel):
    """Sequencing Group Filter"""

    @dataclasses.dataclass(kw_only=True)
    class SequencingGroupAssayFilter(GenericFilterModel):
        """Sequencing Group Assay Filter"""

        id: GenericFilter[int] | None = None
        external_id: GenericFilter[str] | None = None
        meta: GenericMetaFilter | None = None
        type: GenericFilter[str] | None = None

    project: GenericFilter[ProjectId] | None = None
    sample_id: GenericFilter[int] | None = None
    external_id: GenericFilter[str] | None = None
    id: GenericFilter[int] | None = None
    type: GenericFilter[str] | None = None
    technology: GenericFilter[str] | None = None
    platform: GenericFilter[str] | None = None
    active_only: GenericFilter[bool] | None = GenericFilter(eq=True)
    meta: GenericMetaFilter | None = None

    assay: SequencingGroupAssayFilter | None = None

    # These fields are manually handled in the query to speed things up, because multiple table
    # joins and dynamic computation are required.
    created_on: GenericFilter[date] | None = None
    has_cram: bool | None = None
    has_gvcf: bool | None = None
