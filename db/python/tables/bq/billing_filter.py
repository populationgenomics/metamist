# pylint: disable=unused-import,too-many-instance-attributes

import dataclasses
import datetime
from typing import Any

from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from db.python.tables.bq.generic_bq_filter_model import GenericBQFilterModel


@dataclasses.dataclass
class BillingFilter(GenericBQFilterModel):
    """
    Filter for billing, contains all possible attributes to filter on
    """

    # partition specific filters:

    # most billing views are parttioned by day
    day: GenericBQFilter[datetime.datetime] | None = None

    # gpc table has different partition field: part_time
    part_time: GenericBQFilter[datetime.datetime] | None = None

    # aggregate has different partition field: usage_end_time
    usage_end_time: GenericBQFilter[datetime.datetime] | None = None

    # common filters:
    invoice_month: GenericBQFilter[str] | None = None

    # min cost e.g. 0.01, if not set, will show all
    cost: GenericBQFilter[float] | None = None

    ar_guid: GenericBQFilter[str] | None = None
    gcp_project: GenericBQFilter[str] | None = None
    topic: GenericBQFilter[str] | None = None
    batch_id: GenericBQFilter[str] | None = None
    cost_category: GenericBQFilter[str] | None = None
    sku: GenericBQFilter[str] | None = None
    dataset: GenericBQFilter[str] | None = None
    sequencing_type: GenericBQFilter[str] | None = None
    stage: GenericBQFilter[str] | None = None
    sequencing_group: GenericBQFilter[str] | None = None
    compute_category: GenericBQFilter[str] | None = None
    cromwell_sub_workflow_name: GenericBQFilter[str] | None = None
    cromwell_workflow_id: GenericBQFilter[str] | None = None
    goog_pipelines_worker: GenericBQFilter[str] | None = None
    wdl_task_name: GenericBQFilter[str] | None = None
    namespace: GenericBQFilter[str] | None = None

    def __eq__(self, other: Any) -> bool:
        """Equality operator"""
        result = super().__eq__(other)
        if not result or not isinstance(other, BillingFilter):
            return False

        # compare all attributes
        for att in self.__dict__:
            if getattr(self, att) != getattr(other, att):
                return False

        # all attributes are equal
        return True
