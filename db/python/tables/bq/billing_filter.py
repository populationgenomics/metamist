# pylint: disable=unused-import,too-many-instance-attributes

import dataclasses
import datetime
<<<<<<< HEAD
from typing import Any
=======
>>>>>>> dev

from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from db.python.tables.bq.generic_bq_filter_model import GenericBQFilterModel


@dataclasses.dataclass
class BillingFilter(GenericBQFilterModel):
    """
    Filter for billing, contains all possible attributes to filter on
    """

    # partition specific filters:

    # most billing views are parttioned by day
    day: GenericBQFilter[datetime.datetime] = None

    # gpc table has different partition field: part_time
    part_time: GenericBQFilter[datetime.datetime] = None

    # aggregate has different partition field: usage_end_time
    usage_end_time: GenericBQFilter[datetime.datetime] = None

    # common filters:
    invoice_month: GenericBQFilter[str] = None

    # min cost e.g. 0.01, if not set, will show all
    cost: GenericBQFilter[float] = None

    ar_guid: GenericBQFilter[str] = None
    gcp_project: GenericBQFilter[str] = None
    topic: GenericBQFilter[str] = None
    batch_id: GenericBQFilter[str] = None
    cost_category: GenericBQFilter[str] = None
    sku: GenericBQFilter[str] = None
    dataset: GenericBQFilter[str] = None
    sequencing_type: GenericBQFilter[str] = None
    stage: GenericBQFilter[str] = None
    sequencing_group: GenericBQFilter[str] = None
    compute_category: GenericBQFilter[str] = None
    cromwell_sub_workflow_name: GenericBQFilter[str] = None
    cromwell_workflow_id: GenericBQFilter[str] = None
    goog_pipelines_worker: GenericBQFilter[str] = None
    wdl_task_name: GenericBQFilter[str] = None
    namespace: GenericBQFilter[str] = None
<<<<<<< HEAD

    def __eq__(self, other: Any) -> bool:
        """Equality operator"""
        result = super().__eq__(other)
        if not result or not isinstance(other, BillingFilter):
            return False

        # compare all attributes
        for att in self.__dict__:
            if not getattr(self, att).__eq__(getattr(other, att)):
                return False

        # all attributes are equal
        return True
=======
>>>>>>> dev
