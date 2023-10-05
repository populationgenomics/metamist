import dataclasses

from db.python.utils import (
    GenericFilter,
    GenericFilterModel,
)


@dataclasses.dataclass
class BillingFilter(GenericFilterModel):
    """Filter for billing"""

    topic: GenericFilter[str] = None
    date: GenericFilter[str] = None
    cost_category: GenericFilter[str] = None

    def __hash__(self):  # pylint: disable=useless-parent-delegation
        return super().__hash__()
