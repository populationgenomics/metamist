from typing import Any

from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.tables.analysis import AnalysisTable
from db.python.tables.cohort import CohortFilter, CohortTable
from db.python.tables.project import ProjectId
from db.python.tables.sample import SampleTable
from db.python.utils import get_logger
from models.models.cohort import Cohort
from models.models.sequencing_group import SequencingGroupInternal

logger = get_logger()


class CohortLayer(BaseLayer):
    """Layer for cohort logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)

        self.sampt = SampleTable(connection)
        self.at = AnalysisTable(connection)
        self.ct = CohortTable(connection)

    # GETS

    async def query(self, filter_: CohortFilter) -> list[Cohort]:
        """Query Cohorts"""
        cohorts = await self.ct.query(filter_)
        return cohorts

    async def get_cohort_sequencing_group_ids(self, cohort_id: int) -> list[int]:
        """
        Get the sequencing group IDs for the given cohort.
        """
        return await self.ct.get_cohort_sequencing_group_ids(cohort_id)

    # PUTS

    async def create_cohort(
        self,
        project: ProjectId,
        cohort_name: str,
        sequencing_group_ids: list[str],
        author: str,
        description: str,
        derived_from: int | None = None,
    ) -> int:
        """
        Create a new cohort from the given parameters. Returns the newly created cohort_id.
        """

        cohort_id = await self.ct.create_cohort(
            project=project,
            cohort_name=cohort_name,
            sequencing_group_ids=sequencing_group_ids,
            description=description,
            author=author,
            derived_from=derived_from,
        )

        return cohort_id
