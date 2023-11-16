from typing import Any

from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.tables.analysis import AnalysisTable
from db.python.tables.cohort import CohortFilter, CohortTable
from db.python.tables.project import ProjectId
from db.python.tables.sample import SampleTable
from db.python.utils import get_logger
from models.models.cohort import Cohort

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
        """ Query Cohorts"""
        cohorts = await self.ct.query(filter_)
        return cohorts

    async def create_cohort(
        self,
        project: ProjectId,
        cohort_name: str,
        sequencing_group_ids: list[str],
        description: str,
        author: str = None,
    ) -> dict[str, Any]:
        """Create a new cohort"""
        output = await self.ct.create_cohort(project=project, cohort_name=cohort_name, sequencing_group_ids=sequencing_group_ids, description=description, author=author)
        return {'cohort_id': cohort_name, 'sg': sequencing_group_ids, 'output': output}
