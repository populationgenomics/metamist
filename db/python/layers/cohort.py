
from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.tables.analysis import AnalysisTable
from db.python.tables.cohort import CohortFilter, CohortTable
from db.python.tables.project import ProjectId
from db.python.tables.sample import SampleTable
from db.python.utils import get_logger

logger = get_logger()

class CohortLayer(BaseLayer):
    """Layer for cohort logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)

        self.sampt = SampleTable(connection)
        self.at = AnalysisTable(connection)
        self.ct = CohortTable(connection)

    # GETS

    async def query(self, filter_: CohortFilter):
        """ Query Cohorts"""
        cohorts = await self.ct.query(filter_)
        return cohorts

    async def get_sgs_for_cohort(
        self,
        projects: list[str],
    ) -> list[str]:
        """Get all sequencing groups for a cohort"""
        print(projects)
        project_objects = [await self.ptable._get_project_by_name(project) for project in projects]
        project_ids = [project.id for project in project_objects]
        await self.ptable.check_access_to_project_ids(
                self.author, project_ids, readonly=True
        )

        return await self.ct.get_sgs_for_cohort(project_ids)

    async def create_cohort(
        self,
        project: ProjectId,
        cohort_name: str,
        sequencing_group_ids: list[str],
        description: str,
        author: str = None,
    ) -> int:
        """Create a new cohort"""
        output = await self.ct.create_cohort(project=project, cohort_name=cohort_name, sequencing_group_ids=sequencing_group_ids,description=description,author=author)
        return {'cohort_id': cohort_name, 'sg': sequencing_group_ids, 'output': output }

