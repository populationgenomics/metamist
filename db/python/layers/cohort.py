from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.tables.analysis import AnalysisTable
from db.python.tables.cohort import CohortFilter, CohortTable
from db.python.tables.project import ProjectId
from db.python.tables.sample import SampleTable
from db.python.tables.sequencing_group import SequencingGroupTable
from db.python.utils import get_logger
from models.models.cohort import Cohort
from db.python.tables.sequencing_group import SequencingGroupFilter
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.utils import GenericFilter

logger = get_logger()


class CohortLayer(BaseLayer):
    """Layer for cohort logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)

        self.sampt = SampleTable(connection)
        self.at = AnalysisTable(connection)
        self.ct = CohortTable(connection)
        self.sgt = SequencingGroupTable(connection)
        self.sglayer = SequencingGroupLayer(self.connection)

    async def query(self, filter_: CohortFilter) -> list[Cohort]:
        """Query Cohorts"""
        cohorts = await self.ct.query(filter_)
        return cohorts

    async def get_cohort_sequencing_group_ids(self, cohort_id: int) -> list[int]:
        """
        Get the sequencing group IDs for the given cohort.
        """
        return await self.ct.get_cohort_sequencing_group_ids(cohort_id)

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

    async def create_cohort_from_criteria(
            self,
            project_to_write: ProjectId,
            projects_to_pull: list[ProjectId],
            author: str,
            description: str,
            cohort_name: str,
    ):
        """
        Create a new cohort from the given parameters. Returns the newly created cohort_id.
        """

        # 1. Pull SG's based on criteria
        sgs = await self.sglayer.query(
            SequencingGroupFilter(
                project=GenericFilter(in_=projects_to_pull)
            )
        )
        print(sgs)

        # 2. Create Cohort
        cohort_id = await self.ct.create_cohort(
            project=project_to_write,
            cohort_name=cohort_name,
            sequencing_group_ids=[sg.id for sg in sgs],
            description=description,
            author=author,
        )

        return cohort_id
