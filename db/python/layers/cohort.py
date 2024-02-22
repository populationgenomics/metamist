from models.models.cohort import Cohort
from db.python.connect import Connection

from db.python.layers.base import BaseLayer
from db.python.layers.sequencing_group import SequencingGroupLayer

from db.python.tables.analysis import AnalysisTable
from db.python.tables.cohort import CohortFilter, CohortTable
from db.python.tables.project import ProjectId
from db.python.tables.sample import SampleTable
from db.python.tables.sequencing_group import SequencingGroupTable, SequencingGroupFilter

from db.python.utils import GenericFilter, get_logger

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

    async def create_cohort_from_criteria(
            self,
            project_to_write: ProjectId,
            projects_to_pull: list[ProjectId],
            author: str,
            description: str,
            cohort_name: str,
            sg_ids_internal: list[int] | None = None,
            sg_technology: list[str] | None = None,
            sg_platform: list[str] | None = None,
            sg_type: list[str] | None = None,
    ):
        """
        Create a new cohort from the given parameters. Returns the newly created cohort_id.
        """

        # 1. Pull SG's based on criteria

        sgs = await self.sglayer.query(
            SequencingGroupFilter(
                project=GenericFilter(in_=projects_to_pull),
                id=GenericFilter(in_=sg_ids_internal) if sg_ids_internal else None,
                technology=GenericFilter(in_=sg_technology) if sg_technology else None,
                platform=GenericFilter(in_=sg_platform) if sg_platform else None,
                type=GenericFilter(in_=sg_type) if sg_type else None,
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
